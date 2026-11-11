// Package testutil builds a real Postgres + Redis test environment.
package testutil

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"paylite/internal/cache"
	"paylite/internal/payments"
	"paylite/internal/processor"
)

// Options tweaks the environment for one test.
type Options struct {
	RedisDown bool // point the cache at a dead address
	RateLimit int  // requests per window; default 1000
}

// Env is everything a test needs.
type Env struct {
	DB    *sql.DB
	Store *payments.Store
	Svc   *payments.Service
	Proc  *processor.Fake
	Cache payments.Cache
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// unavailable skips the test locally, but fails it in CI (REQUIRE_INTEGRATION=1)
// so a missing database can never make the pipeline silently green.
func unavailable(t testing.TB, what string, err error) {
	t.Helper()
	if os.Getenv("REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("%s unavailable: %v", what, err)
	}
	t.Skipf("%s unavailable (%v); run `make up` or set REQUIRE_INTEGRATION=1 to make this fatal", what, err)
}

// New connects to Postgres and Redis, resets all state, and returns a fresh Env.
func New(t testing.TB, opt Options) *Env {
	t.Helper()
	ctx := context.Background()

	db, err := sql.Open("postgres", getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/paylite_test?sslmode=disable"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		unavailable(t, "postgres", err)
	}
	db.SetMaxOpenConns(40)

	// `go test ./...` runs packages in parallel, and every test resets the same
	// tables. A session-level advisory lock makes tests take turns across
	// packages: it is held until the test (and its cleanups) finish.
	lockConn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockConn.ExecContext(ctx, `SELECT pg_advisory_lock(727274)`); err != nil {
		t.Fatalf("advisory lock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(727274)`)
		_ = lockConn.Close()
	})

	store := payments.NewStore(db)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE ledger_entries, payment_events, webhook_events, payments CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	limit := opt.RateLimit
	if limit == 0 {
		limit = 1000
	}
	redisDB, _ := strconv.Atoi(getenv("REDIS_TEST_DB", "15"))
	rc := cache.NewRedis(cache.Options{
		Addr: getenv("REDIS_ADDR", "localhost:6379"), DB: redisDB,
		Limit: limit, Window: time.Minute, ReplayTTL: time.Hour,
	})
	if opt.RedisDown {
		rc = cache.NewRedis(cache.Options{Addr: "127.0.0.1:1", Limit: limit, Window: time.Minute, ReplayTTL: time.Hour})
	} else {
		if err := rc.Ping(pctx); err != nil {
			unavailable(t, "redis", err)
		}
		if err := rc.FlushDB(ctx); err != nil {
			t.Fatalf("flush redis: %v", err)
		}
	}
	t.Cleanup(func() { _ = rc.Close() })

	proc := processor.NewFake()
	cfg := payments.Config{
		ChargeTimeout:        150 * time.Millisecond,
		PendingStaleAfter:    200 * time.Millisecond,
		ReconcileLease:       100 * time.Millisecond,
		MaxReconcileAttempts: 5,
		BackoffBase:          10 * time.Millisecond,
		BackoffCap:           50 * time.Millisecond,
		ReconcileBatch:       50,
		ReconcileWorkers:     4,
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Env{DB: db, Store: store, Proc: proc, Cache: rc, Svc: payments.NewService(store, proc, rc, cfg, quiet)}
}

// Count runs a single-number SQL query.
func (e *Env) Count(t testing.TB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.DB.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return n
}
