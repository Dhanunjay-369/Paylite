// Command api runs the PayLite HTTP server and the background reconciler.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"paylite/internal/cache"
	"paylite/internal/httpapi"
	"paylite/internal/payments"
	"paylite/internal/processor"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("postgres", env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/paylite?sslmode=disable"))
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(envInt("DB_MAX_CONNS", 25))
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := waitForDB(ctx, db, log); err != nil {
		return err
	}

	store := payments.NewStore(db)
	if err := store.Migrate(ctx); err != nil {
		return err
	}

	rc := cache.NewRedis(cache.Options{
		Addr:      env("REDIS_ADDR", "localhost:6379"),
		Limit:     envInt("RATE_LIMIT_PER_MINUTE", 600),
		Window:    time.Minute,
		ReplayTTL: 24 * time.Hour,
	})
	defer rc.Close()
	if err := rc.Ping(ctx); err != nil {
		// Not fatal on purpose: the service is correct without Redis.
		log.Warn("redis unreachable at startup; running without cache and rate limiting", "err", err)
	}

	// The fake processor stands in for a real one. Replace it with an adapter
	// that implements processor.Processor for Stripe, Adyen, and so on.
	fake := processor.NewFake()
	if m, ok := processor.ParseMode(env("PROCESSOR_MODE", "approve")); ok {
		fake.SetMode(m)
	}
	fake.SetLatency(time.Duration(envInt("PROCESSOR_LATENCY_MS", 0)) * time.Millisecond)

	cfg := payments.DefaultConfig()
	cfg.ChargeTimeout = time.Duration(envInt("CHARGE_TIMEOUT_MS", 5000)) * time.Millisecond
	cfg.PendingStaleAfter = time.Duration(envInt("PENDING_STALE_AFTER_MS", 30000)) * time.Millisecond
	svc := payments.NewService(store, fake, rc, cfg, log)

	api := httpapi.New(svc, store, rc, []byte(env("WEBHOOK_SECRET", "dev-webhook-secret")), log)
	mux := http.NewServeMux()
	mux.Handle("/", api.Routes())
	if os.Getenv("ENABLE_DEBUG") == "1" {
		registerDebug(mux, fake, store) // demo controls; never enable in production
	}

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go svc.RunReconciler(ctx, time.Duration(envInt("RECONCILE_INTERVAL_MS", 2000))*time.Millisecond)

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func waitForDB(ctx context.Context, db *sql.DB, log *slog.Logger) error {
	var err error
	for i := 0; i < 30; i++ {
		if err = db.PingContext(ctx); err == nil {
			return nil
		}
		log.Info("waiting for postgres", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return err
}

// registerDebug adds endpoints that let the demo script flip the fake
// processor's behaviour and read how many times money really moved.
func registerDebug(mux *http.ServeMux, fake *processor.Fake, store *payments.Store) {
	mux.HandleFunc("GET /debug/processor", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mode": fake.Mode().String(), "approved_charges": fake.ApprovedCharges(), "charge_calls": fake.Calls(),
		})
	})
	mux.HandleFunc("GET /debug/ledger", func(w http.ResponseWriter, r *http.Request) {
		entries, debits, credits, err := store.LedgerTotals(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"entries": entries, "debits_cents": debits, "credits_cents": credits, "balanced": debits == credits,
		})
	})
	mux.HandleFunc("POST /debug/processor/mode", func(w http.ResponseWriter, r *http.Request) {
		m, ok := processor.ParseMode(r.URL.Query().Get("mode"))
		if !ok {
			http.Error(w, "mode must be approve, decline, apply_then_timeout or drop_request", http.StatusBadRequest)
			return
		}
		fake.SetMode(m)
		_ = json.NewEncoder(w).Encode(map[string]string{"mode": m.String()})
	})
}
