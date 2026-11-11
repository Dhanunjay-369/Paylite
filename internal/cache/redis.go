// Package cache is the Redis speed layer: a rate limiter and a replay cache.
// Nothing here decides whether money moves. If Redis is down, callers fall back
// to Postgres and the system stays correct, just a little slower.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"paylite/internal/payments"
)

// Options configures the Redis cache.
type Options struct {
	Addr      string
	DB        int
	Limit     int           // requests allowed per merchant per Window
	Window    time.Duration // rate-limit window
	ReplayTTL time.Duration // how long finished payments stay cached
}

// Redis implements payments.Cache.
type Redis struct {
	rdb  *redis.Client
	opts Options
}

// NewRedis builds a client with short timeouts and no retries, so an outage
// fails fast instead of stalling every request.
func NewRedis(o Options) *Redis {
	return &Redis{
		opts: o,
		rdb: redis.NewClient(&redis.Options{
			Addr:         o.Addr,
			DB:           o.DB,
			DialTimeout:  200 * time.Millisecond,
			ReadTimeout:  200 * time.Millisecond,
			WriteTimeout: 200 * time.Millisecond,
			MaxRetries:   -1,
		}),
	}
}

func (r *Redis) Ping(ctx context.Context) error { return r.rdb.Ping(ctx).Err() }

// FlushDB empties the selected Redis database. Tests only.
func (r *Redis) FlushDB(ctx context.Context) error { return r.rdb.FlushDB(ctx).Err() }

func (r *Redis) Close() error { return r.rdb.Close() }

// rateScript counts a request and starts the window on the first hit. Running
// as Lua makes INCR and PEXPIRE atomic: a crash between them can never leave a
// counter without an expiry.
var rateScript = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return {count, redis.call('PTTL', KEYS[1])}
`)

// Allow implements a fixed-window rate limit per merchant.
func (r *Redis) Allow(ctx context.Context, merchantID string) (bool, time.Duration, error) {
	vals, err := rateScript.Run(ctx, r.rdb, []string{"rl:" + merchantID}, r.opts.Window.Milliseconds()).Int64Slice()
	if err != nil {
		return true, 0, err // fail open: a Redis problem must not block payments
	}
	count, ttl := vals[0], vals[1]
	if count > int64(r.opts.Limit) {
		return false, time.Duration(ttl) * time.Millisecond, nil
	}
	return true, 0, nil
}

func replayKey(merchantID, key string) string { return fmt.Sprintf("replay:%s:%s", merchantID, key) }

// GetReplay returns a cached finished payment, or nil on a miss.
func (r *Redis) GetReplay(ctx context.Context, merchantID, key string) (*payments.CachedResponse, error) {
	raw, err := r.rdb.Get(ctx, replayKey(merchantID, key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var c payments.CachedResponse
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// PutReplay caches a FINISHED payment. Only terminal payments are cached because
// they can never change again, so the cache can never serve stale money state.
func (r *Redis) PutReplay(ctx context.Context, merchantID, key, requestHash string, p payments.Payment) error {
	raw, err := json.Marshal(payments.CachedResponse{RequestHash: requestHash, Payment: p})
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, replayKey(merchantID, key), raw, r.opts.ReplayTTL).Err()
}
