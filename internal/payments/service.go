package payments

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"paylite/internal/processor"
)

// CachedResponse is what Redis remembers about a finished payment.
type CachedResponse struct {
	RequestHash string  `json:"request_hash"`
	Payment     Payment `json:"payment"`
}

// Cache is the optional speed layer in front of Postgres. Every method may
// fail; the service treats a failure as "cache unavailable" and carries on, so
// a Redis outage can slow things down but can never cause a double charge.
type Cache interface {
	GetReplay(ctx context.Context, merchantID, key string) (*CachedResponse, error)
	PutReplay(ctx context.Context, merchantID, key, requestHash string, p Payment) error
	Allow(ctx context.Context, merchantID string) (allowed bool, retryAfter time.Duration, err error)
}

// NopCache is a Cache that does nothing.
type NopCache struct{}

func (NopCache) GetReplay(context.Context, string, string) (*CachedResponse, error) { return nil, nil }
func (NopCache) PutReplay(context.Context, string, string, string, Payment) error   { return nil }
func (NopCache) Allow(context.Context, string) (bool, time.Duration, error)         { return true, 0, nil }

// Config tunes timeouts and retry behaviour.
type Config struct {
	ChargeTimeout        time.Duration // how long we wait for the processor
	PendingStaleAfter    time.Duration // a pending row older than this is assumed orphaned (crash)
	ReconcileLease       time.Duration // how long a worker owns a claimed payment
	MaxReconcileAttempts int           // after this many tries, a human takes over
	BackoffBase          time.Duration
	BackoffCap           time.Duration
	ReconcileBatch       int
	ReconcileWorkers     int
}

func DefaultConfig() Config {
	return Config{
		ChargeTimeout:        5 * time.Second,
		PendingStaleAfter:    30 * time.Second,
		ReconcileLease:       30 * time.Second,
		MaxReconcileAttempts: 8,
		BackoffBase:          2 * time.Second,
		BackoffCap:           time.Minute,
		ReconcileBatch:       20,
		ReconcileWorkers:     4,
	}
}

// Service creates payments exactly once and reconciles the uncertain ones.
type Service struct {
	store *Store
	proc  processor.Processor
	cache Cache
	cfg   Config
	log   *slog.Logger
}

func NewService(store *Store, proc processor.Processor, cache Cache, cfg Config, log *slog.Logger) *Service {
	if cache == nil {
		cache = NopCache{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, proc: proc, cache: cache, cfg: cfg, log: log}
}

// Result is the answer to a create request.
type Result struct {
	Payment  Payment
	Replayed bool // true when this answer came from an earlier request with the same key
}

// CreatePayment charges the processor AT MOST ONCE per (merchant, idempotency key),
// no matter how many times, or how concurrently, the same request arrives.
//
//  1. Redis replay cache (fast path, optional)
//  2. INSERT ... ON CONFLICT DO NOTHING in Postgres: exactly one caller wins
//  3. Only the winner calls the processor
//  4. A timeout becomes "unknown", never "failed": the money may have moved
func (s *Service) CreatePayment(ctx context.Context, req CreateRequest) (Result, error) {
	req.Currency = strings.ToUpper(strings.TrimSpace(req.Currency))
	if err := req.Validate(); err != nil {
		return Result{}, err
	}
	hash := req.Hash()

	if c, err := s.cache.GetReplay(ctx, req.MerchantID, req.IdempotencyKey); err != nil {
		s.log.Warn("cache read failed, falling back to postgres", "err", err)
	} else if c != nil {
		if c.RequestHash != hash {
			return Result{}, ErrIdempotencyKeyReuse
		}
		return Result{Payment: c.Payment, Replayed: true}, nil
	}

	stored, created, err := s.store.InsertOrGet(ctx, Payment{
		ID:             newID(),
		MerchantID:     req.MerchantID,
		IdempotencyKey: req.IdempotencyKey,
		RequestHash:    hash,
		AmountCents:    req.AmountCents,
		Currency:       req.Currency,
		CustomerRef:    req.CustomerRef,
	}, s.cfg.PendingStaleAfter)
	if err != nil {
		return Result{}, err
	}
	if !created {
		if stored.RequestHash != hash {
			return Result{}, ErrIdempotencyKeyReuse
		}
		return Result{Payment: stored, Replayed: true}, nil
	}
	return s.charge(ctx, stored, hash)
}

func (s *Service) charge(ctx context.Context, p Payment, hash string) (Result, error) {
	chargeCtx, cancel := context.WithTimeout(ctx, s.cfg.ChargeTimeout)
	defer cancel()
	res, chargeErr := s.proc.Charge(chargeCtx, processor.ChargeRequest{
		IdempotencyKey: p.ID, // stable across retries, so the processor can dedupe as well
		AmountCents:    p.AmountCents,
		Currency:       p.Currency,
		CustomerRef:    p.CustomerRef,
	})

	// Record the outcome even if the client has already hung up. Losing the
	// result of a charge that really happened is the worst thing we can do.
	persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer persistCancel()

	var outcome Outcome
	switch {
	case chargeErr != nil:
		// Timeout or network error: we do NOT know whether the money moved.
		outcome = Outcome{To: StatusUnknown, Note: "processor error: " + chargeErr.Error(), NextReconcileIn: s.backoff(1)}
	case res.Approved:
		outcome = Outcome{To: StatusSucceeded, ProcessorRef: res.Ref, Note: "charge approved"}
	default:
		outcome = Outcome{To: StatusFailed, ProcessorRef: res.Ref, Reason: res.DeclineReason, Note: "charge declined"}
	}

	updated, _, err := s.store.ApplyOutcome(persistCtx, p.ID, outcome)
	if errors.Is(err, ErrIllegalTransition) {
		// A webhook already finalised this payment. Report what is stored.
		updated, err = s.store.Get(persistCtx, p.ID)
	}
	if err != nil {
		// The row stays "pending"; the reconciler will pick it up after PendingStaleAfter.
		return Result{}, err
	}

	if updated.Status.Terminal() {
		if err := s.cache.PutReplay(persistCtx, p.MerchantID, p.IdempotencyKey, hash, updated); err != nil {
			s.log.Warn("cache write failed", "err", err)
		}
	}
	return Result{Payment: updated}, nil
}

// GetPayment loads a payment by ID.
func (s *Service) GetPayment(ctx context.Context, id string) (Payment, error) {
	return s.store.Get(ctx, id)
}

// RunReconciler resolves uncertain payments until ctx is cancelled.
func (s *Service) RunReconciler(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				s.log.Error("reconcile pass failed", "err", err)
			}
		}
	}
}

// ReconcileOnce claims due payments and resolves them with bounded concurrency.
// It returns how many payments it looked at.
func (s *Service) ReconcileOnce(ctx context.Context) (int, error) {
	due, err := s.store.ClaimDue(ctx, s.cfg.ReconcileBatch, s.cfg.ReconcileLease)
	if err != nil {
		return 0, err
	}
	sem := make(chan struct{}, s.cfg.ReconcileWorkers)
	var wg sync.WaitGroup
	for _, p := range due {
		wg.Add(1)
		sem <- struct{}{}
		go func(p Payment) {
			defer wg.Done()
			defer func() { <-sem }()
			s.reconcileOne(ctx, p)
		}(p)
	}
	wg.Wait()
	return len(due), nil
}

// reconcileOne settles a payment whose outcome we do not know. It is safe to
// run at any time, even while the original request is still in flight:
// the processor dedupes by key and applyTx enforces the state machine.
func (s *Service) reconcileOne(ctx context.Context, p Payment) {
	log := s.log.With("payment_id", p.ID, "attempt", p.ReconcileAttempts)

	if p.ReconcileAttempts > s.cfg.MaxReconcileAttempts {
		if err := s.store.Park(ctx, p.ID); err != nil {
			log.Error("could not park payment", "err", err)
			return
		}
		log.Error("payment parked: needs manual review")
		return
	}

	rctx, cancel := context.WithTimeout(ctx, s.cfg.ChargeTimeout)
	defer cancel()

	// 1. Ask what happened.
	res, found, err := s.proc.Lookup(rctx, p.ID)
	// 2. If the processor never saw it, send it again. Same idempotency key,
	//    so a duplicate cannot charge twice.
	if err == nil && !found {
		res, err = s.proc.Charge(rctx, processor.ChargeRequest{
			IdempotencyKey: p.ID, AmountCents: p.AmountCents, Currency: p.Currency, CustomerRef: p.CustomerRef,
		})
	}
	if err != nil {
		wait := s.backoff(p.ReconcileAttempts)
		log.Warn("reconcile failed, will retry", "err", err, "retry_in", wait)
		if rerr := s.store.Reschedule(ctx, p.ID, wait); rerr != nil {
			log.Error("could not reschedule", "err", rerr)
		}
		return
	}

	outcome := Outcome{To: StatusFailed, ProcessorRef: res.Ref, Reason: res.DeclineReason, Note: "reconciled: declined"}
	if res.Approved {
		outcome = Outcome{To: StatusSucceeded, ProcessorRef: res.Ref, Note: "reconciled: approved"}
	}
	updated, changed, err := s.store.ApplyOutcome(ctx, p.ID, outcome)
	switch {
	case errors.Is(err, ErrIllegalTransition):
		log.Warn("reconcile result contradicts stored state", "err", err)
	case err != nil:
		log.Error("could not apply reconcile result", "err", err)
	case changed:
		log.Info("payment reconciled", "status", updated.Status)
	}
}

// backoff returns an exponential delay with jitter, capped at BackoffCap.
func (s *Service) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := s.cfg.BackoffCap
	if attempt < 30 {
		if exp := s.cfg.BackoffBase << (attempt - 1); exp > 0 && exp < d {
			d = exp
		}
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
