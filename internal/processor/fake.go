package processor

import (
	"context"
	"sync"
	"time"
)

// Mode controls how the fake processor misbehaves.
type Mode int

const (
	// Approve charges the card normally.
	Approve Mode = iota
	// Decline refuses the charge.
	Decline
	// ApplyThenTimeout applies the charge, then never answers: the nastiest
	// real-world case. Money moved, but the caller sees a timeout.
	ApplyThenTimeout
	// DropRequest never receives the charge and never answers: the caller sees
	// a timeout, but no money moved.
	DropRequest
)

func (m Mode) String() string {
	switch m {
	case Approve:
		return "approve"
	case Decline:
		return "decline"
	case ApplyThenTimeout:
		return "apply_then_timeout"
	case DropRequest:
		return "drop_request"
	}
	return "unknown"
}

// ParseMode converts a name into a Mode.
func ParseMode(s string) (Mode, bool) {
	for _, m := range []Mode{Approve, Decline, ApplyThenTimeout, DropRequest} {
		if m.String() == s {
			return m, true
		}
	}
	return Approve, false
}

// Fake is an in-memory processor that deduplicates by idempotency key, the way
// real processors do, and counts how many times money actually moved.
type Fake struct {
	mu       sync.Mutex
	mode     Mode
	latency  time.Duration
	charges  map[string]ChargeResult
	approved int
	calls    int
}

func NewFake() *Fake { return &Fake{charges: make(map[string]ChargeResult)} }

func (f *Fake) SetMode(m Mode) { f.mu.Lock(); f.mode = m; f.mu.Unlock() }

func (f *Fake) SetLatency(d time.Duration) { f.mu.Lock(); f.latency = d; f.mu.Unlock() }

// Mode returns the current mode.
func (f *Fake) Mode() Mode { f.mu.Lock(); defer f.mu.Unlock(); return f.mode }

// ApprovedCharges is how many times money really moved. The whole point of the
// project is that this never exceeds one per payment.
func (f *Fake) ApprovedCharges() int { f.mu.Lock(); defer f.mu.Unlock(); return f.approved }

// Calls is how many times Charge was called, including repeats.
func (f *Fake) Calls() int { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }

func (f *Fake) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	f.mu.Lock()
	f.calls++
	if res, ok := f.charges[req.IdempotencyKey]; ok {
		f.mu.Unlock()
		return res, nil
	}
	mode, latency := f.mode, f.latency
	f.mu.Unlock()

	if latency > 0 {
		select {
		case <-time.After(latency):
		case <-ctx.Done():
			return ChargeResult{}, ctx.Err()
		}
	}
	if mode == DropRequest {
		<-ctx.Done()
		return ChargeResult{}, ctx.Err()
	}

	f.mu.Lock()
	if res, ok := f.charges[req.IdempotencyKey]; ok { // a concurrent duplicate got there first
		f.mu.Unlock()
		return res, nil
	}
	res := ChargeResult{Ref: "ch_" + req.IdempotencyKey[:8], Approved: mode != Decline}
	if !res.Approved {
		res.DeclineReason = "card_declined"
	} else {
		f.approved++
	}
	f.charges[req.IdempotencyKey] = res
	f.mu.Unlock()

	if mode == ApplyThenTimeout {
		<-ctx.Done()
		return ChargeResult{}, ctx.Err()
	}
	return res, nil
}

func (f *Fake) Lookup(_ context.Context, idempotencyKey string) (ChargeResult, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res, ok := f.charges[idempotencyKey]
	return res, ok, nil
}
