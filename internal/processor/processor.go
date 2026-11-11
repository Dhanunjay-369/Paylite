// Package processor is the boundary to the outside payment processor.
// The service depends on the small Processor interface, so a real adapter
// (Stripe, Adyen, ...) can replace the fake without touching business logic.
package processor

import "context"

// ChargeRequest is sent to the processor. IdempotencyKey is OUR payment ID:
// it is stable across retries, so the processor can deduplicate on its side too.
type ChargeRequest struct {
	IdempotencyKey string
	AmountCents    int64
	Currency       string
	CustomerRef    string
}

// ChargeResult is the processor's answer.
type ChargeResult struct {
	Ref           string
	Approved      bool
	DeclineReason string
}

// Processor is what the service needs from a payment processor.
type Processor interface {
	// Charge moves money. Calling it again with the same IdempotencyKey must
	// return the original result instead of charging twice.
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
	// Lookup asks what happened to an earlier charge. found is false when the
	// processor never received it.
	Lookup(ctx context.Context, idempotencyKey string) (res ChargeResult, found bool, err error)
}
