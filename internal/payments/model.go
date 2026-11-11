// Package payments holds the domain logic: the payment state machine, the
// Postgres store, and the service that creates and reconciles payments.
package payments

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Status is where a payment is in its life.
//
//	pending   -> we created the row and are talking to the processor
//	unknown   -> the processor call timed out; the charge MAY have happened
//	succeeded -> money moved (terminal)
//	failed    -> the processor declined (terminal)
type Status string

const (
	StatusPending   Status = "pending"
	StatusUnknown   Status = "unknown"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

// Terminal reports whether the status can never change again.
func (s Status) Terminal() bool { return s == StatusSucceeded || s == StatusFailed }

// legalTransitions is the whole state machine. Anything not listed is illegal.
var legalTransitions = map[Status][]Status{
	StatusPending: {StatusSucceeded, StatusFailed, StatusUnknown},
	StatusUnknown: {StatusSucceeded, StatusFailed},
}

// CanTransition reports whether moving from one status to another is allowed.
func CanTransition(from, to Status) bool {
	for _, next := range legalTransitions[from] {
		if next == to {
			return true
		}
	}
	return false
}

var (
	ErrNotFound            = errors.New("payment not found")
	ErrIdempotencyKeyReuse = errors.New("idempotency key was already used with a different request")
	ErrIllegalTransition   = errors.New("illegal status transition")
	ErrValidation          = errors.New("invalid request")
)

// Payment is one charge. Money is always integer cents, never floats.
type Payment struct {
	ID                string    `json:"id"`
	MerchantID        string    `json:"merchant_id"`
	IdempotencyKey    string    `json:"-"`
	RequestHash       string    `json:"-"`
	AmountCents       int64     `json:"amount_cents"`
	Currency          string    `json:"currency"`
	CustomerRef       string    `json:"customer_ref"`
	Status            Status    `json:"status"`
	ProcessorRef      string    `json:"processor_ref,omitempty"`
	FailureReason     string    `json:"failure_reason,omitempty"`
	ReconcileAttempts int       `json:"-"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// CreateRequest is what a merchant sends to create a payment.
type CreateRequest struct {
	MerchantID     string
	IdempotencyKey string
	AmountCents    int64
	Currency       string
	CustomerRef    string
}

// Validate checks the request before we touch any storage.
func (r CreateRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.MerchantID) == "":
		return fmt.Errorf("%w: merchant id is required", ErrValidation)
	case r.IdempotencyKey == "" || len(r.IdempotencyKey) > 255:
		return fmt.Errorf("%w: idempotency key must be 1 to 255 characters", ErrValidation)
	case r.AmountCents <= 0:
		return fmt.Errorf("%w: amount_cents must be positive", ErrValidation)
	case !validCurrency(r.Currency):
		return fmt.Errorf("%w: currency must be a 3-letter code", ErrValidation)
	case strings.TrimSpace(r.CustomerRef) == "":
		return fmt.Errorf("%w: customer_ref is required", ErrValidation)
	}
	return nil
}

// Hash fingerprints the business content of a request. Reusing an idempotency
// key with a different hash is a client bug and is rejected.
func (r CreateRequest) Hash() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s|%s", r.MerchantID, r.AmountCents, r.Currency, r.CustomerRef)))
	return hex.EncodeToString(sum[:])
}

func validCurrency(c string) bool {
	if len(c) != 3 {
		return false
	}
	for _, ch := range c {
		if ch < 'A' || ch > 'Z' {
			return false
		}
	}
	return true
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ValidID reports whether s looks like a payment ID (a lowercase UUID).
func ValidID(s string) bool { return uuidPattern.MatchString(s) }

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
