package payments

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"
)

//go:embed schema.sql
var schemaSQL string

// Store is the only code that talks to Postgres. Postgres is the source of
// truth for every money decision; Redis is never consulted for correctness.
type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// Migrate creates the tables if they do not exist yet.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, schemaSQL)
	return err
}

const paymentCols = `id, merchant_id, idempotency_key, request_hash, amount_cents, currency, customer_ref, status,
	COALESCE(processor_ref, ''), COALESCE(failure_reason, ''), reconcile_attempts, created_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

func scanPayment(r scanner) (Payment, error) {
	var p Payment
	var status string
	err := r.Scan(&p.ID, &p.MerchantID, &p.IdempotencyKey, &p.RequestHash, &p.AmountCents, &p.Currency,
		&p.CustomerRef, &status, &p.ProcessorRef, &p.FailureReason, &p.ReconcileAttempts, &p.CreatedAt, &p.UpdatedAt)
	p.Status = Status(status)
	return p, err
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// InsertOrGet creates the payment, or returns the existing one for the same
// (merchant, idempotency key). created is true only for the single caller whose
// INSERT won; that caller is the only one allowed to charge the processor.
//
// ON CONFLICT DO NOTHING makes Postgres wait for a concurrent in-flight insert
// of the same key, so concurrent duplicates cannot both win.
func (s *Store) InsertOrGet(ctx context.Context, p Payment, staleAfter time.Duration) (Payment, bool, error) {
	var out Payment
	created := false
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `
			INSERT INTO payments (id, merchant_id, idempotency_key, request_hash, amount_cents, currency,
			                      customer_ref, status, next_reconcile_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', now() + ($8::bigint * interval '1 millisecond'))
			ON CONFLICT (merchant_id, idempotency_key) DO NOTHING
			RETURNING `+paymentCols,
			p.ID, p.MerchantID, p.IdempotencyKey, p.RequestHash, p.AmountCents, p.Currency, p.CustomerRef,
			staleAfter.Milliseconds())

		inserted, err := scanPayment(row)
		switch {
		case err == nil:
			created, out = true, inserted
			_, err = tx.ExecContext(ctx, `
				INSERT INTO payment_events (payment_id, from_status, to_status, note)
				VALUES ($1, NULL, 'pending', 'created')`, out.ID)
			return err
		case errors.Is(err, sql.ErrNoRows):
			existing, err := scanPayment(tx.QueryRowContext(ctx,
				`SELECT `+paymentCols+` FROM payments WHERE merchant_id = $1 AND idempotency_key = $2`,
				p.MerchantID, p.IdempotencyKey))
			out = existing
			return err
		default:
			return err
		}
	})
	return out, created, err
}

// Get loads one payment by ID.
func (s *Store) Get(ctx context.Context, id string) (Payment, error) {
	p, err := scanPayment(s.db.QueryRowContext(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	return p, err
}

// Outcome describes a state change to apply to a payment.
type Outcome struct {
	To              Status
	ProcessorRef    string
	Reason          string
	Note            string
	NextReconcileIn time.Duration // when to look at it again; zero clears the schedule
}

// ApplyOutcome is the ONE code path that changes a payment's status. The
// synchronous charge, the reconciler and the webhook handler all go through
// applyTx, so the rules below are enforced everywhere:
//
//   - the row is locked, so two writers cannot interleave
//   - only legal transitions are allowed
//   - re-applying the same status is a harmless no-op
//   - money is booked in the ledger in the same transaction as success
func (s *Store) ApplyOutcome(ctx context.Context, id string, o Outcome) (Payment, bool, error) {
	var out Payment
	changed := false
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		out, changed, err = s.applyTx(ctx, tx, id, o)
		return err
	})
	return out, changed, err
}

func (s *Store) applyTx(ctx context.Context, tx *sql.Tx, id string, o Outcome) (Payment, bool, error) {
	p, err := scanPayment(tx.QueryRowContext(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Payment{}, false, ErrNotFound
	}
	if err != nil {
		return Payment{}, false, err
	}
	if p.Status == o.To {
		return p, false, nil
	}
	if !CanTransition(p.Status, o.To) {
		return p, false, fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, p.Status, o.To)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE payments
		   SET status = $2,
		       processor_ref = COALESCE(NULLIF($3, ''), processor_ref),
		       failure_reason = NULLIF($4, ''),
		       next_reconcile_at = CASE WHEN $5::bigint > 0
		                                THEN now() + ($5::bigint * interval '1 millisecond') END,
		       updated_at = now()
		 WHERE id = $1`,
		id, string(o.To), o.ProcessorRef, o.Reason, o.NextReconcileIn.Milliseconds())
	if err != nil {
		return Payment{}, false, err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO payment_events (payment_id, from_status, to_status, note)
		VALUES ($1, $2, $3, NULLIF($4, ''))`, id, string(p.Status), string(o.To), o.Note)
	if err != nil {
		return Payment{}, false, err
	}

	if o.To == StatusSucceeded {
		// Double-entry: the customer's funds are debited and the merchant is
		// credited by the same amount. UNIQUE (payment_id, account, direction)
		// makes a second booking of the same payment impossible.
		_, err = tx.ExecContext(ctx, `
			INSERT INTO ledger_entries (payment_id, account, direction, amount_cents)
			VALUES ($1, 'customer_funds', 'debit', $2),
			       ($1, $3, 'credit', $2)`, id, p.AmountCents, "merchant:"+p.MerchantID)
		if err != nil {
			return Payment{}, false, err
		}
	}

	p.Status = o.To
	if o.ProcessorRef != "" {
		p.ProcessorRef = o.ProcessorRef
	}
	p.FailureReason = o.Reason
	return p, true, nil
}

// WebhookResult says what happened to a processor webhook.
type WebhookResult string

const (
	WebhookApplied   WebhookResult = "applied"
	WebhookDuplicate WebhookResult = "duplicate"
	WebhookIgnored   WebhookResult = "ignored"
)

// ApplyWebhook records the event ID and applies the state change in ONE
// transaction. If it were two steps, a crash between them would leave an event
// marked "seen" but never applied, and the processor's retry would be dropped.
func (s *Store) ApplyWebhook(ctx context.Context, eventID, paymentID string, o Outcome) (WebhookResult, error) {
	result := WebhookApplied
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO webhook_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			result = WebhookDuplicate
			return nil
		}
		_, _, err = s.applyTx(ctx, tx, paymentID, o)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrIllegalTransition):
			// Nothing was written. Keep the event ID so retries stop, and let the
			// caller log it: a processor contradicting a final state needs a human.
			result = WebhookIgnored
			return nil
		default:
			return err // includes ErrNotFound: roll back so the event can be retried
		}
	})
	return result, err
}

// ClaimDue leases unfinished payments that are due for reconciliation. The
// lease pushes next_reconcile_at forward in the same statement, and
// FOR UPDATE SKIP LOCKED lets many workers or instances claim without
// colliding or waiting on each other.
func (s *Store) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]Payment, error) {
	rows, err := s.db.QueryContext(ctx, `
		UPDATE payments
		   SET next_reconcile_at = now() + ($2::bigint * interval '1 millisecond'),
		       reconcile_attempts = reconcile_attempts + 1,
		       updated_at = now()
		 WHERE id IN (
		       SELECT id FROM payments
		        WHERE status IN ('pending', 'unknown') AND next_reconcile_at <= now()
		        ORDER BY next_reconcile_at
		        LIMIT $1
		        FOR UPDATE SKIP LOCKED)
		RETURNING `+paymentCols, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Payment
	for rows.Next() {
		p, err := scanPayment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Reschedule sets when an unfinished payment should be looked at next.
func (s *Store) Reschedule(ctx context.Context, id string, in time.Duration) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE payments
		   SET next_reconcile_at = now() + ($2::bigint * interval '1 millisecond'), updated_at = now()
		 WHERE id = $1 AND status IN ('pending', 'unknown')`, id, in.Milliseconds())
	return err
}

// Park stops automatic reconciliation and leaves a note for a human.
func (s *Store) Park(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `
		WITH p AS (
		    UPDATE payments SET next_reconcile_at = NULL, updated_at = now()
		     WHERE id = $1 AND status IN ('pending', 'unknown')
		 RETURNING id, status)
		INSERT INTO payment_events (payment_id, from_status, to_status, note)
		SELECT id, status, status, 'parked: needs manual review' FROM p`, id)
	return err
}

// LedgerTotals returns the entry count and the summed debits and credits. The
// invariant that matters: debits always equal credits.
func (s *Store) LedgerTotals(ctx context.Context) (entries int, debits, credits int64, err error) {
	err = s.db.QueryRowContext(ctx, `
		SELECT count(*),
		       COALESCE(SUM(amount_cents) FILTER (WHERE direction = 'debit'), 0),
		       COALESCE(SUM(amount_cents) FILTER (WHERE direction = 'credit'), 0)
		  FROM ledger_entries`).Scan(&entries, &debits, &credits)
	return
}
