-- PayLite schema. Safe to run repeatedly (everything is IF NOT EXISTS).

-- One row per payment. UNIQUE (merchant_id, idempotency_key) is the safety net
-- that makes "charge exactly once" true even if Redis is empty or down.
CREATE TABLE IF NOT EXISTS payments (
    id                 uuid PRIMARY KEY,
    merchant_id        text        NOT NULL,
    idempotency_key    text        NOT NULL,
    request_hash       text        NOT NULL,
    amount_cents       bigint      NOT NULL CHECK (amount_cents > 0),
    currency           char(3)     NOT NULL,
    customer_ref       text        NOT NULL,
    status             text        NOT NULL CHECK (status IN ('pending', 'unknown', 'succeeded', 'failed')),
    processor_ref      text,
    failure_reason     text,
    reconcile_attempts int         NOT NULL DEFAULT 0,
    next_reconcile_at  timestamptz,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (merchant_id, idempotency_key)
);

-- The reconciler only looks at unfinished payments that are due.
CREATE INDEX IF NOT EXISTS payments_reconcile_idx
    ON payments (next_reconcile_at)
    WHERE status IN ('pending', 'unknown') AND next_reconcile_at IS NOT NULL;

-- Append-only audit trail of every state change.
CREATE TABLE IF NOT EXISTS payment_events (
    id          bigserial   PRIMARY KEY,
    payment_id  uuid        NOT NULL REFERENCES payments (id),
    from_status text,
    to_status   text        NOT NULL,
    note        text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Append-only double-entry ledger. A payment can be booked at most once:
-- the UNIQUE constraint rejects a second debit or credit for the same payment.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id           bigserial   PRIMARY KEY,
    payment_id   uuid        NOT NULL REFERENCES payments (id),
    account      text        NOT NULL,
    direction    text        NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_cents bigint      NOT NULL CHECK (amount_cents > 0),
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (payment_id, account, direction)
);

-- Processed webhook event IDs. Written in the SAME transaction as the state
-- change, so an event is never "seen but not applied".
CREATE TABLE IF NOT EXISTS webhook_events (
    event_id    text        PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now()
);
