-- Ledger service database: double-entry bookkeeping.

-- Every PaymentCaptured event applied. Its primary key makes applying idempotent: the
-- handler inserts here first with ON CONFLICT DO NOTHING, and stops if nothing was inserted.
CREATE TABLE applied_events (
  event_id   uuid        PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

-- Each payment becomes exactly two entries of the same amount: a debit and a credit.
CREATE TABLE entries (
  entry_id     bigserial   PRIMARY KEY,
  event_id     uuid        NOT NULL REFERENCES applied_events (event_id),
  order_id     text        NOT NULL,
  account      text        NOT NULL,
  direction    char(1)     NOT NULL CHECK (direction IN ('D', 'C')),
  amount_minor bigint      NOT NULL CHECK (amount_minor > 0),
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (event_id, direction)                          -- never two debits (or credits) for one event
);
CREATE INDEX entries_order ON entries (order_id);
