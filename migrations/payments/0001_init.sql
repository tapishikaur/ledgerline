-- Payments service database.

-- Every OrderCreated event handled. The primary key is what makes the handler idempotent:
-- INSERT ... ON CONFLICT DO NOTHING inserts nothing for an event seen before.
CREATE TABLE processed_events (
  event_id     uuid        PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);

-- The simulated payment provider's own records. It stands in for an external system (a
-- card processor), so it is written in its own transaction: a charge survives even if the
-- payments transaction that asked for it rolls back. Like real providers, it is idempotent
-- on a key: the same key returns the same charge instead of charging again.
CREATE TABLE provider_charges (
  idempotency_key text        PRIMARY KEY,              -- the order_id
  charge_id       uuid        NOT NULL DEFAULT gen_random_uuid(),
  amount_minor    bigint      NOT NULL CHECK (amount_minor > 0),
  calls           integer     NOT NULL DEFAULT 1,       -- how many times it was asked (retries included)
  created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE payments (
  payment_id   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id     text        NOT NULL UNIQUE,             -- at most one payment per order
  customer_id  text        NOT NULL,
  amount_minor bigint      NOT NULL CHECK (amount_minor > 0),
  charge_id    uuid        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
  id           bigserial   PRIMARY KEY,
  event_id     uuid        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  event_type   text        NOT NULL,
  aggregate_id text        NOT NULL,
  payload      bytea       NOT NULL,
  traceparent  text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  sent_at      timestamptz,
  attempts     integer     NOT NULL DEFAULT 0,
  last_error   text
);
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
