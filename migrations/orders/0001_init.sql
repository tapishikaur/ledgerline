-- Orders service database.

CREATE TABLE orders (
  order_id     text        PRIMARY KEY,                 -- client-chosen: a retry can't create a second order
  customer_id  text        NOT NULL,
  amount_minor bigint      NOT NULL CHECK (amount_minor > 0),
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- Events to deliver. Written in the same transaction as the orders row, so an order and its
-- OrderCreated event are saved together or not at all.
CREATE TABLE outbox (
  id           bigserial   PRIMARY KEY,                 -- delivery order
  event_id     uuid        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
  event_type   text        NOT NULL,
  aggregate_id text        NOT NULL,                    -- the order the event is about
  payload      bytea       NOT NULL,                    -- the event message, protobuf-encoded
  traceparent  text,                                    -- W3C trace context, so the trace continues in the worker
  created_at   timestamptz NOT NULL DEFAULT now(),
  sent_at      timestamptz,                             -- NULL until delivered and acknowledged
  attempts     integer     NOT NULL DEFAULT 0,
  last_error   text
);

-- The worker only ever looks at unsent rows: keep that scan small however big the table grows.
CREATE INDEX outbox_unsent ON outbox (id) WHERE sent_at IS NULL;
