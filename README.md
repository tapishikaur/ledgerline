# Ledgerline

[![CI](https://github.com/tapishikaur/ledgerline/actions/workflows/ci.yml/badge.svg)](https://github.com/tapishikaur/ledgerline/actions/workflows/ci.yml)

Event-driven orders, payments and a double-entry ledger in Go, built so that **a payment is never charged twice and
no ledger entry is ever lost**, even when services are killed mid-request. No message broker: a **transactional
outbox** in PostgreSQL and **idempotent consumers** give effectively-once processing.

## Results

| Claim | Result | Record |
|---|---|---|
| No duplicate or lost ledger entries across 50 crashes in a 500,000-event run | **0 breaks**: 250,000 orders, payments and provider charges; 500,000 ledger entries; debits = credits = orders total | [results/chaos-500k-events.md](results/chaos-500k-events.md) |
| One trace follows an order through every service and both outbox workers | **8 spans from 5 services**, one trace ID, read back from Jaeger's API | [results/trace-check.md](results/trace-check.md) |
| The check can fail | With the ledger's duplicate protection removed, the same procedure gives **FAIL, 127 breaks** | [results/planted-bug-check.md](results/planted-bug-check.md) |

The chaos run used a 2-core Codespace, with the whole stack on that one machine. While `loadgen` created 250,000
orders, an outbox worker or the ledger was killed with `docker kill` (SIGKILL: no shutdown, no flush) 50 times at
random moments. Each was then started again. Once both outboxes had drained, the `reconcile` tool compared all three
databases. CI runs a smaller version on every push: 2,000 orders and 5 kills, plus the trace check.

![One order's trace in Jaeger: orders, outbox worker, payments, provider, outbox worker, ledger](results/trace.png)

## How it works

```mermaid
flowchart LR
  C[client] -->|gRPC CreateOrder| O[orders]
  O -->|"one transaction: order + OrderCreated event"| ODB[(orders DB: orders, outbox)]
  ODB -->|"worker: SELECT … FOR UPDATE SKIP LOCKED"| WA[outbox worker A]
  WA -->|gRPC HandleOrderCreated| P[payments]
  P -->|"idempotent charge (key = order_id)"| PR[payment provider]
  P -->|"one transaction: dedupe + payment + PaymentCaptured"| PDB[(payments DB: payments, outbox)]
  PDB --> WB[outbox worker B]
  WB -->|gRPC ApplyPaymentCaptured| L[ledger]
  L -->|"one transaction: dedupe + debit + credit"| LDB[(ledger DB: entries)]
```

1. **Transactional outbox.** A service saves its business change and an "event happened" row in its `outbox` table
   **in the same transaction**, so either both are saved or neither is.
2. **Outbox worker.** The worker claims unsent rows with `SELECT … FOR UPDATE SKIP LOCKED`, so several workers can share
   one outbox without delivering the same row at the same moment. It calls the next service over gRPC, marks the rows
   sent and commits. If it dies before the commit, the locks are released and the rows are delivered again. That makes
   delivery **at-least-once**.
3. **Idempotent consumers.** Every event has a unique `event_id`. The receiver inserts it with
   `INSERT … ON CONFLICT DO NOTHING` in the same transaction as its own change. A second delivery inserts nothing and
   changes nothing. At-least-once delivery plus idempotent consumers gives **effectively-once** processing.
4. **No double charge.** The payment provider is simulated. Like the idempotency keys of real card processors, it is
   idempotent on the order ID. Asking again after a crash or a lost reply returns the original charge.
5. **Reconciliation.** `ledgerline reconcile` compares the three databases. Every order needs exactly one payment, and
   that payment's charge must be the provider's. Every payment needs exactly one debit and one credit for the right
   amount. Every outbox row must be delivered, and debits must equal credits must equal the orders total.

### What a crash can interrupt, and why nothing breaks

| Killed while… | What's left behind | What happens next |
|---|---|---|
| orders is in its transaction | nothing: the order and its event roll back together | the client gets an error and retries; `CreateOrder` is idempotent on `order_id` |
| a worker is delivering, before the receiver commits | the receiver's transaction rolls back; the outbox row is still unsent | the row is delivered again |
| a worker has delivered, but before it marks the row sent | the receiver has applied the event; the outbox row is still unsent | redelivered; the receiver finds the `event_id` and does nothing |
| payments after the provider charged, before it commits | a charge at the provider, but no payment row | redelivered; the provider returns **the same** charge for that order, and payments commits it once |
| a reply is lost on the way back | the work is done, but the sender doesn't know | the sender retries, and it's a duplicate as above |
| the ledger mid-transaction | nothing: the dedupe marker and both entries roll back together | redelivered and applied once |

## Why this choice

- **An outbox instead of a message broker.** Writing to a database and then to a broker is two separate writes, and a
  crash between them loses one ("the dual-write problem"). A broker would still need an outbox to be correct. With
  the outbox, PostgreSQL is the only moving part.
- **`FOR UPDATE SKIP LOCKED`.** Workers can be added or killed at any time without coordinating. A killed worker's
  rows are unlocked by PostgreSQL, not by the worker.
- **The dedupe marker goes in the same transaction as the change.** Checking for a duplicate first and writing later
  leaves a gap that two concurrent deliveries can both pass through. With the marker in the same transaction, the
  second insert waits for the first one to commit, then inserts nothing.
- **The provider is called outside any database transaction.** Holding a transaction open across a network call ties up
  a connection for the whole call. In an early version, the end-to-end test deadlocked: every pooled connection was
  held by a transaction waiting on a provider call, and that call needed a connection too. Calling the provider before
  the transaction and making it idempotent removed the problem.
- **Handlers finish even if the caller disappears.** The gRPC handlers detach from the caller's cancellation, with a
  10-second limit of their own. So a killed worker doesn't throw away work that was nearly done, and the redelivery
  finds the work already applied.
- **Trace context is stored in the outbox row.** The worker continues the trace that `CreateOrder` started, so one trace
  covers the asynchronous hops too. Sampling is decided once at the root and followed everywhere
  (`TRACE_SAMPLE`, default 1.0).
- **One binary, one image.** `ledgerline orders | payments | ledger | outbox-worker | loadgen | reconcile | health`.
  Compose runs the same image with different commands.
- **Generated gRPC code is committed.** You can build without protoc. CI regenerates it with a pinned protoc (33.5) and
  fails if anything differs.

## Run it

You need Docker with Compose. A GitHub Codespace on this repo has everything, including Go 1.26 and Python 3.

```bash
docker compose up --build -d --wait         # PostgreSQL 18, Jaeger, 3 services, 2 outbox workers
docker run --rm --network ledgerline_default ledgerline:dev loadgen -addr orders:50051 -n 1000
# Jaeger UI: http://localhost:16686

bash scripts/chaos.sh 2000 5 7 /tmp/chaos.md          # small chaos test (what CI runs)
bash scripts/chaos.sh                                 # the full one: 250,000 orders, 50 kills (about 16 min on 2 cores)
python3 scripts/trace_check.py --out /tmp/trace.md    # one order, one trace, all services
bash scripts/k6.sh 300 60s /tmp/k6.txt                # k6 load test: CreateOrder at a constant rate

docker compose down -v
```

Tests (unit, integration and an end-to-end test with crashing workers) need a PostgreSQL server. They create and drop
their own databases:

```bash
LEDGERLINE_TEST_DB=postgres://user:pass@localhost:5432/postgres bash scripts/test.sh -race -count=1
bash scripts/gen.sh     # regenerate gen/ from proto/ (needs protoc 33.5)
```

The payment provider can be made slow or unreliable: `PROVIDER_LATENCY_MS`, `PROVIDER_FAIL_RATE` and
`PROVIDER_LOST_REPLY_RATE` (the charge happens, but the reply is dropped).

## Layout

| Path | What |
|---|---|
| `proto/`, `gen/` | the gRPC API and its generated Go code |
| `cmd/ledgerline/` | the single binary: services, worker, loadgen, reconcile, health |
| `internal/orders`, `payments`, `ledger` | the three services |
| `internal/outbox` | writing events in a transaction, and the delivery worker |
| `internal/reconcile` | the cross-database checks |
| `internal/e2e` | all services in one test process, with workers killed at random |
| `migrations/` | each service's schema, embedded in the binary |
| `scripts/`, `loadtest/` | chaos test, trace check, k6 load test, local PostgreSQL, code generation |

## Limitations

- The payment provider is simulated: configurable delay, failures and lost replies, but no real money.
- Not included: refunds, currencies, multiple merchants, authentication.
- Docker Compose only, not a cluster. The three services have separate databases, but they share one PostgreSQL
  server.
- Events are delivered in parallel, so they aren't guaranteed to arrive in order. That's fine here, because each
  event stands alone.
- An event that keeps failing is retried forever (`attempts` and `last_error` are recorded). There's no dead-letter
  queue, and sent outbox rows aren't cleaned up.
- Each event costs its own gRPC call and its own transaction at the receiver.

## Next steps

- **Batch delivery.** Send one gRPC call and run one receiver transaction per batch of events, with each event
  keeping its own trace context. This cuts the per-event cost.
- A dead-letter table after N attempts, and a job that deletes old sent outbox rows.
- `LISTEN/NOTIFY` to wake the worker instead of polling.

## License

MIT
