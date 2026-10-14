# Chaos test: 500000 events, 50 forced crashes

The full Docker Compose stack ran while `loadgen` created 250000 orders (each produces OrderCreated and
PaymentCaptured: 500000 events). Meanwhile an outbox worker or the ledger service was killed with
`docker kill` (SIGKILL: no shutdown, no flush) 50 times at random moments (seed 7), and started again.
After both outboxes drained, every order, provider charge, payment and ledger entry was reconciled.

Load: loadgen: 250000 orders (250000 created, 0 already existed, 0 retries) in 474.4 s, 527 orders/s

Total time including the drain: 931 s. Machine: 2 CPUs, Linux 6.8.0-1064-azure x86_64.

| Kill | At | Container | Down for |
|---|---|---|---|
| 1 | 10 s | ledger | 0.9 s |
| 2 | 20 s | ledger | 1.9 s |
| 3 | 35 s | worker-payments | 1.5 s |
| 4 | 43 s | worker-payments | 0.6 s |
| 5 | 58 s | worker-payments | 1.3 s |
| 6 | 71 s | ledger | 0.1 s |
| 7 | 78 s | worker-orders | 1.0 s |
| 8 | 92 s | worker-payments | 2.6 s |
| 9 | 101 s | ledger | 2.5 s |
| 10 | 109 s | worker-orders | 1.6 s |
| 11 | 120 s | worker-payments | 1.6 s |
| 12 | 136 s | ledger | 2.3 s |
| 13 | 147 s | ledger | 1.7 s |
| 14 | 155 s | worker-payments | 1.1 s |
| 15 | 172 s | ledger | 2.5 s |
| 16 | 181 s | worker-payments | 0.6 s |
| 17 | 196 s | ledger | 1.6 s |
| 18 | 209 s | worker-orders | 2.3 s |
| 19 | 218 s | ledger | 0.1 s |
| 20 | 231 s | ledger | 1.9 s |
| 21 | 246 s | worker-payments | 1.3 s |
| 22 | 261 s | worker-payments | 0.8 s |
| 23 | 270 s | worker-orders | 1.2 s |
| 24 | 283 s | worker-orders | 2.4 s |
| 25 | 298 s | worker-payments | 2.9 s |
| 26 | 312 s | ledger | 0.9 s |
| 27 | 321 s | ledger | 0.3 s |
| 28 | 328 s | ledger | 0.0 s |
| 29 | 342 s | worker-orders | 2.4 s |
| 30 | 354 s | worker-orders | 0.2 s |
| 31 | 365 s | worker-orders | 1.2 s |
| 32 | 375 s | worker-orders | 2.6 s |
| 33 | 383 s | worker-orders | 2.6 s |
| 34 | 391 s | worker-payments | 1.3 s |
| 35 | 401 s | ledger | 0.2 s |
| 36 | 408 s | ledger | 2.0 s |
| 37 | 418 s | worker-orders | 1.0 s |
| 38 | 428 s | worker-orders | 1.8 s |
| 39 | 445 s | worker-payments | 2.6 s |
| 40 | 452 s | ledger | 2.2 s |
| 41 | 461 s | ledger | 1.7 s |
| 42 | 473 s | worker-payments | 1.9 s |
| 43 | 488 s | ledger | 1.8 s |
| 44 | 503 s | worker-orders | 1.0 s |
| 45 | 519 s | worker-payments | 1.6 s |
| 46 | 530 s | worker-payments | 2.6 s |
| 47 | 540 s | worker-payments | 2.0 s |
| 48 | 556 s | worker-orders | 1.0 s |
| 49 | 566 s | worker-orders | 2.5 s |
| 50 | 575 s | worker-orders | 2.4 s |

## Reconciliation after 50 kills

**Result: PASS, 0 breaks**

| Compared | Count |
|---|---|
| Orders | 250000 |
| Payments | 250000 |
| Provider charges | 250000 |
| Ledger entries | 500000 |
| Orders total / debits / credits (minor units) | 12507426325 / 12507426325 / 12507426325 |

| Check | Breaks |
|---|---|
| Order without a payment (lost) | 0 |
| Payment without an order | 0 |
| Charged by the provider but no payment recorded | 0 |
| Payment's charge isn't the provider's charge | 0 |
| Amount differs from the order | 0 |
| Missing ledger entry | 0 |
| Duplicate ledger entry | 0 |
| Ledger entry without a payment | 0 |
| Outbox events never delivered | 0 |
| Debits = credits = orders total | yes |
