# One trace, one order, every service

Trace `92370c5423096a289cdfaf1d7e46c7a4` for order `trace-1791944427-00000000`, fetched from Jaeger's API: 8 spans from 5 services, all with the same trace ID.

| Start (ms) | Duration (ms) | Service | Span |
|---|---|---|---|
| 0.0 | 3.9 | orders | ledgerline.v1.Orders/CreateOrder |
| 35.5 | 59.3 | outbox-worker-payments | outbox.deliver OrderCreated |
| 76.5 | 18.0 | outbox-worker-payments | ledgerline.v1.Payments/HandleOrderCreated |
| 78.9 | 14.5 | payments | ledgerline.v1.Payments/HandleOrderCreated |
| 84.3 | 2.1 | payments | provider.Charge |
| 101.2 | 81.2 | outbox-worker-ledger | outbox.deliver PaymentCaptured |
| 177.5 | 4.9 | outbox-worker-ledger | ledgerline.v1.Ledger/ApplyPaymentCaptured |
| 178.4 | 3.8 | ledger | ledgerline.v1.Ledger/ApplyPaymentCaptured |

**Result: PASS**: spans from orders, outbox-worker-payments, payments, outbox-worker-ledger, ledger are all in one trace.
