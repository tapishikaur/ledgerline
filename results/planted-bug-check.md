# Planted-bug check: can the chaos test fail?

A reconciliation that always says "0 breaks" proves nothing unless it can fail. So a real bug was
planted on purpose, in a temporary copy of the code that was never committed. The ledger lost **both**
of its duplicate protections:

- its idempotency check (an event already in `applied_events` is skipped), and
- the database rule that one event can never have two debits (`UNIQUE (event_id, direction)`).

Then the same chaos procedure ran: 20,000 orders, 8 random `docker kill`s of a worker or the ledger
(seed 11), drain, reconcile. Redeliveries after the crashes are exactly what the protections exist for:

**Result: FAIL, 127 breaks.** 126 orders got two debits and two credits, and the books stopped balancing:

| | Minor units |
|---|---|
| Orders total | 998,228,361 |
| Debits | 1,004,196,463 |
| Credits | 1,004,196,463 |

| Check | Breaks |
|---|---|
| Duplicate ledger entry | 126 |
| Debits = credits = orders total | **no** |
| Every other check | 0 |

With the protections in place (the committed code), the full 500,000-event run with 50 kills
reconciles with 0 breaks: [chaos-500k-events.md](chaos-500k-events.md).

Why both had to go (from the code; this was not run separately): with only the unique constraint
removed, the idempotency check still skips the duplicate. With only the idempotency check removed, the
constraint rejects the second debit, so the duplicate delivery fails instead of corrupting the books.
