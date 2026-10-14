package reconcile

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tapishikaur/ledgerline/internal/testdb"
)

type dbs struct{ orders, payments, ledger *pgxpool.Pool }

func setup(t *testing.T) dbs {
	return dbs{testdb.New(t, "orders"), testdb.New(t, "payments"), testdb.New(t, "ledger")}
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

const charge = "11111111-1111-1111-1111-111111111111"

// A consistent order: order, charge, payment, two entries.
func good(t *testing.T, d dbs, order string, amount int64, event string) {
	exec(t, d.orders, `INSERT INTO orders (order_id, customer_id, amount_minor) VALUES ($1, 'c', $2)`, order, amount)
	exec(t, d.payments, `INSERT INTO provider_charges (idempotency_key, amount_minor) VALUES ($1, $2)`, order, amount)
	exec(t, d.payments, `INSERT INTO payments (order_id, customer_id, amount_minor, charge_id)
		SELECT $1, 'c', $2, charge_id FROM provider_charges WHERE idempotency_key = $1`, order, amount)
	exec(t, d.ledger, `INSERT INTO applied_events (event_id) VALUES ($1)`, event)
	exec(t, d.ledger, `INSERT INTO entries (event_id, order_id, account, direction, amount_minor)
		VALUES ($1, $2, 'customer:c', 'D', $3), ($1, $2, 'merchant:revenue', 'C', $3)`, event, order, amount)
}

func check(t *testing.T, d dbs) *Report {
	t.Helper()
	r, err := Check(context.Background(), d.orders, d.payments, d.ledger)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestConsistentBooksHaveNoBreaks(t *testing.T) {
	d := setup(t)
	good(t, d, "o1", 500, "00000000-0000-0000-0000-000000000001")
	good(t, d, "o2", 700, "00000000-0000-0000-0000-000000000002")
	r := check(t, d)
	if r.Breaks() != 0 || r.Orders != 2 || r.LedgerEntries != 4 || r.Debits != 1200 || r.Credits != 1200 {
		t.Fatalf("unexpected report:\n%s", r.Markdown("t"))
	}
}

// Each kind of damage must be counted: the reconciliation is only proof if it can fail.
func TestEveryKindOfBreakIsDetected(t *testing.T) {
	d := setup(t)
	good(t, d, "ok", 100, "00000000-0000-0000-0000-000000000001")

	// Lost: an order whose payment never happened.
	exec(t, d.orders, `INSERT INTO orders (order_id, customer_id, amount_minor) VALUES ('lost', 'c', 300)`)
	// Charged but unrecorded: the provider took money, no payment row.
	exec(t, d.payments, `INSERT INTO provider_charges (idempotency_key, amount_minor) VALUES ('ghost', 50)`)
	// Duplicate: an order applied twice under two event IDs.
	good(t, d, "dup", 200, "00000000-0000-0000-0000-000000000002")
	exec(t, d.ledger, `INSERT INTO applied_events (event_id) VALUES ('00000000-0000-0000-0000-000000000003')`)
	exec(t, d.ledger, `INSERT INTO entries (event_id, order_id, account, direction, amount_minor)
		VALUES ('00000000-0000-0000-0000-000000000003', 'dup', 'customer:c', 'D', 200),
		       ('00000000-0000-0000-0000-000000000003', 'dup', 'merchant:revenue', 'C', 200)`)
	// Undelivered: an event left in the outbox.
	exec(t, d.orders, `INSERT INTO outbox (event_type, aggregate_id, payload) VALUES ('OrderCreated', 'lost', '\x00')`)

	r := check(t, d)
	if r.LostPayments != 1 || r.UnrecordedCharges != 1 || r.DuplicateEntries != 1 || r.Undelivered != 1 || !r.Unbalanced {
		t.Fatalf("breaks not counted correctly:\n%s", r.Markdown("t"))
	}
	if r.Breaks() == 0 {
		t.Fatal("a damaged ledger passed")
	}
}

func TestMissingAndWrongEntriesAreDetected(t *testing.T) {
	d := setup(t)
	good(t, d, "a", 100, "00000000-0000-0000-0000-000000000001")
	// A payment with no ledger entries at all.
	exec(t, d.orders, `INSERT INTO orders (order_id, customer_id, amount_minor) VALUES ('b', 'c', 400)`)
	exec(t, d.payments, `INSERT INTO provider_charges (idempotency_key, amount_minor) VALUES ('b', 400)`)
	exec(t, d.payments, `INSERT INTO payments (order_id, customer_id, amount_minor, charge_id)
		SELECT 'b', 'c', 400, charge_id FROM provider_charges WHERE idempotency_key = 'b'`)
	// A payment whose amount disagrees with its order.
	exec(t, d.orders, `INSERT INTO orders (order_id, customer_id, amount_minor) VALUES ('w', 'c', 900)`)
	exec(t, d.payments, `INSERT INTO provider_charges (idempotency_key, amount_minor) VALUES ('w', 800)`)
	exec(t, d.payments, `INSERT INTO payments (order_id, customer_id, amount_minor, charge_id)
		SELECT 'w', 'c', 800, charge_id FROM provider_charges WHERE idempotency_key = 'w'`)
	r := check(t, d)
	if r.MissingEntries < 2 || r.WrongAmounts < 1 {
		t.Fatalf("missing/wrong entries not counted:\n%s", r.Markdown("t"))
	}
}
