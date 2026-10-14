// Package reconcile checks the three services' databases against each other and counts every
// way money could have gone wrong. A clean run is all zeros.
package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Report counts what was compared and every break found.
type Report struct {
	Orders, Payments, ProviderCharges, LedgerEntries int64
	OrdersTotal, Debits, Credits                     int64 // minor units

	// Breaks: each is a count of orders affected.
	LostPayments      int64 // order with no payment
	PhantomPayments   int64 // payment with no order
	WrongAmounts      int64 // payment or entry amount differs from the order
	UnrecordedCharges int64 // provider charged, but no payment row: money taken and not recorded
	ChargeMismatches  int64 // payment's charge_id isn't the provider's charge for that order
	MissingEntries    int64 // a payment without exactly one debit and one credit (fewer)
	DuplicateEntries  int64 // more than one debit or credit for an order
	OrphanEntries     int64 // ledger entries for an order with no payment
	Undelivered       int64 // outbox events never delivered (orders + payments outboxes)
	Unbalanced        bool  // total debits != total credits, or != the orders' total

	Examples []string // up to 20 sample breaks, for the report
}

// Breaks is the total number of problems found.
func (r *Report) Breaks() int64 {
	n := r.LostPayments + r.PhantomPayments + r.WrongAmounts + r.UnrecordedCharges + r.ChargeMismatches +
		r.MissingEntries + r.DuplicateEntries + r.OrphanEntries + r.Undelivered
	if r.Unbalanced {
		n++
	}
	return n
}

func (r *Report) example(format string, args ...any) {
	if len(r.Examples) < 20 {
		r.Examples = append(r.Examples, fmt.Sprintf(format, args...))
	}
}

type payment struct {
	amount   int64
	chargeID string
}

type ledgerSide struct {
	count  int64
	amount int64
}

// Check reads all three databases and compares them. It holds every order in memory, which is
// fine for the 250k orders of the chaos test (a few tens of MB).
func Check(ctx context.Context, ordersDB, paymentsDB, ledgerDB *pgxpool.Pool) (*Report, error) {
	r := &Report{}

	orders := map[string]int64{}
	if err := scanAll(ctx, ordersDB, `SELECT order_id, amount_minor FROM orders`, func(id string, amount int64) {
		orders[id] = amount
		r.OrdersTotal += amount
	}); err != nil {
		return nil, fmt.Errorf("orders: %w", err)
	}
	r.Orders = int64(len(orders))

	payments := map[string]payment{}
	rows, err := paymentsDB.Query(ctx, `SELECT order_id, amount_minor, charge_id::text FROM payments`)
	if err != nil {
		return nil, fmt.Errorf("payments: %w", err)
	}
	for rows.Next() {
		var id string
		var p payment
		if err := rows.Scan(&id, &p.amount, &p.chargeID); err != nil {
			return nil, err
		}
		payments[id] = p
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.Payments = int64(len(payments))

	charges := map[string]payment{}
	rows, err = paymentsDB.Query(ctx, `SELECT idempotency_key, amount_minor, charge_id::text FROM provider_charges`)
	if err != nil {
		return nil, fmt.Errorf("provider charges: %w", err)
	}
	for rows.Next() {
		var key string
		var c payment
		if err := rows.Scan(&key, &c.amount, &c.chargeID); err != nil {
			return nil, err
		}
		charges[key] = c
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	r.ProviderCharges = int64(len(charges))

	type sides struct{ debit, credit ledgerSide }
	ledger := map[string]*sides{}
	rows, err = ledgerDB.Query(ctx,
		`SELECT order_id, direction, count(*), sum(amount_minor)::bigint FROM entries GROUP BY order_id, direction`)
	if err != nil {
		return nil, fmt.Errorf("ledger: %w", err)
	}
	for rows.Next() {
		var id, dir string
		var s ledgerSide
		if err := rows.Scan(&id, &dir, &s.count, &s.amount); err != nil {
			return nil, err
		}
		e := ledger[id]
		if e == nil {
			e = &sides{}
			ledger[id] = e
		}
		if strings.TrimSpace(dir) == "D" {
			e.debit = s
			r.Debits += s.amount
		} else {
			e.credit = s
			r.Credits += s.amount
		}
		r.LedgerEntries += s.count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, id := range sortedKeys(orders) {
		amount := orders[id]
		p, paid := payments[id]
		if !paid {
			r.LostPayments++
			r.example("order %s has no payment", id)
			continue
		}
		if p.amount != amount {
			r.WrongAmounts++
			r.example("order %s: payment %d but order %d", id, p.amount, amount)
		}
		if c, ok := charges[id]; !ok || c.chargeID != p.chargeID {
			r.ChargeMismatches++
			r.example("order %s: payment's charge %s isn't the provider's", id, p.chargeID)
		}
		e := ledger[id]
		switch {
		case e == nil || e.debit.count == 0 || e.credit.count == 0:
			r.MissingEntries++
			r.example("order %s: ledger entries missing", id)
		case e.debit.count > 1 || e.credit.count > 1:
			r.DuplicateEntries++
			r.example("order %s: %d debits and %d credits", id, e.debit.count, e.credit.count)
		case e.debit.amount != amount || e.credit.amount != amount:
			r.WrongAmounts++
			r.example("order %s: entries %d/%d but order %d", id, e.debit.amount, e.credit.amount, amount)
		}
	}
	for id := range payments {
		if _, ok := orders[id]; !ok {
			r.PhantomPayments++
			r.example("payment for unknown order %s", id)
		}
	}
	for key := range charges {
		if _, ok := payments[key]; !ok {
			r.UnrecordedCharges++
			r.example("provider charged order %s but no payment was recorded", key)
		}
	}
	for id := range ledger {
		if _, ok := payments[id]; !ok {
			r.OrphanEntries++
			r.example("ledger entries for order %s without a payment", id)
		}
	}
	for name, pool := range map[string]*pgxpool.Pool{"orders": ordersDB, "payments": paymentsDB} {
		var n int64
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
			return nil, fmt.Errorf("%s outbox: %w", name, err)
		}
		if n > 0 {
			r.Undelivered += n
			r.example("%d events still undelivered in the %s outbox", n, name)
		}
	}
	if r.Debits != r.Credits || r.Debits != r.OrdersTotal {
		r.Unbalanced = true
		r.example("debits %d, credits %d, orders total %d", r.Debits, r.Credits, r.OrdersTotal)
	}
	return r, nil
}

func scanAll(ctx context.Context, pool *pgxpool.Pool, sql string, each func(string, int64)) error {
	rows, err := pool.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return err
		}
		each(id, n)
	}
	return rows.Err()
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Markdown renders the report as a table.
func (r *Report) Markdown(title string) string {
	var b strings.Builder
	result := "PASS, 0 breaks"
	if n := r.Breaks(); n > 0 {
		result = fmt.Sprintf("FAIL, %d breaks", n)
	}
	fmt.Fprintf(&b, "# %s\n\n**Result: %s**\n\n", title, result)
	fmt.Fprintf(&b, "| Compared | Count |\n|---|---|\n")
	fmt.Fprintf(&b, "| Orders | %d |\n| Payments | %d |\n| Provider charges | %d |\n| Ledger entries | %d |\n",
		r.Orders, r.Payments, r.ProviderCharges, r.LedgerEntries)
	fmt.Fprintf(&b, "| Orders total / debits / credits (minor units) | %d / %d / %d |\n\n", r.OrdersTotal, r.Debits, r.Credits)
	fmt.Fprintf(&b, "| Check | Breaks |\n|---|---|\n")
	for _, row := range []struct {
		name string
		n    int64
	}{
		{"Order without a payment (lost)", r.LostPayments},
		{"Payment without an order", r.PhantomPayments},
		{"Charged by the provider but no payment recorded", r.UnrecordedCharges},
		{"Payment's charge isn't the provider's charge", r.ChargeMismatches},
		{"Amount differs from the order", r.WrongAmounts},
		{"Missing ledger entry", r.MissingEntries},
		{"Duplicate ledger entry", r.DuplicateEntries},
		{"Ledger entry without a payment", r.OrphanEntries},
		{"Outbox events never delivered", r.Undelivered},
	} {
		fmt.Fprintf(&b, "| %s | %d |\n", row.name, row.n)
	}
	balanced := "yes"
	if r.Unbalanced {
		balanced = "**no**"
	}
	fmt.Fprintf(&b, "| Debits = credits = orders total | %s |\n", balanced)
	if len(r.Examples) > 0 {
		b.WriteString("\nExamples:\n\n")
		for _, e := range r.Examples {
			fmt.Fprintf(&b, "- %s\n", e)
		}
	}
	return b.String()
}
