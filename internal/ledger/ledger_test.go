package ledger

import (
	"context"
	"sync"
	"testing"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/testdb"
)

const ev1 = "00000000-0000-0000-0000-0000000000a1"

func captured(id string) *v1.PaymentCapturedEvent {
	return &v1.PaymentCapturedEvent{EventId: id, Data: &v1.PaymentCaptured{
		PaymentId: "p1", OrderId: "o1", CustomerId: "c9", AmountMinor: 777, ChargeId: "ch1",
	}}
}

type entry struct {
	account, direction string
	amount             int64
}

func entries(t *testing.T, s *Server) []entry {
	t.Helper()
	rows, err := s.Pool.Query(context.Background(), `SELECT account, direction, amount_minor FROM entries ORDER BY direction DESC`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.account, &e.direction, &e.amount); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func TestAPaymentBecomesOneDebitAndOneCredit(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "ledger")}
	ack, err := s.ApplyPaymentCaptured(context.Background(), captured(ev1))
	if err != nil || !ack.Applied {
		t.Fatalf("%v %v", ack, err)
	}
	got := entries(t, s)
	want := []entry{{"customer:c9", "D", 777}, {RevenueAccount, "C", 777}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("entries %v, want %v", got, want)
	}
}

func TestADuplicateDeliveryWritesNothingMore(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "ledger")}
	for i := 0; i < 3; i++ {
		ack, err := s.ApplyPaymentCaptured(context.Background(), captured(ev1))
		if err != nil {
			t.Fatal(err)
		}
		if ack.Applied != (i == 0) {
			t.Fatalf("delivery %d: applied = %v", i, ack.Applied)
		}
	}
	if n := len(entries(t, s)); n != 2 {
		t.Fatalf("%d entries, want 2", n)
	}
}

func TestConcurrentDuplicateDeliveriesApplyOnce(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "ledger")}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.ApplyPaymentCaptured(context.Background(), captured(ev1)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := len(entries(t, s)); n != 2 {
		t.Fatalf("%d entries, want 2", n)
	}
}

func TestInvalidEventsAreRejected(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "ledger")}
	bad := captured(ev1)
	bad.Data.AmountMinor = 0
	if _, err := s.ApplyPaymentCaptured(context.Background(), bad); err == nil {
		t.Fatal("zero amount accepted")
	}
	if _, err := s.ApplyPaymentCaptured(context.Background(), &v1.PaymentCapturedEvent{EventId: ev1}); err == nil {
		t.Fatal("missing data accepted")
	}
}
