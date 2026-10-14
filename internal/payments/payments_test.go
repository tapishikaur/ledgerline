package payments

import (
	"context"
	"sync"
	"testing"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/testdb"
)

func newServer(t *testing.T) *Server {
	pool := testdb.New(t, "payments")
	return &Server{Pool: pool, Provider: &Provider{Pool: pool}}
}

func event(id, order string) *v1.OrderCreatedEvent {
	return &v1.OrderCreatedEvent{
		EventId: id,
		Data:    &v1.OrderCreated{OrderId: order, CustomerId: "c1", AmountMinor: 4200},
	}
}

func counts(t *testing.T, s *Server) (payments, charges, calls, events int) {
	t.Helper()
	ctx := context.Background()
	q := func(sql string) int {
		var n int
		if err := s.Pool.QueryRow(ctx, sql).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return q(`SELECT count(*) FROM payments`), q(`SELECT count(*) FROM provider_charges`),
		q(`SELECT COALESCE(sum(calls), 0) FROM provider_charges`), q(`SELECT count(*) FROM outbox`)
}

const ev1 = "00000000-0000-0000-0000-000000000001"

func TestADuplicateDeliveryChargesOnce(t *testing.T) {
	s := newServer(t)
	for i := 0; i < 3; i++ {
		ack, err := s.HandleOrderCreated(context.Background(), event(ev1, "o1"))
		if err != nil {
			t.Fatal(err)
		}
		if ack.Applied != (i == 0) {
			t.Fatalf("delivery %d: applied = %v", i, ack.Applied)
		}
	}
	p, c, calls, e := counts(t, s)
	if p != 1 || c != 1 || calls != 1 || e != 1 {
		t.Fatalf("payments %d, charges %d, provider calls %d, events %d; want 1 each", p, c, calls, e)
	}
}

// The provider charges the card but the reply is lost, so the payments transaction rolls back.
// The redelivery must get the same charge back: one charge, two provider calls, one payment.
func TestALostProviderReplyDoesNotChargeTwice(t *testing.T) {
	s := newServer(t)
	s.Provider.LostReplyRate = 1
	if _, err := s.HandleOrderCreated(context.Background(), event(ev1, "o1")); err == nil {
		t.Fatal("expected the lost reply to fail the first delivery")
	}
	p, c, _, e := counts(t, s)
	if p != 0 || c != 1 || e != 0 {
		t.Fatalf("after the lost reply: payments %d, charges %d, events %d; want 0, 1, 0", p, c, e)
	}

	s.Provider.LostReplyRate = 0
	ack, err := s.HandleOrderCreated(context.Background(), event(ev1, "o1"))
	if err != nil || !ack.Applied {
		t.Fatalf("redelivery: %v %v", ack, err)
	}
	p, c, calls, e := counts(t, s)
	if p != 1 || c != 1 || calls != 2 || e != 1 {
		t.Fatalf("payments %d, charges %d, provider calls %d, events %d; want 1, 1, 2, 1", p, c, calls, e)
	}
	var paymentCharge, providerCharge string
	_ = s.Pool.QueryRow(context.Background(), `SELECT charge_id::text FROM payments`).Scan(&paymentCharge)
	_ = s.Pool.QueryRow(context.Background(), `SELECT charge_id::text FROM provider_charges`).Scan(&providerCharge)
	if paymentCharge != providerCharge {
		t.Fatalf("payment records charge %s, provider has %s", paymentCharge, providerCharge)
	}
}

func TestAProviderFailureBeforeChargingChargesNothing(t *testing.T) {
	s := newServer(t)
	s.Provider.FailRate = 1
	if _, err := s.HandleOrderCreated(context.Background(), event(ev1, "o1")); err == nil {
		t.Fatal("expected an error")
	}
	if p, c, _, e := counts(t, s); p+c+e != 0 {
		t.Fatalf("payments %d, charges %d, events %d after a failure", p, c, e)
	}
}

// Many deliveries of the same event at the same moment: exactly one may apply, and the card is
// charged once. (They may each *ask* the provider: it answers every ask for the same order with
// the same single charge.)
func TestConcurrentDuplicateDeliveriesApplyOnce(t *testing.T) {
	s := newServer(t)
	var wg sync.WaitGroup
	var mu sync.Mutex
	applied := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ack, err := s.HandleOrderCreated(context.Background(), event(ev1, "o1"))
			if err != nil {
				t.Error(err)
				return
			}
			if ack.Applied {
				mu.Lock()
				applied++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	p, c, calls, e := counts(t, s)
	if applied != 1 || p != 1 || c != 1 || e != 1 || calls < 1 {
		t.Fatalf("applied %d, payments %d, charges %d, events %d (provider asked %d times); want 1, 1, 1, 1",
			applied, p, c, e, calls)
	}
}

func TestInvalidEventsAreRejected(t *testing.T) {
	s := newServer(t)
	for _, ev := range []*v1.OrderCreatedEvent{
		{EventId: "", Data: &v1.OrderCreated{OrderId: "o", AmountMinor: 1}},
		{EventId: ev1, Data: nil},
		{EventId: ev1, Data: &v1.OrderCreated{OrderId: "o", AmountMinor: 0}},
	} {
		if _, err := s.HandleOrderCreated(context.Background(), ev); err == nil {
			t.Errorf("%v accepted", ev)
		}
	}
}
