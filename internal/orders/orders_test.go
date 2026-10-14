package orders

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/outbox"
	"github.com/tapishikaur/ledgerline/internal/testdb"
)

func count(t *testing.T, s *Server, sql string) int {
	t.Helper()
	var n int
	if err := s.Pool.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCreateOrderWritesTheOrderAndItsEventTogether(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "orders")}
	resp, err := s.CreateOrder(context.Background(), &v1.CreateOrderRequest{OrderId: "o1", CustomerId: "c1", AmountMinor: 1250})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Created || resp.Order.AmountMinor != 1250 {
		t.Fatalf("unexpected response %v", resp)
	}
	var eventType, aggregate string
	var payload []byte
	if err := s.Pool.QueryRow(context.Background(),
		`SELECT event_type, aggregate_id, payload FROM outbox`).Scan(&eventType, &aggregate, &payload); err != nil {
		t.Fatal(err)
	}
	ev := &v1.OrderCreated{}
	if err := proto.Unmarshal(payload, ev); err != nil {
		t.Fatal(err)
	}
	if eventType != EventOrderCreated || aggregate != "o1" || ev.AmountMinor != 1250 || ev.CustomerId != "c1" {
		t.Fatalf("bad event %s %s %v", eventType, aggregate, ev)
	}
}

func TestRetryingCreateOrderCreatesNothingNew(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "orders")}
	req := &v1.CreateOrderRequest{OrderId: "o1", CustomerId: "c1", AmountMinor: 500}
	for i := 0; i < 3; i++ {
		resp, err := s.CreateOrder(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Created != (i == 0) {
			t.Fatalf("call %d: created = %v", i, resp.Created)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM orders`); n != 1 {
		t.Fatalf("%d orders", n)
	}
	if n := count(t, s, `SELECT count(*) FROM outbox`); n != 1 {
		t.Fatalf("%d events: a retry must not emit a second OrderCreated", n)
	}
}

func TestReusingAnOrderIDForADifferentOrderIsRefused(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "orders")}
	ctx := context.Background()
	if _, err := s.CreateOrder(ctx, &v1.CreateOrderRequest{OrderId: "o1", CustomerId: "c1", AmountMinor: 500}); err != nil {
		t.Fatal(err)
	}
	_, err := s.CreateOrder(ctx, &v1.CreateOrderRequest{OrderId: "o1", CustomerId: "c1", AmountMinor: 999})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestInvalidRequestsAreRejected(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "orders")}
	for _, req := range []*v1.CreateOrderRequest{
		{OrderId: "", CustomerId: "c", AmountMinor: 1},
		{OrderId: "o", CustomerId: "", AmountMinor: 1},
		{OrderId: "o", CustomerId: "c", AmountMinor: 0},
		{OrderId: "o", CustomerId: "c", AmountMinor: -5},
	} {
		if _, err := s.CreateOrder(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v: got %v", req, err)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM orders`); n != 0 {
		t.Fatalf("%d orders written by invalid requests", n)
	}
}

// The outbox guarantee itself: if the transaction fails after both writes, neither survives.
func TestAFailedTransactionLeavesNeitherOrderNorEvent(t *testing.T) {
	s := &Server{Pool: testdb.New(t, "orders")}
	ctx := context.Background()
	boom := errors.New("crash before commit")
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO orders (order_id, customer_id, amount_minor) VALUES ('o1', 'c1', 10)`); err != nil {
			return err
		}
		if _, err := outbox.Insert(ctx, tx, EventOrderCreated, "o1", &v1.OrderCreated{OrderId: "o1"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if n := count(t, s, `SELECT count(*) FROM orders`) + count(t, s, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("%d rows survived a rolled-back transaction", n)
	}
}
