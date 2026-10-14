package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/outbox"
	"github.com/tapishikaur/ledgerline/internal/testdb"
)

// The orders schema has an outbox table; any service's would do.
func seed(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("o%05d", i)
			if _, err := outbox.Insert(ctx, tx, "OrderCreated", id, &v1.OrderCreated{OrderId: id, AmountMinor: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func unsent(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	n, err := outbox.Backlog(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Four workers share one outbox. SKIP LOCKED must hand each row to exactly one worker:
// with no failures, every event is delivered exactly once, never concurrently twice.
func TestWorkersSharingAnOutboxDeliverEachEventOnce(t *testing.T) {
	pool := testdb.New(t, "orders")
	seed(t, pool, 2000)
	var deliveries sync.Map // event_id -> *int64
	handler := func(_ context.Context, e outbox.Event) error {
		v, _ := deliveries.LoadOrStore(e.EventID, new(int64))
		atomic.AddInt64(v.(*int64), 1)
		return nil
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := &outbox.Worker{Pool: pool, Handlers: map[string]outbox.Handler{"OrderCreated": handler}, BatchSize: 50}
			for {
				n, err := worker.RunOnce(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()
	total := 0
	deliveries.Range(func(_, v any) bool {
		total++
		if c := atomic.LoadInt64(v.(*int64)); c != 1 {
			t.Errorf("an event was delivered %d times", c)
		}
		return true
	})
	if total != 2000 || unsent(t, pool) != 0 {
		t.Fatalf("delivered %d of 2000 events, %d left unsent", total, unsent(t, pool))
	}
}

func TestFailedDeliveriesStayUnsentAndAreRetried(t *testing.T) {
	pool := testdb.New(t, "orders")
	seed(t, pool, 10)
	failing := true
	w := &outbox.Worker{Pool: pool, Handlers: map[string]outbox.Handler{"OrderCreated": func(_ context.Context, e outbox.Event) error {
		if failing && e.AggregateID < "o00005" {
			return errors.New("receiver down")
		}
		return nil
	}}}
	n, err := w.RunOnce(context.Background())
	if n != 5 || err == nil {
		t.Fatalf("first pass: delivered %d, err %v; want 5 and an error", n, err)
	}
	if left := unsent(t, pool); left != 5 {
		t.Fatalf("%d unsent, want 5", left)
	}
	var attempts int
	var lastErr string
	_ = pool.QueryRow(context.Background(),
		`SELECT attempts, last_error FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 1`).Scan(&attempts, &lastErr)
	if attempts != 1 || lastErr == "" {
		t.Fatalf("attempts %d, last_error %q", attempts, lastErr)
	}
	failing = false
	if n, err := w.RunOnce(context.Background()); n != 5 || err != nil {
		t.Fatalf("retry: delivered %d, err %v", n, err)
	}
	if left := unsent(t, pool); left != 0 {
		t.Fatalf("%d unsent after the retry", left)
	}
}

// A worker that dies after delivering some events but before committing must leave them all
// unsent: they're delivered again (at-least-once), which is why receivers are idempotent.
func TestAWorkerDyingMidBatchLeavesTheBatchForRedelivery(t *testing.T) {
	pool := testdb.New(t, "orders")
	seed(t, pool, 20)
	ctx, crash := context.WithCancel(context.Background())
	var delivered atomic.Int64
	w := &outbox.Worker{Pool: pool, Parallel: 1, Handlers: map[string]outbox.Handler{"OrderCreated": func(context.Context, outbox.Event) error {
		if delivered.Add(1) == 10 {
			crash() // the process dies here: no commit
		}
		return nil
	}}}
	if _, err := w.RunOnce(ctx); err == nil {
		t.Fatal("expected the crashed batch to fail")
	}
	if left := unsent(t, pool); left != 20 {
		t.Fatalf("%d unsent after the crash, want all 20", left)
	}
	again := &outbox.Worker{Pool: pool, Handlers: map[string]outbox.Handler{"OrderCreated": func(context.Context, outbox.Event) error { return nil }}}
	if n, err := again.RunOnce(context.Background()); n != 20 || err != nil {
		t.Fatalf("redelivery: %d, %v", n, err)
	}
}

func TestRunDrainsTheOutboxAndStopsOnCancel(t *testing.T) {
	pool := testdb.New(t, "orders")
	seed(t, pool, 300)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := &outbox.Worker{Pool: pool, BatchSize: 25, Handlers: map[string]outbox.Handler{"OrderCreated": func(context.Context, outbox.Event) error { return nil }}}
	go func() { done <- w.Run(ctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for unsent(t, pool) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if left := unsent(t, pool); left != 0 {
		t.Fatalf("%d unsent", left)
	}
}

// The trace context stored with the event must be continued by the worker: the delivery span
// belongs to the same trace as the span that wrote the event.
func TestTheWorkerContinuesTheTraceStoredWithTheEvent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	pool := testdb.New(t, "orders")
	ctx, span := provider.Tracer("test").Start(context.Background(), "CreateOrder")
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := outbox.Insert(ctx, tx, "OrderCreated", "o1", &v1.OrderCreated{OrderId: "o1"})
		return err
	})
	span.End()
	if err != nil {
		t.Fatal(err)
	}
	w := &outbox.Worker{Pool: pool, Handlers: map[string]outbox.Handler{"OrderCreated": func(context.Context, outbox.Event) error { return nil }}}
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var deliverySpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.Name() == "outbox.deliver OrderCreated" {
			deliverySpan = s
		}
	}
	if deliverySpan == nil {
		t.Fatal("no delivery span recorded")
	}
	if deliverySpan.SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Fatal("the worker started a new trace instead of continuing the order's")
	}
	if deliverySpan.Parent().SpanID() != span.SpanContext().SpanID() {
		t.Fatal("the delivery span isn't a child of the span that wrote the event")
	}
}
