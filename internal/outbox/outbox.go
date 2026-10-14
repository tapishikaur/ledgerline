// Package outbox implements the transactional outbox: events are written in the same
// database transaction as the business change, and a worker delivers them afterwards.
//
// Delivery is at-least-once: if the worker crashes after delivering an event but before
// marking it sent, the transaction holding its row lock rolls back and the event is delivered
// again later. Receivers are idempotent on event_id, so the effect is exactly once.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Event is one outbox row.
type Event struct {
	ID          int64  // the row's position in the outbox
	EventID     string // unique; receivers deduplicate on it
	Type        string
	AggregateID string
	Payload     []byte // the event message, protobuf-encoded
	TraceParent string // W3C trace context of the transaction that wrote it ("" if none)
}

// Insert adds an event to the outbox inside tx, the caller's transaction, so the event is
// committed if and only if the caller's own changes are. The current trace context is stored
// with it, so the worker's delivery joins the same trace. Returns the new event_id.
func Insert(ctx context.Context, tx pgx.Tx, eventType, aggregateID string, msg proto.Message) (string, error) {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("encoding %s: %w", eventType, err)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	var traceParent *string
	if tp := carrier.Get("traceparent"); tp != "" {
		traceParent = &tp
	}
	var eventID string
	err = tx.QueryRow(ctx,
		`INSERT INTO outbox (event_type, aggregate_id, payload, traceparent) VALUES ($1, $2, $3, $4)
		 RETURNING event_id::text`,
		eventType, aggregateID, payload, traceParent).Scan(&eventID)
	return eventID, err
}

// Handler delivers one event. Returning nil means the receiver has applied it (now or
// before); any error means "try again later".
type Handler func(ctx context.Context, e Event) error

// Worker delivers unsent outbox events.
type Worker struct {
	Pool       *pgxpool.Pool
	Handlers   map[string]Handler // by event type
	BatchSize  int                // rows claimed per transaction (default 100)
	Parallel   int                // deliveries in flight per batch (default 16)
	Timeout    time.Duration      // per delivery (default 5 s)
	IdleWait   time.Duration      // pause when the outbox is empty (default 50 ms)
	Log        *slog.Logger
	tracerOnce sync.Once
	tracer     trace.Tracer
}

// Run delivers events until ctx is cancelled: as fast as possible while there is a backlog,
// pausing IdleWait when the outbox is empty and backing off after errors.
func (w *Worker) Run(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	for {
		n, err := w.RunOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var wait time.Duration
		switch {
		case err != nil:
			w.log().Warn("outbox batch failed; will retry", "err", err, "delivered", n, "backoff", backoff)
			wait = backoff
			backoff = min(backoff*2, 2*time.Second)
		case n == 0:
			wait = w.idleWait()
			backoff = 100 * time.Millisecond
		default:
			backoff = 100 * time.Millisecond
			continue // a backlog: take the next batch at once
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

// RunOnce claims up to BatchSize unsent events, delivers them, and marks the delivered ones
// sent, all in one transaction. Returns how many were delivered and the first delivery error.
//
// FOR UPDATE locks the claimed rows until the transaction ends, so no other worker delivers
// them at the same time; SKIP LOCKED makes other workers skip those rows instead of waiting,
// so several workers can share one outbox. If this worker dies mid-batch, its transaction
// rolls back, the locks are released and the rows are delivered again by whoever comes next.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // a no-op after Commit

	rows, err := tx.Query(ctx,
		`SELECT id, event_id::text, event_type, aggregate_id, payload, COALESCE(traceparent, '')
		   FROM outbox
		  WHERE sent_at IS NULL
		  ORDER BY id
		  LIMIT $1
		    FOR UPDATE SKIP LOCKED`, w.batchSize())
	if err != nil {
		return 0, err
	}
	events, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Event, error) {
		var e Event
		err := r.Scan(&e.ID, &e.EventID, &e.Type, &e.AggregateID, &e.Payload, &e.TraceParent)
		return e, err
	})
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, tx.Commit(ctx)
	}

	// Deliver in parallel. Within one outbox there is one event per aggregate on this
	// flow (one OrderCreated per order, one PaymentCaptured per payment), so delivery order
	// inside a batch doesn't matter.
	errs := make([]error, len(events))
	sem := make(chan struct{}, w.parallel())
	var wg sync.WaitGroup
	for i := range events {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[i] = w.deliver(ctx, events[i])
		}(i)
	}
	wg.Wait()

	var sent []int64
	var firstErr error
	for i, e := range events {
		if errs[i] == nil {
			sent = append(sent, e.ID)
			continue
		}
		if firstErr == nil {
			firstErr = errs[i]
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
			e.ID, truncate(errs[i].Error(), 500)); err != nil {
			return 0, err
		}
	}
	if len(sent) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE outbox SET sent_at = now(), attempts = attempts + 1 WHERE id = ANY($1)`, sent); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(sent), firstErr
}

// deliver runs one event's handler inside a span that continues the event's stored trace.
func (w *Worker) deliver(ctx context.Context, e Event) error {
	if e.TraceParent != "" {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": e.TraceParent})
	}
	ctx, span := w.tr().Start(ctx, "outbox.deliver "+e.Type, trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("outbox.event_id", e.EventID),
			attribute.String("outbox.event_type", e.Type),
			attribute.String("outbox.aggregate_id", e.AggregateID),
			attribute.Int64("outbox.row_id", e.ID),
		))
	defer span.End()

	handler, ok := w.Handlers[e.Type]
	if !ok {
		err := fmt.Errorf("no handler for event type %q", e.Type)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, w.timeout())
	defer cancel()
	if err := handler(ctx, e); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("event %s (%s): %w", e.EventID, e.Type, err)
	}
	return nil
}

// Backlog counts unsent events (for drain checks and monitoring).
func Backlog(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n)
	return n, err
}

func (w *Worker) batchSize() int {
	if w.BatchSize > 0 {
		return w.BatchSize
	}
	return 100
}

func (w *Worker) parallel() int {
	if w.Parallel > 0 {
		return w.Parallel
	}
	return 16
}

func (w *Worker) timeout() time.Duration {
	if w.Timeout > 0 {
		return w.Timeout
	}
	return 5 * time.Second
}

func (w *Worker) idleWait() time.Duration {
	if w.IdleWait > 0 {
		return w.IdleWait
	}
	return 50 * time.Millisecond
}

func (w *Worker) log() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.Default()
}

func (w *Worker) tr() trace.Tracer {
	w.tracerOnce.Do(func() { w.tracer = otel.Tracer("github.com/tapishikaur/ledgerline/internal/outbox") })
	return w.tracer
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
