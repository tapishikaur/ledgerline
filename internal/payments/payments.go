// Package payments is the payments service: it handles OrderCreated events, charges the
// customer exactly once, and writes the payment and a PaymentCaptured event in one transaction.
package payments

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/outbox"
)

// EventPaymentCaptured is the outbox event type this service writes.
const EventPaymentCaptured = "PaymentCaptured"

type Server struct {
	v1.UnimplementedPaymentsServer
	Pool     *pgxpool.Pool
	Provider *Provider
}

// HandleOrderCreated applies an OrderCreated event exactly once, however many times it is
// delivered:
//
//  1. If the event_id is already in processed_events, it was handled before: acknowledge and
//     stop (no provider call at all).
//  2. Charge through the provider with the order_id as idempotency key, outside any database
//     transaction. However often this runs for one order (redeliveries after crashes,
//     concurrent duplicates), the provider charges once and returns that same charge.
//  3. In one short transaction: INSERT the event_id into processed_events ON CONFLICT DO
//     NOTHING (if nothing was inserted, a concurrent delivery got here first: stop), then the
//     payment (order_id is UNIQUE) and the PaymentCaptured outbox event. COMMIT.
//
// Why the provider call is outside the transaction: holding a transaction (and its pooled
// connection and row locks) open across a network call to an external system is a classic
// way to exhaust a connection pool. Under crash-and-redeliver load, transactions waiting on
// each other's locks held every connection while the call they waited for needed one more.
func (s *Server) HandleOrderCreated(ctx context.Context, ev *v1.OrderCreatedEvent) (*v1.Ack, error) {
	if ev.EventId == "" || ev.Data == nil || ev.Data.OrderId == "" || ev.Data.AmountMinor <= 0 {
		return nil, status.Error(codes.InvalidArgument, "event_id, order_id and a positive amount are required")
	}
	ctx, cancel := workContext(ctx)
	defer cancel()

	var seen bool
	if err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM processed_events WHERE event_id = $1)`, ev.EventId).
		Scan(&seen); err != nil {
		return nil, status.Errorf(codes.Unavailable, "handling OrderCreated %s: %v", ev.EventId, err)
	}
	if seen {
		return &v1.Ack{Applied: false}, nil // duplicate delivery
	}
	chargeID, err := s.Provider.Charge(ctx, ev.Data.OrderId, ev.Data.AmountMinor)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "charging for order %s: %v", ev.Data.OrderId, err)
	}

	applied := false
	err = pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, ev.EventId)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // a concurrent delivery of the same event committed first
		}
		var paymentID string
		err = tx.QueryRow(ctx,
			`INSERT INTO payments (order_id, customer_id, amount_minor, charge_id) VALUES ($1, $2, $3, $4)
			 RETURNING payment_id::text`,
			ev.Data.OrderId, ev.Data.CustomerId, ev.Data.AmountMinor, chargeID).Scan(&paymentID)
		if err != nil {
			return err
		}
		_, err = outbox.Insert(ctx, tx, EventPaymentCaptured, ev.Data.OrderId, &v1.PaymentCaptured{
			PaymentId: paymentID, OrderId: ev.Data.OrderId, CustomerId: ev.Data.CustomerId,
			AmountMinor: ev.Data.AmountMinor, ChargeId: chargeID,
		})
		applied = err == nil
		return err
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "handling OrderCreated %s: %v", ev.EventId, err)
	}
	return &v1.Ack{Applied: applied}, nil
}

// workContext detaches the handler's database work from the caller's cancellation, with its
// own time limit. If the outbox worker that sent the event dies mid-call, the transaction is
// finished (or rolled back cleanly) instead of being cut off in the middle, which would also
// throw away a database connection. Safe either way: the worker never saw the reply, so it
// redelivers, and the duplicate is ignored. Trace context is kept.
func workContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// Provider simulates an external card processor. Like real ones (an "Idempotency-Key" header),
// it charges once per key: asking again with the same key returns the original charge.
//
// It records charges in its own table and its own transaction, separate from the payments
// transaction, because an external system's state doesn't roll back when ours does.
type Provider struct {
	Pool *pgxpool.Pool
	// Simulated behaviour, for tests and chaos runs.
	Latency       time.Duration // added to every call
	FailRate      float64       // chance a call fails before charging
	LostReplyRate float64       // chance a call charges but the reply is "lost" (returns an error)

	mu  sync.Mutex
	rng *rand.Rand
}

var errProvider = errors.New("payment provider unavailable (simulated)")

func (p *Provider) Charge(ctx context.Context, idempotencyKey string, amountMinor int64) (string, error) {
	ctx, span := otel.Tracer("github.com/tapishikaur/ledgerline/internal/payments").Start(ctx, "provider.Charge")
	defer span.End()
	span.SetAttributes(attribute.String("payment.idempotency_key", idempotencyKey))

	if p.Latency > 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(p.Latency):
		}
	}
	if p.roll(p.FailRate) {
		return "", errProvider
	}
	var chargeID string
	var amount int64
	err := p.Pool.QueryRow(ctx,
		`INSERT INTO provider_charges (idempotency_key, amount_minor) VALUES ($1, $2)
		 ON CONFLICT (idempotency_key) DO UPDATE SET calls = provider_charges.calls + 1
		 RETURNING charge_id::text, amount_minor`,
		idempotencyKey, amountMinor).Scan(&chargeID, &amount)
	if err != nil {
		return "", fmt.Errorf("provider: %w", err)
	}
	if amount != amountMinor {
		return "", fmt.Errorf("provider: key %s was used for amount %d, not %d", idempotencyKey, amount, amountMinor)
	}
	if p.roll(p.LostReplyRate) {
		return "", errProvider // charged, but the caller never hears back
	}
	return chargeID, nil
}

func (p *Provider) roll(rate float64) bool {
	if rate <= 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rng == nil {
		p.rng = rand.New(rand.NewPCG(1, 2))
	}
	return p.rng.Float64() < rate
}
