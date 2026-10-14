// Package ledger is the ledger service: double-entry bookkeeping, applied idempotently.
package ledger

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
)

// Accounts. Each payment debits the customer's account and credits merchant revenue.
const RevenueAccount = "merchant:revenue"

func CustomerAccount(customerID string) string { return "customer:" + customerID }

type Server struct {
	v1.UnimplementedLedgerServer
	Pool *pgxpool.Pool
}

// ApplyPaymentCaptured writes one debit and one credit for a payment, exactly once per event:
// the event_id goes into applied_events with ON CONFLICT DO NOTHING first, and if nothing was
// inserted the event was applied before. Both entries and the marker commit together.
func (s *Server) ApplyPaymentCaptured(ctx context.Context, ev *v1.PaymentCapturedEvent) (*v1.Ack, error) {
	if ev.EventId == "" || ev.Data == nil || ev.Data.OrderId == "" || ev.Data.AmountMinor <= 0 {
		return nil, status.Error(codes.InvalidArgument, "event_id, order_id and a positive amount are required")
	}
	// Finish the transaction even if the sender disappears mid-call (see payments.workContext).
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	applied := false
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO applied_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, ev.EventId)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // duplicate delivery
		}
		_, err = tx.Exec(ctx,
			`INSERT INTO entries (event_id, order_id, account, direction, amount_minor)
			 VALUES ($1, $2, $3, 'D', $5), ($1, $2, $4, 'C', $5)`,
			ev.EventId, ev.Data.OrderId, CustomerAccount(ev.Data.CustomerId), RevenueAccount, ev.Data.AmountMinor)
		applied = err == nil
		return err
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "applying PaymentCaptured %s: %v", ev.EventId, err)
	}
	return &v1.Ack{Applied: applied}, nil
}
