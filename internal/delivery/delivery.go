// Package delivery turns outbox events into gRPC calls to the service that consumes them.
package delivery

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/ledger"
	"github.com/tapishikaur/ledgerline/internal/orders"
	"github.com/tapishikaur/ledgerline/internal/outbox"
	"github.com/tapishikaur/ledgerline/internal/payments"
)

// ToPayments delivers OrderCreated events (the orders outbox) to the payments service.
func ToPayments(client v1.PaymentsClient) map[string]outbox.Handler {
	return map[string]outbox.Handler{
		orders.EventOrderCreated: func(ctx context.Context, e outbox.Event) error {
			data := &v1.OrderCreated{}
			if err := proto.Unmarshal(e.Payload, data); err != nil {
				return fmt.Errorf("decoding %s: %w", e.Type, err)
			}
			_, err := client.HandleOrderCreated(ctx, &v1.OrderCreatedEvent{EventId: e.EventID, Data: data})
			return err
		},
	}
}

// ToLedger delivers PaymentCaptured events (the payments outbox) to the ledger service.
func ToLedger(client v1.LedgerClient) map[string]outbox.Handler {
	return map[string]outbox.Handler{
		payments.EventPaymentCaptured: func(ctx context.Context, e outbox.Event) error {
			data := &v1.PaymentCaptured{}
			if err := proto.Unmarshal(e.Payload, data); err != nil {
				return fmt.Errorf("decoding %s: %w", e.Type, err)
			}
			_, err := client.ApplyPaymentCaptured(ctx, &v1.PaymentCapturedEvent{EventId: e.EventID, Data: data})
			return err
		},
	}
}

var _ = ledger.RevenueAccount // the ledger package is the consumer of ToLedger's events
