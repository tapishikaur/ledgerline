// Package orders is the orders service: it records orders and, in the same transaction,
// an OrderCreated event in its outbox.
package orders

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/outbox"
)

// EventOrderCreated is the outbox event type this service writes.
const EventOrderCreated = "OrderCreated"

type Server struct {
	v1.UnimplementedOrdersServer
	Pool *pgxpool.Pool
}

// CreateOrder is idempotent on order_id. The order row and its OrderCreated event are written
// in one transaction: there is never an order without its event, or an event without its order.
func (s *Server) CreateOrder(ctx context.Context, req *v1.CreateOrderRequest) (*v1.CreateOrderResponse, error) {
	if err := validate(req); err != nil {
		return nil, err
	}
	created := false
	err := pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO orders (order_id, customer_id, amount_minor) VALUES ($1, $2, $3)
			 ON CONFLICT (order_id) DO NOTHING`,
			req.OrderId, req.CustomerId, req.AmountMinor)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // a retry of an order we already have: no second event
		}
		created = true
		_, err = outbox.Insert(ctx, tx, EventOrderCreated, req.OrderId, &v1.OrderCreated{
			OrderId: req.OrderId, CustomerId: req.CustomerId, AmountMinor: req.AmountMinor,
		})
		return err
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "creating order: %v", err)
	}
	order, err := s.get(ctx, req.OrderId)
	if err != nil {
		return nil, err
	}
	// Reusing an order_id for a different order is a client bug, not a retry.
	if !created && (order.CustomerId != req.CustomerId || order.AmountMinor != req.AmountMinor) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"order %s already exists with a different customer or amount", req.OrderId)
	}
	return &v1.CreateOrderResponse{Order: order, Created: created}, nil
}

func (s *Server) GetOrder(ctx context.Context, req *v1.GetOrderRequest) (*v1.Order, error) {
	return s.get(ctx, req.OrderId)
}

func (s *Server) get(ctx context.Context, orderID string) (*v1.Order, error) {
	o := &v1.Order{}
	err := s.Pool.QueryRow(ctx,
		`SELECT order_id, customer_id, amount_minor, (extract(epoch FROM created_at) * 1000)::bigint
		   FROM orders WHERE order_id = $1`, orderID).
		Scan(&o.OrderId, &o.CustomerId, &o.AmountMinor, &o.CreatedUnixMs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Errorf(codes.NotFound, "order %s not found", orderID)
	}
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reading order: %v", err)
	}
	return o, nil
}

func validate(req *v1.CreateOrderRequest) error {
	switch {
	case req.OrderId == "" || len(req.OrderId) > 64:
		return status.Error(codes.InvalidArgument, "order_id must be 1 to 64 characters")
	case req.CustomerId == "" || len(req.CustomerId) > 64:
		return status.Error(codes.InvalidArgument, "customer_id must be 1 to 64 characters")
	case req.AmountMinor <= 0:
		return status.Error(codes.InvalidArgument, "amount_minor must be positive")
	}
	return nil
}
