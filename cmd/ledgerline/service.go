package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/db"
	"github.com/tapishikaur/ledgerline/internal/delivery"
	"github.com/tapishikaur/ledgerline/internal/ledger"
	"github.com/tapishikaur/ledgerline/internal/orders"
	"github.com/tapishikaur/ledgerline/internal/outbox"
	"github.com/tapishikaur/ledgerline/internal/payments"
	"github.com/tapishikaur/ledgerline/internal/telemetry"
	"github.com/tapishikaur/ledgerline/migrations"
)

// runService runs the orders, payments or ledger gRPC service.
//
// Environment: DATABASE_URL (required), LISTEN_ADDR (default :50051), DB_MAX_CONNS (default 20),
// OTEL_EXPORTER_OTLP_ENDPOINT (optional). payments also reads PROVIDER_LATENCY_MS,
// PROVIDER_FAIL_RATE and PROVIDER_LOST_REPLY_RATE to simulate a misbehaving card processor.
func runService(ctx context.Context, name string) error {
	dbURL, err := mustEnv("DATABASE_URL")
	if err != nil {
		return err
	}
	shutdown, err := telemetry.Setup(ctx, name)
	if err != nil {
		return err
	}
	defer shutdown(context.Background()) //nolint:errcheck

	pool, err := db.Open(ctx, dbURL, int32(intEnv("DB_MAX_CONNS", 20)), time.Minute)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool, migrations.FS, name); err != nil {
		return fmt.Errorf("migrating: %w", err)
	}

	srv := grpc.NewServer(telemetry.ServerOption())
	switch name {
	case "orders":
		v1.RegisterOrdersServer(srv, &orders.Server{Pool: pool})
	case "payments":
		v1.RegisterPaymentsServer(srv, &payments.Server{Pool: pool, Provider: &payments.Provider{
			Pool:          pool,
			Latency:       time.Duration(intEnv("PROVIDER_LATENCY_MS", 0)) * time.Millisecond,
			FailRate:      floatEnv("PROVIDER_FAIL_RATE", 0),
			LostReplyRate: floatEnv("PROVIDER_LOST_REPLY_RATE", 0),
		}})
	case "ledger":
		v1.RegisterLedgerServer(srv, &ledger.Server{Pool: pool})
	}
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)

	lis, err := net.Listen("tcp", env("LISTEN_ADDR", ":50051"))
	if err != nil {
		return err
	}
	slog.Info("serving", "service", name, "addr", lis.Addr().String())
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case <-ctx.Done():
		healthSrv.Shutdown()
		srv.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}

// runWorker delivers one outbox to its consumer.
//
// Environment: SOURCE_DATABASE_URL (the outbox's database), TARGET (payments or ledger),
// TARGET_ADDR (host:port), BATCH_SIZE (100), PARALLEL (16), OTEL_EXPORTER_OTLP_ENDPOINT.
func runWorker(ctx context.Context) error {
	dbURL, err := mustEnv("SOURCE_DATABASE_URL")
	if err != nil {
		return err
	}
	target, err := mustEnv("TARGET")
	if err != nil {
		return err
	}
	addr, err := mustEnv("TARGET_ADDR")
	if err != nil {
		return err
	}
	shutdown, err := telemetry.Setup(ctx, "outbox-worker-"+target)
	if err != nil {
		return err
	}
	defer shutdown(context.Background()) //nolint:errcheck

	pool, err := db.Open(ctx, dbURL, 4, time.Minute)
	if err != nil {
		return err
	}
	defer pool.Close()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), telemetry.DialOption())
	if err != nil {
		return err
	}
	defer conn.Close()

	var handlers map[string]outbox.Handler
	switch target {
	case "payments":
		handlers = delivery.ToPayments(v1.NewPaymentsClient(conn))
	case "ledger":
		handlers = delivery.ToLedger(v1.NewLedgerClient(conn))
	default:
		return fmt.Errorf("TARGET must be payments or ledger, not %q", target)
	}
	w := &outbox.Worker{Pool: pool, Handlers: handlers, BatchSize: intEnv("BATCH_SIZE", 100), Parallel: intEnv("PARALLEL", 16)}
	slog.Info("delivering outbox", "target", target, "addr", addr)
	return w.Run(ctx)
}

// runHealth checks a service's gRPC health endpoint (used by Docker health checks).
func runHealth(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ledgerline health HOST:PORT")
	}
	conn, err := grpc.NewClient(args[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

func intEnv(name string, fallback int) int {
	if v, err := strconv.Atoi(env(name, "")); err == nil {
		return v
	}
	return fallback
}

func floatEnv(name string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(env(name, ""), 64); err == nil {
		return v
	}
	return fallback
}
