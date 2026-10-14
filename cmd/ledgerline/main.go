// ledgerline: one binary for every part of the system, chosen by the first argument.
//
//	ledgerline orders | payments | ledger     run a service (configured by environment variables)
//	ledgerline outbox-worker                  deliver one service's outbox to the next service
//	ledgerline loadgen   [flags]              create synthetic orders
//	ledgerline reconcile [flags]              check all three databases against each other
//	ledgerline health ADDR                    exit 0 if the gRPC service at ADDR is serving
//
// One binary means one Docker image; Compose runs it with different commands.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "orders", "payments", "ledger":
		err = runService(ctx, os.Args[1])
	case "outbox-worker":
		err = runWorker(ctx)
	case "loadgen":
		err = runLoadgen(ctx, os.Args[2:])
	case "reconcile":
		err = runReconcile(ctx, os.Args[2:])
	case "health":
		err = runHealth(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "ledgerline %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: ledgerline orders|payments|ledger|outbox-worker|loadgen|reconcile|health [args]")
	os.Exit(2)
}

// env returns an environment variable, or fallback if it's unset or empty.
func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func mustEnv(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("%s must be set", name)
	}
	return v, nil
}
