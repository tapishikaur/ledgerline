package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/db"
	"github.com/tapishikaur/ledgerline/internal/outbox"
	"github.com/tapishikaur/ledgerline/internal/reconcile"
)

// runLoadgen creates synthetic orders as fast as -concurrency allows. Order IDs are
// "<run>-<n>", amounts and customers come from a fixed seed, and a failed call is retried
// with the same order_id (CreateOrder is idempotent, so a retry never makes a second order).
func runLoadgen(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ExitOnError)
	addr := fs.String("addr", "localhost:50051", "orders service")
	n := fs.Int("n", 1000, "orders to create")
	concurrency := fs.Int("concurrency", 32, "requests in flight")
	run := fs.String("run", "run", "order_id prefix")
	seed := fs.Uint64("seed", 42, "random seed for amounts and customers")
	customers := fs.Int("customers", 1000, "distinct customers")
	_ = fs.Parse(args)

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := v1.NewOrdersClient(conn)

	var next, created, existed, retries atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	errc := make(chan error, *concurrency)
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(*n) || ctx.Err() != nil {
					return
				}
				// Per-order randomness from (seed, i): the same order always gets the same values.
				r := rand.New(rand.NewPCG(*seed, uint64(i)))
				req := &v1.CreateOrderRequest{
					OrderId:     fmt.Sprintf("%s-%08d", *run, i),
					CustomerId:  fmt.Sprintf("c%05d", r.IntN(*customers)),
					AmountMinor: 100 + r.Int64N(99_900), // 1.00 to 999.99
				}
				for attempt := 0; ; attempt++ {
					cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
					resp, err := client.CreateOrder(cctx, req)
					cancel()
					if err == nil {
						if resp.Created {
							created.Add(1)
						} else {
							existed.Add(1)
						}
						break
					}
					if c := status.Code(err); (c != codes.Unavailable && c != codes.DeadlineExceeded) || attempt >= 50 {
						errc <- fmt.Errorf("order %s: %w", req.OrderId, err)
						return
					}
					retries.Add(1)
					time.Sleep(time.Duration(min(attempt+1, 20)) * 50 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	close(errc)
	if err := <-errc; err != nil {
		return err
	}
	elapsed := time.Since(start)
	fmt.Printf("loadgen: %d orders (%d created, %d already existed, %d retries) in %.1f s, %.0f orders/s\n",
		created.Load()+existed.Load(), created.Load(), existed.Load(), retries.Load(), elapsed.Seconds(),
		float64(created.Load()+existed.Load())/elapsed.Seconds())
	return nil
}

// runReconcile waits until both outboxes are drained (or -wait passes), then compares the three
// databases. Exits 1 if there is any break.
func runReconcile(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	ordersURL := fs.String("orders-db", os.Getenv("ORDERS_DATABASE_URL"), "orders database URL")
	paymentsURL := fs.String("payments-db", os.Getenv("PAYMENTS_DATABASE_URL"), "payments database URL")
	ledgerURL := fs.String("ledger-db", os.Getenv("LEDGER_DATABASE_URL"), "ledger database URL")
	wait := fs.Duration("wait", 5*time.Minute, "how long to wait for the outboxes to drain")
	expect := fs.Int64("expect-orders", -1, "fail unless exactly this many orders exist (-1: don't check)")
	out := fs.String("out", "", "also write the report (Markdown) here")
	title := fs.String("title", "Reconciliation", "report title")
	_ = fs.Parse(args)

	ordersDB, err := db.Open(ctx, *ordersURL, 4, 30*time.Second)
	if err != nil {
		return fmt.Errorf("orders db: %w", err)
	}
	defer ordersDB.Close()
	paymentsDB, err := db.Open(ctx, *paymentsURL, 4, 30*time.Second)
	if err != nil {
		return fmt.Errorf("payments db: %w", err)
	}
	defer paymentsDB.Close()
	ledgerDB, err := db.Open(ctx, *ledgerURL, 4, 30*time.Second)
	if err != nil {
		return fmt.Errorf("ledger db: %w", err)
	}
	defer ledgerDB.Close()

	// Drained: both outboxes empty. Each event's consumer has committed before it's marked sent.
	deadline := time.Now().Add(*wait)
	for {
		a, err1 := outbox.Backlog(ctx, ordersDB)
		b, err2 := outbox.Backlog(ctx, paymentsDB)
		if err := errors.Join(err1, err2); err != nil {
			return err
		}
		if a+b == 0 || time.Now().After(deadline) {
			break
		}
		fmt.Printf("reconcile: waiting for %d undelivered events\n", a+b)
		time.Sleep(time.Second)
	}

	report, err := reconcile.Check(ctx, ordersDB, paymentsDB, ledgerDB)
	if err != nil {
		return err
	}
	md := report.Markdown(*title)
	fmt.Print(md)
	if *out != "" {
		if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
			return err
		}
	}
	if *expect >= 0 && report.Orders != *expect {
		return fmt.Errorf("expected %d orders, found %d", *expect, report.Orders)
	}
	if report.Breaks() > 0 {
		return fmt.Errorf("%d breaks", report.Breaks())
	}
	return nil
}
