// Package e2e runs the whole system in one process: real gRPC servers on localhost, real
// PostgreSQL databases, outbox workers that are crashed and restarted again and again, and a
// payment provider that fails and loses replies. At the end the books must reconcile exactly.
package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	v1 "github.com/tapishikaur/ledgerline/gen/ledgerline/v1"
	"github.com/tapishikaur/ledgerline/internal/delivery"
	"github.com/tapishikaur/ledgerline/internal/ledger"
	"github.com/tapishikaur/ledgerline/internal/orders"
	"github.com/tapishikaur/ledgerline/internal/outbox"
	"github.com/tapishikaur/ledgerline/internal/payments"
	"github.com/tapishikaur/ledgerline/internal/reconcile"
	"github.com/tapishikaur/ledgerline/internal/testdb"
)

func serve(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// crashyWorkers runs `replicas` workers on one outbox, killing each one (cancelling its
// context, possibly mid-batch) at random moments and starting a fresh one, until stop is
// closed. Returns a channel that receives the number of crashes when they've all exited.
func crashyWorkers(pool *pgxpool.Pool, handlers map[string]outbox.Handler, replicas int, seed uint64, stop <-chan struct{}) <-chan int {
	crashes := make(chan int, 1)
	var mu sync.Mutex
	total := 0
	var wg sync.WaitGroup
	for r := 0; r < replicas; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(r)))
			for {
				ctx, kill := context.WithCancel(context.Background())
				w := &outbox.Worker{Pool: pool, Handlers: handlers, BatchSize: 20, Parallel: 8, IdleWait: 5 * time.Millisecond}
				done := make(chan struct{})
				go func() { _ = w.Run(ctx); close(done) }()
				select {
				case <-stop:
					kill()
					<-done
					return
				// Long enough for batches to commit, short enough to crash often, many of them
				// mid-batch. (A killed worker's connection is discarded and replaced; PostgreSQL on
				// Windows starts a process per connection, which can take a second or more, so much
				// shorter lives would leave no time for any work there.)
				case <-time.After(lifetime(rng)):
					kill() // a crash: whatever batch it was in rolls back
					<-done
					mu.Lock()
					total++
					mu.Unlock()
				}
			}
		}(r)
	}
	go func() {
		wg.Wait()
		crashes <- total
	}()
	return crashes
}

// lifetime is how long a worker runs before it's killed. E2E_NO_CRASH=1 turns crashes off,
// to measure the same flow without them.
func lifetime(rng *rand.Rand) time.Duration {
	if os.Getenv("E2E_NO_CRASH") != "" {
		return time.Hour
	}
	return time.Duration(300+rng.IntN(2700)) * time.Millisecond
}

func TestEverythingReconcilesThroughCrashesAndProviderFailures(t *testing.T) {
	ordersDB := testdb.New(t, "orders")
	paymentsDB := testdb.New(t, "payments")
	ledgerDB := testdb.New(t, "ledger")

	ordersConn := serve(t, func(s *grpc.Server) { v1.RegisterOrdersServer(s, &orders.Server{Pool: ordersDB}) })
	paymentsConn := serve(t, func(s *grpc.Server) {
		v1.RegisterPaymentsServer(s, &payments.Server{Pool: paymentsDB, Provider: &payments.Provider{
			Pool: paymentsDB, FailRate: 0.05, LostReplyRate: 0.05, // a flaky card processor
		}})
	})
	ledgerConn := serve(t, func(s *grpc.Server) { v1.RegisterLedgerServer(s, &ledger.Server{Pool: ledgerDB}) })

	stop := make(chan struct{})
	crashesA := crashyWorkers(ordersDB, delivery.ToPayments(v1.NewPaymentsClient(paymentsConn)), 2, 1, stop)
	crashesB := crashyWorkers(paymentsDB, delivery.ToLedger(v1.NewLedgerClient(ledgerConn)), 2, 2, stop)

	// Create orders concurrently; every request is retried until it succeeds, with the same
	// order_id, and a few are sent twice on purpose (a client retry after a lost reply).
	n := 3000
	if v, err := strconv.Atoi(os.Getenv("E2E_ORDERS")); err == nil && v > 0 {
		n = v // a quicker run while debugging
	}
	client := v1.NewOrdersClient(ordersConn)
	var wg sync.WaitGroup
	next := make(chan int)
	go func() {
		for i := 0; i < n; i++ {
			next <- i
		}
		close(next)
	}()
	for c := 0; c < 16; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				req := &v1.CreateOrderRequest{OrderId: fmt.Sprintf("o%05d", i), CustomerId: fmt.Sprintf("c%03d", i%97),
					AmountMinor: int64(100 + i*7%9000)}
				sends := 1
				if i%50 == 0 {
					sends = 2
				}
				for s := 0; s < sends; s++ {
					for {
						_, err := client.CreateOrder(context.Background(), req)
						if err == nil {
							break
						}
						if status.Code(err) != codes.Unavailable {
							t.Error(err)
							return
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	// Let the crashing workers drain both outboxes, then stop them.
	// Generous: PostgreSQL on Windows starts a process per connection, which is slow on some
	// machines; on Linux this drains in seconds.
	drain := 8 * time.Minute
	if d, err := time.ParseDuration(os.Getenv("E2E_DRAIN")); err == nil {
		drain = d
	}
	deadline := time.Now().Add(drain)
	for {
		a, _ := outbox.Backlog(context.Background(), ordersDB)
		b, _ := outbox.Backlog(context.Background(), paymentsDB)
		if a+b == 0 {
			break
		}
		if time.Now().After(deadline) {
			for name, pool := range map[string]*pgxpool.Pool{"orders": ordersDB, "payments": paymentsDB} {
				var attempts int
				var lastErr *string
				_ = pool.QueryRow(context.Background(),
					`SELECT attempts, last_error FROM outbox WHERE sent_at IS NULL ORDER BY attempts DESC LIMIT 1`).
					Scan(&attempts, &lastErr)
				if lastErr != nil {
					t.Logf("%s outbox: an undelivered event has %d attempts; last error: %s", name, attempts, *lastErr)
				}
			}
			t.Fatalf("outboxes didn't drain: %d + %d left", a, b)
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(stop)
	crashes := <-crashesA + <-crashesB

	report, err := reconcile.Check(context.Background(), ordersDB, paymentsDB, ledgerDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d orders, %d payments, %d ledger entries; workers crashed %d times", report.Orders, report.Payments,
		report.LedgerEntries, crashes)
	if report.Breaks() != 0 {
		t.Fatalf("%d breaks:\n%s", report.Breaks(), report.Markdown("e2e"))
	}
	if report.Orders != int64(n) || report.Payments != int64(n) || report.LedgerEntries != int64(2*n) {
		t.Fatalf("orders %d, payments %d, entries %d; want %d, %d, %d", report.Orders, report.Payments,
			report.LedgerEntries, n, n, 2*n)
	}
	if crashes < 10 {
		t.Fatalf("only %d worker crashes: the test didn't stress recovery", crashes)
	}
	var retriedCharges int
	_ = paymentsDB.QueryRow(context.Background(), `SELECT count(*) FROM provider_charges WHERE calls > 1`).Scan(&retriedCharges)
	t.Logf("%d charges were asked for more than once and still charged once", retriedCharges)
	if retriedCharges == 0 {
		t.Fatal("no provider retries happened: the lost-reply path wasn't exercised")
	}
}
