// Package testdb gives each test its own freshly migrated PostgreSQL database.
package testdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tapishikaur/ledgerline/internal/db"
	"github.com/tapishikaur/ledgerline/migrations"
)

// New creates a database with the given service's schema (orders, payments or ledger) and
// drops it when the test ends. Skips the test if LEDGERLINE_TEST_DB isn't set.
func New(t testing.TB, service string) *pgxpool.Pool {
	t.Helper()
	server := os.Getenv("LEDGERLINE_TEST_DB")
	if server == "" {
		t.Skip("LEDGERLINE_TEST_DB not set (scripts/test.sh starts a local PostgreSQL)")
	}
	ctx := context.Background()
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	name := "t_" + service + "_" + hex.EncodeToString(suffix)
	url, err := db.CreateDatabase(ctx, server, name)
	if err != nil {
		t.Fatalf("creating database: %v", err)
	}
	pool, err := db.Open(ctx, url, 40, 10*time.Second)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if err := db.DropDatabase(context.Background(), server, name); err != nil {
			t.Logf("dropping %s: %v", name, err)
		}
	})
	if err := db.Migrate(ctx, pool, migrations.FS, service); err != nil {
		t.Fatalf("migrating: %v", err)
	}
	return pool
}
