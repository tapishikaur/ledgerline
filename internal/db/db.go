// Package db opens PostgreSQL connection pools and applies schema migrations.
package db

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open connects to url, waiting up to `wait` for the database to come up (containers start
// in any order), and checks the connection.
func Open(ctx context.Context, url string, maxConns int32, wait time.Duration) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("bad database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	// Keep a few connections open: opening one is slow on some platforms (PostgreSQL on
	// Windows starts a process per connection).
	cfg.MinConns = min(4, cfg.MaxConns)
	deadline := time.Now().Add(wait)
	for {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("can't reach the database: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Migrate applies every *.sql file in dir of files, in name order, each once, recording what
// was applied in schema_migrations. Each file runs in its own transaction. An advisory lock
// stops two processes starting at once from applying the same file twice.
func Migrate(ctx context.Context, pool *pgxpool.Pool, files fs.FS, dir string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	const lockID = 7_313_731 // any fixed number shared by every process migrating this database
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	names, err := fs.Glob(files, dir+"/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		var done bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)", name).
			Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := fs.ReadFile(files, name)
		if err != nil {
			return err
		}
		err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, string(sql)); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (name) VALUES ($1)", name)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateDatabase creates a database (if missing) on the server that serverURL points at, and
// returns the URL of the new database. For tests and for first start-up.
func CreateDatabase(ctx context.Context, serverURL, name string) (string, error) {
	if !validName(name) {
		return "", fmt.Errorf("database name must be lowercase letters, digits and _: %q", name)
	}
	conn, err := pgx.Connect(ctx, serverURL)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name).
		Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
			return "", err
		}
	}
	return withDatabase(serverURL, name)
}

// DropDatabase removes a database, closing any connections to it first. For tests.
func DropDatabase(ctx context.Context, serverURL, name string) error {
	if !validName(name) {
		return fmt.Errorf("bad database name %q", name)
	}
	conn, err := pgx.Connect(ctx, serverURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	return err
}

func withDatabase(serverURL, name string) (string, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}
	u.Path = "/" + name
	return u.String(), nil
}

func validName(name string) bool {
	if name == "" || len(name) > 48 {
		return false
	}
	return strings.Trim(name, "abcdefghijklmnopqrstuvwxyz0123456789_") == "" && !(name[0] >= '0' && name[0] <= '9')
}
