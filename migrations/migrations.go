// Package migrations holds each service's database schema, embedded in the binaries.
package migrations

import "embed"

// FS holds orders/*.sql, payments/*.sql and ledger/*.sql, applied in name order.
//
//go:embed orders/*.sql payments/*.sql ledger/*.sql
var FS embed.FS
