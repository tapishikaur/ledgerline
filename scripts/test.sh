#!/usr/bin/env bash
# Runs every test. The database tests need PostgreSQL: set LEDGERLINE_TEST_DB to a server URL
# (any database on it; tests create and drop their own), or leave it unset to start the
# throwaway local cluster (scripts/pg_local.sh).
#
# Usage: scripts/test.sh [go test flags]     e.g. scripts/test.sh -race -count=1
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"
cd "$ROOT"

if [ -z "${LEDGERLINE_TEST_DB:-}" ]; then
  bash scripts/pg_local.sh start
  export LEDGERLINE_TEST_DB="$LEDGERLINE_LOCAL_DB"
fi
go vet ./...
go test "$@" ./...
