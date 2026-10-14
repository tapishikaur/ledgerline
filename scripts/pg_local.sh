#!/usr/bin/env bash
# A throwaway PostgreSQL cluster for local tests: port 54331, user "ledgerline", no password,
# local connections only, data outside the repo. It never touches any other PostgreSQL server.
# To remove it: stop it, then delete the pgdata folder printed by "status".
#
# Usage: scripts/pg_local.sh start | stop | status
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/scripts/env.sh"

PG_BIN=""
for d in /c/Program\ Files/PostgreSQL/*/bin /usr/lib/postgresql/*/bin; do
  [ -x "$d/initdb$EXE" ] && [ -x "$d/pg_ctl$EXE" ] && PG_BIN="$d"
done
if [ -z "$PG_BIN" ] && command -v initdb > /dev/null; then PG_BIN="$(dirname "$(command -v initdb)")"; fi
[ -n "$PG_BIN" ] || { echo "PostgreSQL server programs (initdb, pg_ctl) not found" >&2; exit 1; }

pgctl() { "$PG_BIN/pg_ctl$EXE" -D "$LEDGERLINE_PG_DATA" "$@"; }

case "${1:-status}" in
  start)
    if [ ! -f "$LEDGERLINE_PG_DATA/PG_VERSION" ]; then
      mkdir -p "$LEDGERLINE_PG_DATA"
      "$PG_BIN/initdb$EXE" -D "$LEDGERLINE_PG_DATA" -U ledgerline -A trust -E UTF8 --no-locale > "$LEDGERLINE_OUT/pg_initdb.log"
    fi
    if pgctl status > /dev/null 2>&1; then
      echo "PostgreSQL already running on port $LEDGERLINE_PG_PORT"
    else
      # No stdin/stdout/stderr for the server: on Windows it would inherit (and hold open) the
      # caller's pipe, so `scripts/test.sh | tail` would never finish.
      pgctl -w -l "$LEDGERLINE_OUT/pg_server.log" \
        -o "-p $LEDGERLINE_PG_PORT -c listen_addresses=localhost -c max_connections=200" start \
        < /dev/null > /dev/null 2>&1
      echo "PostgreSQL started on port $LEDGERLINE_PG_PORT"
    fi
    ;;
  stop)
    if pgctl status > /dev/null 2>&1; then pgctl -w -m fast stop > /dev/null && echo "PostgreSQL stopped"; else echo "PostgreSQL not running"; fi
    ;;
  status)
    echo "data: $LEDGERLINE_PG_DATA"
    pgctl status || true
    ;;
  *)
    echo "usage: $0 start | stop | status" >&2
    exit 2
    ;;
esac
