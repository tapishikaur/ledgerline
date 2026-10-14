#!/usr/bin/env bash
# The chaos test, against the real Docker Compose stack. loadgen creates ORDERS orders (each
# produces two events, OrderCreated and PaymentCaptured, so 250,000 orders = 500,000 events)
# while this script docker-kills an outbox worker or the ledger KILLS times at random moments
# (SIGKILL: no shutdown, no flush; whatever it was doing is cut off) and starts it again. When
# everything has drained, the reconcile tool checks every order, charge, payment and ledger entry.
#
# Usage:  scripts/chaos.sh [ORDERS=250000] [KILLS=50] [SEED=7] [results file]
# Needs:  Docker with compose (a Codespace has it). Starts the stack itself.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
ORDERS="${1:-250000}"
KILLS="${2:-50}"
SEED="${3:-7}"
OUT="${4:-results/chaos-$((ORDERS * 2))-events.md}"
RUN="chaos$(date +%s)"
NET="ledgerline_default"
DB="postgres://ledgerline:ledgerline@postgres:5432"
mkdir -p "$(dirname "$OUT")"

docker compose up --build -d --wait
app() { docker run --rm --network "$NET" ledgerline:dev "$@"; }

echo "chaos: $ORDERS orders ($((ORDERS * 2)) events), $KILLS kills, seed $SEED"
start=$(date +%s)
app loadgen -addr orders:50051 -n "$ORDERS" -concurrency 32 -run "$RUN" -seed "$SEED" > /tmp/loadgen.out 2>&1 &
LOADGEN=$!

RANDOM=$SEED # bash's RANDOM is repeatable from a seed
targets=(worker-orders worker-payments ledger)
rows=()
for i in $(seq 1 "$KILLS"); do
  sleep "$((2 + RANDOM % 10)).$((RANDOM % 10))"
  target="${targets[$((RANDOM % ${#targets[@]}))]}"
  docker compose kill -s SIGKILL "$target" > /dev/null 2>&1
  down="$((RANDOM % 3)).$((RANDOM % 10))"
  sleep "$down"
  docker compose start "$target" > /dev/null 2>&1
  rows+=("| $i | $(($(date +%s) - start)) s | $target | ${down} s |")
  echo "kill $i/$KILLS: $target (down ${down} s)"
done

wait "$LOADGEN" || { cat /tmp/loadgen.out; exit 1; }
loadgen_line="$(grep '^loadgen:' /tmp/loadgen.out)"
echo "$loadgen_line"

status=0
app reconcile -orders-db "$DB/orders?sslmode=disable" -payments-db "$DB/payments?sslmode=disable" \
  -ledger-db "$DB/ledger?sslmode=disable" -expect-orders "$ORDERS" -wait 60m \
  -title "Reconciliation after $KILLS kills" > /tmp/reconcile.md 2>&1 || status=$?
elapsed=$(($(date +%s) - start))

{
  echo "# Chaos test: $((ORDERS * 2)) events, $KILLS forced crashes"
  echo
  echo "The full Docker Compose stack ran while \`loadgen\` created $ORDERS orders (each produces OrderCreated and"
  echo "PaymentCaptured: $((ORDERS * 2)) events). Meanwhile an outbox worker or the ledger service was killed with"
  echo "\`docker kill\` (SIGKILL: no shutdown, no flush) $KILLS times at random moments (seed $SEED), and started again."
  echo "After both outboxes drained, every order, provider charge, payment and ledger entry was reconciled."
  echo
  echo "Load: $loadgen_line"
  echo
  echo "Total time including the drain: $elapsed s. Machine: $(nproc) CPUs, $(uname -srm)."
  echo
  echo "| Kill | At | Container | Down for |"
  echo "|---|---|---|---|"
  printf '%s\n' "${rows[@]}"
  echo
  sed -n '/^# /,$p' /tmp/reconcile.md | sed 's/^# /## /'
} > "$OUT"
cat "$OUT" | tail -25
echo "report: $OUT"
exit $status
