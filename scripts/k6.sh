#!/usr/bin/env bash
# Load test with k6 against the running Compose stack: CreateOrder at a constant RATE per second
# for DURATION, from a k6 container on the same machine (and the same 2 CPUs) as the stack.
# Writes the k6 summary and the machine description to OUT.
#
# Usage:  scripts/k6.sh [RATE=300] [DURATION=60s] [OUT=/tmp/k6-summary.txt]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RATE="${1:-300}"
DURATION="${2:-60s}"
OUT="${3:-/tmp/k6-summary.txt}"

docker compose up --build -d --wait
# Warm-up (not reported): fills connection pools and caches.
docker run --rm --network ledgerline_default -v "$ROOT:/work" -w /work/loadtest grafana/k6:1.8.2 run --quiet \
  -e ADDR=orders:50051 -e RATE="$RATE" -e DURATION=15s -e RUN="warm$(date +%s)" create_order.js > /dev/null

{
  echo "== k6 CreateOrder, $RATE requests/s for $DURATION"
  echo "machine: $(nproc) CPUs, $(free -g | awk '/Mem:/ {print $2}') GB RAM, $(uname -srm); stack and k6 on the same machine"
  docker run --rm --network ledgerline_default -v "$ROOT:/work" -w /work/loadtest grafana/k6:1.8.2 run \
    -e ADDR=orders:50051 -e RATE="$RATE" -e DURATION="$DURATION" -e RUN="k6$(date +%s)" create_order.js 2>&1 |
    grep -vE "^\s*$|execution: local|script:|output:"
} | tee "$OUT"
