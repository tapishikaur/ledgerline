"""Proves one trace follows an order through every service and both outbox workers.

Creates one order, waits for it to reach the ledger, fetches its trace from Jaeger's HTTP API,
and checks it contains spans from all five processes. Writes a Markdown report; with
--screenshot, also saves a PNG of the trace in the Jaeger UI (needs: pip install playwright,
then: python -m playwright install --with-deps chromium).

    python scripts/trace_check.py --out results/trace-check.md --screenshot results/trace.png
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

JAEGER = "http://localhost:16686"
EXPECTED = ["orders", "outbox-worker-payments", "payments", "outbox-worker-ledger", "ledger"]


def jaeger(path: str) -> dict:
    """GET from Jaeger's HTTP API. Jaeger answers 404 while it has no spans yet for a service
    (spans are exported in batches), so 404 means "nothing yet", not an error."""
    try:
        with urllib.request.urlopen(JAEGER + path, timeout=10) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"data": []}
        raise


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--screenshot")
    args = p.parse_args()

    order_id = f"trace-{int(time.time())}"
    subprocess.run(["docker", "run", "--rm", "--network", "ledgerline_default", "ledgerline:dev",
                    "loadgen", "-addr", "orders:50051", "-n", "1", "-run", order_id], check=True)
    order_id += "-00000000"

    # The order is in the ledger once both workers have delivered; spans are exported in batches.
    trace = None
    for _ in range(60):
        time.sleep(1)
        found = jaeger(f"/api/traces?service=orders&tags=%7B%7D&limit=50&lookback=1h").get("data", [])
        for t in found:
            tags = [tag for s in t["spans"] for tag in s.get("tags", [])]
            if any(tag.get("value") == order_id for tag in tags):
                services = {t["processes"][s["processID"]]["serviceName"] for s in t["spans"]}
                if set(EXPECTED) <= services:
                    trace = t
                    break
        if trace:
            break
    if not trace:
        print(f"no complete trace found for {order_id}", file=sys.stderr)
        return 1

    spans = sorted(trace["spans"], key=lambda s: s["startTime"])
    start = spans[0]["startTime"]
    lines = [
        "# One trace, one order, every service",
        "",
        f"Trace `{trace['traceID']}` for order `{order_id}`, fetched from Jaeger's API: "
        f"{len(spans)} spans from {len({s['processID'] for s in spans})} processes.",
        "",
        "| Start (ms) | Duration (ms) | Service | Span |",
        "|---|---|---|---|",
    ]
    for s in spans:
        svc = trace["processes"][s["processID"]]["serviceName"]
        lines.append(f"| {(s['startTime'] - start) / 1000:.1f} | {s['duration'] / 1000:.1f} | {svc} | {s['operationName']} |")
    lines += ["", "**Result: PASS**: spans from " + ", ".join(EXPECTED) + " are all in one trace."]
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))

    if args.screenshot:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page(viewport={"width": 1600, "height": 1000})
            page.goto(f"{JAEGER}/trace/{trace['traceID']}", wait_until="networkidle")
            page.wait_for_timeout(1500)
            page.screenshot(path=args.screenshot, full_page=True)
            browser.close()
        print(f"screenshot: {args.screenshot}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
