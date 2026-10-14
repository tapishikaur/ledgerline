"""Proves one trace follows an order through every service and both outbox workers.

Creates one order, waits for it to reach the ledger, finds its trace through Jaeger's API (v3,
OTLP JSON), and checks it contains spans from all five processes. Writes a Markdown report;
with --screenshot, also saves a PNG of the trace in the Jaeger UI (needs: pip install
playwright, then: python -m playwright install --with-deps chromium).

    python3 scripts/trace_check.py --out results/trace-check.md --screenshot results/trace.png
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

JAEGER = "http://localhost:16686"
EXPECTED = ["orders", "outbox-worker-payments", "payments", "outbox-worker-ledger", "ledger"]


def get(path: str, **params) -> dict:
    """GET from Jaeger's v3 API. It answers 404 when nothing matches yet (spans are exported in
    batches), which means "nothing yet", not an error."""
    url = JAEGER + path + ("?" + urllib.parse.urlencode(params) if params else "")
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {}
        raise


def spans_of(result: dict) -> list[dict]:
    """Flattens OTLP JSON into one dict per span, tagged with its service name."""
    out = []
    for rs in result.get("result", {}).get("resourceSpans", []):
        attrs = {a["key"]: a["value"] for a in rs.get("resource", {}).get("attributes", [])}
        service = attrs.get("service.name", {}).get("stringValue", "?")
        for ss in rs.get("scopeSpans", []):
            for s in ss.get("spans", []):
                s = dict(s)
                s["service"] = service
                s["attrs"] = {a["key"]: next(iter(a["value"].values()), None) for a in s.get("attributes", [])}
                out.append(s)
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--screenshot")
    args = p.parse_args()

    run = f"trace-{int(time.time())}"
    subprocess.run(["docker", "run", "--rm", "--network", "ledgerline_default", "ledgerline:dev",
                    "loadgen", "-addr", "orders:50051", "-n", "1", "-run", run], check=True)
    order_id = run + "-00000000"

    now = datetime.now(timezone.utc)
    window = {"query.start_time_min": (now - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
              "query.start_time_max": (now + timedelta(minutes=10)).isoformat().replace("+00:00", "Z")}
    trace_id, spans = None, []
    for _ in range(60):
        time.sleep(1)
        # The worker's delivery spans carry the order ID; find the trace through them.
        found = spans_of(get("/api/v3/traces", **{"query.service_name": "outbox-worker-payments",
                                                   "query.num_traces": "50", **window}))
        ids = {s["traceId"] for s in found if s["attrs"].get("outbox.aggregate_id") == order_id}
        for tid in ids:
            spans = spans_of(get(f"/api/v3/traces/{tid}"))
            if set(EXPECTED) <= {s["service"] for s in spans}:
                trace_id = tid
                break
        if trace_id:
            break
    if not trace_id:
        print(f"no complete trace found for {order_id}", file=sys.stderr)
        return 1

    spans.sort(key=lambda s: int(s["startTimeUnixNano"]))
    start = int(spans[0]["startTimeUnixNano"])
    lines = [
        "# One trace, one order, every service",
        "",
        f"Trace `{trace_id}` for order `{order_id}`, fetched from Jaeger's API: {len(spans)} spans from "
        f"{len({s['service'] for s in spans})} services, all with the same trace ID.",
        "",
        "| Start (ms) | Duration (ms) | Service | Span |",
        "|---|---|---|---|",
    ]
    for s in spans:
        begin = (int(s["startTimeUnixNano"]) - start) / 1e6
        took = (int(s["endTimeUnixNano"]) - int(s["startTimeUnixNano"])) / 1e6
        lines.append(f"| {begin:.1f} | {took:.1f} | {s['service']} | {s['name']} |")
    lines += ["", "**Result: PASS**: spans from " + ", ".join(EXPECTED) + " are all in one trace."]
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))

    if args.screenshot:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page(viewport={"width": 1600, "height": 1000})
            page.goto(f"{JAEGER}/trace/{trace_id}", wait_until="networkidle")
            page.wait_for_timeout(2000)
            page.screenshot(path=args.screenshot, full_page=True)
            browser.close()
        print(f"screenshot: {args.screenshot}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
