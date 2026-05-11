#!/usr/bin/env python3
"""
Parse OTel Prometheus text for oms_fix_ingress_to_nos_milliseconds histogram.
Used by shoot-ingress-orders.sh: baseline count, wait-until-delta, print full-flow ms stats.

Histogram is cumulative; quantile() follows Prometheus histogram_quantile semantics.
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path
from dataclasses import dataclass

METRIC = "oms_fix_ingress_to_nos_milliseconds"


@dataclass(frozen=True)
class HistogramSnapshot:
    count: int
    sum_ms: float
    buckets: list[tuple[float, int]]  # (upper_bound_ms, cumulative_count), +Inf last


def _parse_count(text: str) -> int | None:
    m = re.search(rf"^{re.escape(METRIC)}_count(?:\{{[^}}]*\}})?\s+(\d+)\s*$", text, re.MULTILINE)
    return int(m.group(1)) if m else None


def parse_count(text: str) -> int | None:
    return _parse_count(text)


def format_count_out(c: int | None) -> str:
    return "NA" if c is None else str(c)


def parse_histogram(text: str) -> HistogramSnapshot | None:
    c = _parse_count(text)
    if c is None or c <= 0:
        return None
    sm = re.search(rf"^{re.escape(METRIC)}_sum(?:\{{[^}}]*\}})?\s+([\d.eE+-]+)\s*$", text, re.MULTILINE)
    if not sm:
        return None
    sum_ms = float(sm.group(1))
    buckets: list[tuple[float, int]] = []
    pat = re.compile(
        rf"^{re.escape(METRIC)}_bucket(?:\{{[^}}]*le=\"([^\"]+)\"[^}}]*\}})\s+(\d+)\s*$", re.MULTILINE
    )
    for m in pat.finditer(text):
        le_raw = m.group(1)
        cnt = int(m.group(2))
        if le_raw == "+Inf":
            upper = math.inf
        else:
            upper = float(le_raw)
        buckets.append((upper, cnt))
    if not buckets:
        return HistogramSnapshot(count=c, sum_ms=sum_ms, buckets=[(math.inf, c)])
    buckets.sort(key=lambda x: (math.inf if x[0] == math.inf else x[0],))
    return HistogramSnapshot(count=c, sum_ms=sum_ms, buckets=buckets)


def parse_histogram_or_empty(text: str) -> HistogramSnapshot:
    h = parse_histogram(text)
    if h is not None:
        return h
    return HistogramSnapshot(0, 0.0, [])


def cum_le(h: HistogramSnapshot, le: float) -> int:
    if h.count == 0:
        return 0
    if math.isinf(le) and le > 0:
        return h.count
    r = 0
    for upper, cum in h.buckets:
        if math.isinf(upper):
            continue
        if upper <= le + 1e-9:
            r = max(r, cum)
    return r


def delta_histogram(before: str, after: str) -> HistogramSnapshot | None:
    hb = parse_histogram_or_empty(before)
    ha = parse_histogram_or_empty(after)
    d_count = ha.count - hb.count
    if d_count <= 0:
        return None
    d_sum = ha.sum_ms - hb.sum_ms
    finite = sorted(
        {u for u, _ in ha.buckets + hb.buckets if not math.isinf(u)},
    )
    if not finite:
        mean = d_sum / d_count if d_count else 0.0
        return HistogramSnapshot(
            count=d_count,
            sum_ms=d_sum,
            buckets=[(mean, d_count), (math.inf, d_count)],
        )
    prev = 0
    new_b: list[tuple[float, int]] = []
    for u in finite:
        d = cum_le(ha, u) - cum_le(hb, u)
        if d < prev:
            d = prev
        if d > d_count:
            d = d_count
        new_b.append((u, d))
        prev = d
    new_b.append((math.inf, d_count))
    return HistogramSnapshot(count=d_count, sum_ms=d_sum, buckets=new_b)


def histogram_quantile(q: float, h: HistogramSnapshot) -> float:
    """Returns latency in ms (same unit as bucket `le` labels for this OTel export)."""
    if h.count == 0 or not h.buckets:
        return float("nan")
    if q < 0 or q > 1 or math.isnan(q):
        return float("nan")
    rank = q * h.count
    prev_upper = 0.0
    prev_cum = 0
    for upper, cum in h.buckets:
        if cum >= rank:
            if upper == math.inf:
                return prev_upper
            bucket_width = upper - prev_upper
            bucket_count = cum - prev_cum
            if bucket_count <= 0:
                return upper
            pos = rank - prev_cum
            frac = pos / bucket_count
            return prev_upper + frac * bucket_width
        prev_upper = upper if upper != math.inf else prev_upper
        prev_cum = cum
    return prev_upper


def print_histogram_summary(h: HistogramSnapshot, *, label_count: str) -> None:
    mean = h.sum_ms / h.count
    p50 = histogram_quantile(0.50, h)
    p95 = histogram_quantile(0.95, h)
    p99 = histogram_quantile(0.99, h)
    mx = h.buckets[-1][0] if h.buckets else float("nan")
    print(f"{label_count}={h.count}")
    print(f"mean_ms={mean:.3f}")
    print(f"p50_ms={p50:.3f}")
    print(f"p95_ms={p95:.3f}")
    print(f"p99_ms={p99:.3f}")
    print(f"max_bucket_upper_ms={mx}")


def main() -> None:
    if len(sys.argv) < 2:
        print(
            "usage: parse_otel_ingress_to_nos_histogram.py count|summary <stdin>\n"
            "       parse_otel_ingress_to_nos_histogram.py delta-summary <before.prom> <after.prom>",
            file=sys.stderr,
        )
        sys.exit(2)
    mode = sys.argv[1]
    if mode == "delta-summary":
        if len(sys.argv) != 4:
            print("usage: parse_otel_ingress_to_nos_histogram.py delta-summary <before.prom> <after.prom>", file=sys.stderr)
            sys.exit(2)
        before = Path(sys.argv[2]).read_text(encoding="utf-8", errors="replace")
        after = Path(sys.argv[3]).read_text(encoding="utf-8", errors="replace")
        hd = delta_histogram(before, after)
        if hd is None:
            print(
                "status=NO_DELTA (no new ingress→NOS observations between scrapes, or missing OTel histogram lines)"
            )
            return
        print_histogram_summary(hd, label_count="count_this_run")
        return

    text = sys.stdin.read()
    if mode == "count":
        print(format_count_out(parse_count(text)))
        return
    if mode == "summary":
        h = parse_histogram(text)
        if h is None:
            print("status=NO_HISTOGRAM (no oms_fix_ingress_to_nos_milliseconds_count in scrape — wrong URL or OTel off)")
            return
        print_histogram_summary(h, label_count="count_global")
        print(
            "note=GLOBAL cumulative histogram since JVM start — use delta-summary with before/after scrapes for one batch"
        )
        return
    print("unknown mode", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
