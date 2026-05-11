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
        return None
    buckets.sort(key=lambda x: (math.inf if x[0] == math.inf else x[0],))
    return HistogramSnapshot(count=c, sum_ms=sum_ms, buckets=buckets)


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


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: parse_otel_ingress_to_nos_histogram.py count|summary", file=sys.stderr)
        sys.exit(2)
    text = sys.stdin.read()
    mode = sys.argv[1]
    if mode == "count":
        print(format_count_out(parse_count(text)))
        return
    if mode == "summary":
        h = parse_histogram(text)
    if h is None:
        print("status=NO_HISTOGRAM (no oms_fix_ingress_to_nos_milliseconds_count in scrape — wrong URL or OTel off)")
        return
        mean = h.sum_ms / h.count
        p50 = histogram_quantile(0.50, h)
        p95 = histogram_quantile(0.95, h)
        p99 = histogram_quantile(0.99, h)
        mx = h.buckets[-1][0] if h.buckets else float("nan")
        print(f"count={h.count}")
        print(f"mean_ms={mean:.3f}")
        print(f"p50_ms={p50:.3f}")
        print(f"p95_ms={p95:.3f}")
        print(f"p99_ms={p99:.3f}")
        print(f"max_bucket_upper_ms={mx}")
        return
    print("unknown mode", file=sys.stderr)
    sys.exit(2)


if __name__ == "__main__":
    main()
