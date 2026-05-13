#!/usr/bin/env python3
"""
Summarise per-role Prometheus scrapes for the post-Phase-3 multi-JVM OMS topology.

Replaces summarize_micrometer_pipeline_deltas.py (slice 4i) which was wired against the
deleted Phase-1 monolith breakdown.

Inputs:
- Pre/post Prometheus scrape file pairs for each role (any subset; absent files are skipped).
- A `created` count from the shoot loop (HTTP 201s) so per-request averages are honest.

Outputs:
- Histogram-based p50/p99 in milliseconds for ingress-replica timers (Δ count and sum across the run).
- Gauge values from projector + fix-egress at end-of-run (`oms.projector.lag_seconds`,
  `oms.fix_egress.lag_seconds`); flags sentinel `-1.0` separately ("publisher has no row yet").
- Snapshot freshness sanity from cluster-node `oms.cluster.snapshot.age_seconds` (slice 4h).
- A derived "ingress→NOS upper bound" = ingress-replica `oms.cluster.client.commit_round_trip` p99
  + end-of-run `oms.fix_egress.lag_seconds` (NOT a per-order histogram; an honest upper-bound proxy
  for the cross-JVM wall clock that lost its single-process sample point in slice 3b-2).

Per ecosystem `config-and-limits` rule: thresholds and metric names live as named module-level
constants below.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# --- Named constants (config-and-limits) ---

INGRESS_TIMERS_MS: List[Tuple[str, str]] = [
    ("oms_pipeline_ingress_accept", "Ingress: Postgres accept tx (commit)"),
    ("oms_cluster_client_commit_round_trip", "Cluster client: commit round-trip (slice 4c)"),
]
"""Ingress-replica histogram timers (Prometheus base names; `_seconds_*` suffixes are appended)."""

PROJECTOR_GAUGES: List[Tuple[str, str]] = [
    ("oms_projector_lag_seconds", "Projector: cursor lag (slice 4d)"),
]
FIX_EGRESS_GAUGES: List[Tuple[str, str]] = [
    ("oms_fix_egress_lag_seconds", "FIX egress: cursor lag (slice 4d)"),
]
CLUSTER_NODE_GAUGES: List[Tuple[str, str]] = [
    ("oms_cluster_snapshot_age_seconds", "Cluster: snapshot freshness (slice 4h)"),
]

LAG_SENTINEL = -1.0
"""Slice 4d: lag publishers emit -1.0 when no row exists yet (cold start). NOT a real lag."""

P50_QUANTILE = 0.50
P99_QUANTILE = 0.99
NS_PER_S = 1_000_000_000
MS_PER_S = 1_000

# --- Histogram quantile from delta buckets ---

_BUCKET_RE = re.compile(
    r'^(?P<base>oms_[a-z0-9_]+)_seconds_bucket\{(?P<labels>[^}]*)\}\s+(?P<val>[0-9.eE+-]+)\s*$'
)
_LE_RE = re.compile(r'le="([^"]+)"')


def _parse_buckets(text: str) -> Dict[str, Dict[str, float]]:
    """Collapse bucket counts across all label permutations except `le`.

    Returns base -> { le_string -> count }.
    """
    out: Dict[str, Dict[str, float]] = {}
    for line in text.splitlines():
        m = _BUCKET_RE.match(line)
        if not m:
            continue
        base = m.group("base")
        labels = m.group("labels")
        le_match = _LE_RE.search(labels)
        if not le_match:
            continue
        le = le_match.group(1)
        try:
            val = float(m.group("val"))
        except ValueError:
            continue
        bucket = out.setdefault(base, {})
        bucket[le] = bucket.get(le, 0.0) + val
    return out


def _quantile_from_delta_buckets(
    before: Dict[str, float], after: Dict[str, float], q: float
) -> Optional[float]:
    """Return the quantile bound (le) from a Prometheus classic histogram delta.

    Returns the upper-bound `le` (in the timer's base unit, seconds) of the bucket that
    contains the q-th percentile. Returns None if the delta has no observations.
    """
    if not after:
        return None
    delta: List[Tuple[float, float]] = []
    for le, after_count in after.items():
        before_count = before.get(le, 0.0)
        bucket_delta = after_count - before_count
        if bucket_delta < 0:
            bucket_delta = 0.0
        try:
            le_val = math.inf if le == "+Inf" else float(le)
        except ValueError:
            continue
        delta.append((le_val, bucket_delta))
    if not delta:
        return None
    delta.sort()
    total_increment = max(delta[-1][1], 0.0)
    if total_increment <= 0:
        return None
    target = total_increment * q
    for le_val, cum in delta:
        if cum >= target:
            return le_val
    return delta[-1][0]


def _scrape_total(text: str, base: str) -> Optional[Tuple[float, float]]:
    """Returns (sum_seconds, count) for a Prometheus timer, summed across all label permutations."""
    sum_re = re.compile(rf'^{re.escape(base)}_seconds_sum(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)\s*$')
    count_re = re.compile(rf'^{re.escape(base)}_seconds_count(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)\s*$')
    s = 0.0
    c = 0.0
    found = False
    for line in text.splitlines():
        m_s = sum_re.match(line)
        if m_s:
            s += float(m_s.group(1))
            found = True
            continue
        m_c = count_re.match(line)
        if m_c:
            c += float(m_c.group(1))
            found = True
    return (s, c) if found else None


def _scrape_gauge(text: str, base: str) -> Optional[float]:
    """Latest value of a single-metric gauge (sums across labels for parity with timer logic)."""
    gauge_re = re.compile(rf'^{re.escape(base)}(?:\{{[^}}]*\}})?\s+([0-9.eE+-]+)\s*$')
    val: Optional[float] = None
    for line in text.splitlines():
        m = gauge_re.match(line)
        if m:
            try:
                v = float(m.group(1))
            except ValueError:
                continue
            val = v if val is None else val + v
    return val


# --- Output formatting ---


def _fmt_ms_or_dash(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    return f"{seconds * MS_PER_S:.3f} ms"


def _fmt_lag(seconds: Optional[float]) -> str:
    if seconds is None:
        return "— (publisher absent)"
    if seconds == LAG_SENTINEL:
        return f"{seconds:.1f} (cold-start sentinel — publisher reports no rows yet)"
    return f"{seconds:.3f} s"


def _summarise_timers(
    label: str,
    timers: List[Tuple[str, str]],
    before_text: Optional[str],
    after_text: Optional[str],
    indent: str = "  ",
) -> None:
    if before_text is None or after_text is None:
        print(f"{label}: scrape missing (skipped).")
        return
    before_buckets = _parse_buckets(before_text)
    after_buckets = _parse_buckets(after_text)
    print(f"{label}:")
    any_data = False
    for base, description in timers:
        before_b = before_buckets.get(base, {})
        after_b = after_buckets.get(base, {})
        before_total = _scrape_total(before_text, base)
        after_total = _scrape_total(after_text, base)
        if not after_b and after_total is None:
            print(f"{indent}{base}: (not found in scrape — meter not registered yet on this JVM?)")
            continue
        any_data = True
        if before_total is None or after_total is None:
            d_count = None
            d_mean_ms: Optional[float] = None
        else:
            d_sum = max(after_total[0] - before_total[0], 0.0)
            d_count = max(after_total[1] - before_total[1], 0.0)
            d_mean_ms = (d_sum / d_count * MS_PER_S) if d_count > 0 else None
        p50 = _quantile_from_delta_buckets(before_b, after_b, P50_QUANTILE)
        p99 = _quantile_from_delta_buckets(before_b, after_b, P99_QUANTILE)
        count_str = "—" if d_count is None else f"{int(d_count)}"
        mean_str = "—" if d_mean_ms is None else f"{d_mean_ms:.3f} ms"
        print(
            f"{indent}{base}: Δcount={count_str}, mean={mean_str}, "
            f"p50={_fmt_ms_or_dash(p50)}, p99={_fmt_ms_or_dash(p99)}"
        )
        print(f"{indent}  ({description})")
    if not any_data:
        print(f"{indent}(no timer increments observed in this run)")


def _summarise_gauges(
    label: str,
    gauges: List[Tuple[str, str]],
    after_text: Optional[str],
    is_lag: bool = False,
    indent: str = "  ",
) -> Dict[str, Optional[float]]:
    if after_text is None:
        print(f"{label}: scrape missing (skipped).")
        return {}
    print(f"{label} (end-of-run):")
    captured: Dict[str, Optional[float]] = {}
    for base, description in gauges:
        v = _scrape_gauge(after_text, base)
        captured[base] = v
        rendered = _fmt_lag(v) if is_lag else (f"{v:.3f}" if v is not None else "— (not found)")
        print(f"{indent}{base} = {rendered}")
        print(f"{indent}  ({description})")
    return captured


# --- Main ---


def _read_or_none(path: Optional[Path]) -> Optional[str]:
    if path is None:
        return None
    if not path.exists() or path.stat().st_size == 0:
        return None
    return path.read_text()


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Summarise per-role Prometheus scrapes for the OMS Aeron-Cluster topology."
    )
    parser.add_argument("--ingress-before", type=Path)
    parser.add_argument("--ingress-after", type=Path)
    parser.add_argument("--projector-after", type=Path)
    parser.add_argument("--fix-egress-after", type=Path)
    parser.add_argument("--cluster-node-after", type=Path)
    parser.add_argument(
        "--created",
        type=int,
        default=0,
        help="Number of HTTP 201 responses (from the shoot loop) for context.",
    )
    args = parser.parse_args(argv)

    print("=" * 70)
    print("OMS multi-JVM pipeline — per-role Prometheus delta summary")
    print("=" * 70)
    print(f"  HTTP 201 (created) this run: {args.created}")
    print()

    ingress_before = _read_or_none(args.ingress_before)
    ingress_after = _read_or_none(args.ingress_after)
    projector_after = _read_or_none(args.projector_after)
    fix_after = _read_or_none(args.fix_egress_after)
    cluster_after = _read_or_none(args.cluster_node_after)

    _summarise_timers(
        "Ingress-replica timers (Δ between pre/post scrape)",
        INGRESS_TIMERS_MS,
        ingress_before,
        ingress_after,
    )
    print()

    proj_gauges = _summarise_gauges(
        "Projector gauge", PROJECTOR_GAUGES, projector_after, is_lag=True
    )
    print()

    fix_gauges = _summarise_gauges(
        "FIX egress gauge", FIX_EGRESS_GAUGES, fix_after, is_lag=True
    )
    print()

    _summarise_gauges(
        "Cluster-node gauge", CLUSTER_NODE_GAUGES, cluster_after, is_lag=False
    )
    print()

    if ingress_after is not None:
        before_buckets = _parse_buckets(ingress_before or "")
        after_buckets = _parse_buckets(ingress_after)
        crt_p99 = _quantile_from_delta_buckets(
            before_buckets.get("oms_cluster_client_commit_round_trip", {}),
            after_buckets.get("oms_cluster_client_commit_round_trip", {}),
            P99_QUANTILE,
        )
        fix_lag = fix_gauges.get("oms_fix_egress_lag_seconds")
        fix_lag_real = (
            fix_lag if (fix_lag is not None and fix_lag != LAG_SENTINEL) else None
        )
        if crt_p99 is not None and fix_lag_real is not None:
            upper_bound_ms = (crt_p99 + fix_lag_real) * MS_PER_S
            print(
                "Derived (NOT a per-order histogram — an honest upper-bound proxy):"
            )
            print(
                f"  ingress\u2192NOS upper bound \u2248 commit_round_trip_p99 + fix_egress_lag_seconds"
            )
            print(
                f"                            \u2248 {crt_p99 * MS_PER_S:.3f} ms + {fix_lag_real:.3f} s"
                f" = {upper_bound_ms:.1f} ms ({upper_bound_ms / MS_PER_S:.3f} s)"
            )
            print(
                "  (replaces the deleted slice-3b-2 cross-JVM oms.fix.ingress_to_nos histogram;"
            )
            print(
                "   end-of-run lag is a snapshot, not a distribution — re-bench with"
            )
            print(
                "   `clusterBench` for HdrHistogram-grade tail latency on the cluster path)"
            )
        else:
            print(
                "Derived ingress\u2192NOS upper bound: skipped (commit_round_trip and/or fix_egress_lag missing)."
            )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
