#!/usr/bin/env python3
"""
Delta two Prometheus scrapes for OMS slice-1 pipeline Micrometer Timers (oms.pipeline.*).

Prints mean latency per series for observations recorded between baseline and final scrape.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

# Order for display (Micrometer → Prometheus: dots → underscores, suffix _seconds_*)
PIPELINE_ORDER: list[tuple[str, str]] = [
    ("oms_pipeline_ingress_accept", "Ingress: Postgres accept tx (commit)"),
    ("oms_pipeline_control_outbox_to_chronicle_lag", "Outbox → Chronicle wall (wait + tick)"),
    ("oms_pipeline_control_chronicle_append", "Chronicle append + markAppended (reconciler)"),
    ("oms_pipeline_control_apply", "ControlTailer.apply (risk, buying power, CAS, …)"),
    ("oms_pipeline_fix_outbound_nos", "FIX: build NOS + sendToTarget"),
    ("oms_pipeline_ingress_to_fix_nos", "Total: commit → NOS (Micrometer mirror)"),
]

RE_COUNT = re.compile(
    r"^(?P<base>oms_pipeline_[a-z0-9_]+)_seconds_count(?P<tags>\{[^}]*\})?\s+(?P<val>[0-9.eE+-]+)\s*$",
    re.MULTILINE,
)
RE_SUM = re.compile(
    r"^(?P<base>oms_pipeline_[a-z0-9_]+)_seconds_sum(?P<tags>\{[^}]*\})?\s+(?P<val>[0-9.eE+-]+)\s*$",
    re.MULTILINE,
)


def load_timer_pairs(text: str) -> dict[str, tuple[float, float]]:
    """Map series key (base+tags) -> (count, sum_seconds)."""
    counts: dict[str, float] = {}
    sums: dict[str, float] = {}
    for m in RE_COUNT.finditer(text):
        key = m.group("base") + (m.group("tags") or "")
        counts[key] = float(m.group("val"))
    for m in RE_SUM.finditer(text):
        key = m.group("base") + (m.group("tags") or "")
        sums[key] = float(m.group("val"))
    out: dict[str, tuple[float, float]] = {}
    for key, c in counts.items():
        s = sums.get(key, 0.0)
        out[key] = (c, s)
    return out


def main() -> None:
    if len(sys.argv) != 3:
        print("usage: summarize_micrometer_pipeline_deltas.py <before.prom> <after.prom>", file=sys.stderr)
        sys.exit(2)
    before = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
    after = Path(sys.argv[2]).read_text(encoding="utf-8", errors="replace")
    b = load_timer_pairs(before)
    a = load_timer_pairs(after)
    keys = sorted(set(b) | set(a))

    rows: list[tuple[str, str, float, float, float]] = []
    for key in keys:
        if not any(key.startswith(pfx) for pfx, _ in PIPELINE_ORDER):
            continue
        c0, s0 = b.get(key, (0.0, 0.0))
        c1, s1 = a.get(key, (0.0, 0.0))
        dc = c1 - c0
        ds = s1 - s0
        if dc <= 0:
            continue
        mean_ms = (ds / dc) * 1000.0
        label = next((lbl for pfx, lbl in PIPELINE_ORDER if key.startswith(pfx)), key)
        rows.append((key, label, dc, ds, mean_ms))

    def sort_key(r: tuple[str, str, float, float, float]) -> tuple[int, str]:
        key, _, _, _, _ = r
        for i, (pfx, _) in enumerate(PIPELINE_ORDER):
            if key.startswith(pfx):
                return (i, key)
        return (999, key)

    rows.sort(key=sort_key)

    print("")
    print("======================================================================")
    print("WHERE TIME WENT (this batch) — Micrometer Δ from /actuator/prometheus")
    print("======================================================================")
    if not rows:
        print("  (no oms_pipeline_* timer deltas — scrape failed, or metrics not registered yet)")
        print("  Set SHOOT_PROMETHEUS_URL if OMS is not on 8088, e.g. http://127.0.0.1:8088/actuator/prometheus")
        return
    print(f"  {'mean_ms':>10}  {'Δn':>6}  stage / series")
    for key, label, dc, _ds, mean_ms in rows:
        print(f"  {mean_ms:>10.2f}  {dc:>6.0f}  {label}")
        if len(key) < 120:
            print(f"              {key}")
        else:
            print(f"              {key[:117]}...")
    print("")
    print("  mean_ms = (seconds_sum delta) / (seconds_count delta) for that timer series.")
    print("  Compare rows to see whether lag is in outbox→Chronicle, control.apply, or FIX outbound.")


if __name__ == "__main__":
    main()
