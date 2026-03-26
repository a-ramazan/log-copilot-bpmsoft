from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import quantiles
from typing import Iterable, List

from ..models import Event


def minute_bucket(timestamp: datetime | None) -> str:
    if timestamp is None:
        return "unknown"
    return timestamp.replace(second=0, microsecond=0).isoformat(sep=" ")


def percentile_95(values: List[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 3)
    return round(quantiles(values, n=100, method="inclusive")[94], 3)


def derive_operation(event: Event) -> str:
    if event.path:
        return event.path
    if event.message:
        return event.message.split(" - ")[0][:120]
    return "unknown"


def build_heatmap_rows(events: Iterable[Event]) -> List[dict]:
    grouped = defaultdict(list)
    for event in events:
        key = (minute_bucket(event.timestamp), event.component or "unknown", derive_operation(event))
        grouped[key].append(event)

    rows = []
    for (bucket_start, component, operation), bucket_events in grouped.items():
        latencies = [event.latency_ms for event in bucket_events if event.latency_ms is not None]
        hits = len(bucket_events)
        rows.append(
            {
                "bucket_start": bucket_start,
                "component": component,
                "operation": operation,
                "hits": hits,
                "qps": round(hits / 60.0, 3),
                "p95_latency_ms": percentile_95(latencies),
            }
        )
    rows.sort(key=lambda item: (item["hits"], item["p95_latency_ms"] or 0), reverse=True)
    return rows


def write_heatmap_timeseries_csv(path: Path, rows: List[dict]) -> None:
    fieldnames = ["bucket_start", "component", "operation", "hits", "qps", "p95_latency_ms"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_top_hotspots_md(path: Path, rows: List[dict], events: List[Event]) -> None:
    component_counts = Counter(event.component or "unknown" for event in events)
    operation_counts = Counter(derive_operation(event) for event in events)
    lines = [
        "# Heatmap Hotspots",
        "",
        f"- Events: {len(events)}",
        f"- Components seen: {len(component_counts)}",
        f"- Operations seen: {len(operation_counts)}",
        "",
        "## Hottest buckets",
        "",
    ]
    if not rows:
        lines.append("No buckets were produced.")
    else:
        for index, row in enumerate(rows[:10], start=1):
            latency = "n/a" if row["p95_latency_ms"] is None else f"{row['p95_latency_ms']:.3f} ms"
            lines.extend(
                [
                    f"### {index}. {row['bucket_start']}",
                    f"- component: {row['component']}",
                    f"- operation: {row['operation']}",
                    f"- hits: {row['hits']}",
                    f"- qps: {row['qps']}",
                    f"- p95 latency: {latency}",
                    "",
                ]
            )
    lines.extend(["## Top components", ""])
    for component, hits in component_counts.most_common(10):
        lines.append(f"- {component}: {hits}")
    lines.extend(["", "## Top operations", ""])
    for operation, hits in operation_counts.most_common(10):
        lines.append(f"- {operation}: {hits}")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def run_heatmap_profile(events: List[Event], output_dir: Path) -> dict:
    rows = build_heatmap_rows(events)
    timeseries_path = output_dir / "heatmap_timeseries.csv"
    hotspots_path = output_dir / "top_hotspots.md"
    write_heatmap_timeseries_csv(timeseries_path, rows)
    write_top_hotspots_md(hotspots_path, rows, events)
    return {
        "rows": rows,
        "artifact_paths": {
            "heatmap_timeseries_csv": str(timeseries_path),
            "top_hotspots_md": str(hotspots_path),
        },
        "summary": {
            "bucket_count": len(rows),
            "hottest_bucket": rows[0] if rows else None,
        },
    }
