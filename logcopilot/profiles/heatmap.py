from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import quantiles
from typing import Iterable, List
from urllib.parse import urlsplit

from ..models import Event


_PATH_ID_RE = re.compile(r"/(?:(?:\d+)|(?:[0-9a-fA-F]{8,})|(?:[0-9a-fA-F-]{8,}))(?:/|$)")
_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(value: str | None) -> str:
    if not value:
        return "unknown"
    normalized = _WHITESPACE_RE.sub(" ", value).strip()
    return normalized[:120] if normalized else "unknown"


def normalize_path(path: str | None) -> str | None:
    if not path:
        return None
    raw_path = urlsplit(path).path or path
    raw_path = raw_path.strip()
    if not raw_path:
        return None
    normalized = _PATH_ID_RE.sub("/{id}/", raw_path)
    normalized = re.sub(r"/+", "/", normalized)
    if normalized != "/" and normalized.endswith("/"):
        normalized = normalized[:-1]
    return normalized or "/"


def top_counter_items(counter: Counter, limit: int = 10) -> List[dict]:
    return [{"value": value, "hits": hits} for value, hits in counter.most_common(limit)]


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
    normalized_path = normalize_path(event.path)
    if normalized_path:
        method = normalize_text(event.method).upper() if getattr(event, "method", None) else None
        return f"{method} {normalized_path}" if method else normalized_path
    if event.message:
        message_head = event.message.split(" - ", 1)[0]
        return normalize_text(message_head)
    if event.component:
        return normalize_text(event.component)
    return "unknown"


def build_heatmap_rows(events: Iterable[Event]) -> List[dict]:
    grouped = defaultdict(list)
    for event in events:
        bucket = minute_bucket(event.timestamp)
        component = normalize_text(event.component)
        operation = derive_operation(event)
        if bucket == "unknown" and component == "unknown" and operation == "unknown":
            continue
        key = (bucket, component, operation)
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


def build_heatmap_findings(events: List[Event], rows: List[dict]) -> dict:
    component_counts = Counter(normalize_text(event.component) for event in events)
    operation_counts = Counter(derive_operation(event) for event in events)
    status_counts = Counter(str(status) for status in (getattr(event, "http_status", None) for event in events) if status is not None)
    ip_counts = Counter(ip for ip in (getattr(event, "client_ip", None) for event in events) if ip)

    per_ip_bucket = defaultdict(int)
    for event in events:
        ip = getattr(event, "client_ip", None)
        if not ip:
            continue
        bucket = minute_bucket(event.timestamp)
        per_ip_bucket[(bucket, ip)] += 1

    ip_bursts = [
        {"bucket_start": bucket, "client_ip": ip, "hits": hits}
        for (bucket, ip), hits in sorted(per_ip_bucket.items(), key=lambda item: item[1], reverse=True)
    ]

    suspicious_ip_bursts = []
    for item in ip_bursts:
        if item["hits"] >= 20:
            suspicious_ip_bursts.append({**item, "reason": "high_requests_per_minute"})
        if len(suspicious_ip_bursts) >= 10:
            break

    hottest_buckets = []
    for row in rows[:10]:
        hottest_buckets.append(
            {
                "bucket_start": row["bucket_start"],
                "component": row["component"],
                "operation": row["operation"],
                "hits": row["hits"],
                "qps": row["qps"],
                "p95_latency_ms": row["p95_latency_ms"],
            }
        )

    return {
        "profile": "heatmap",
        "total_events": len(events),
        "bucket_count": len(rows),
        "top_components": top_counter_items(component_counts),
        "top_operations": top_counter_items(operation_counts),
        "top_status_codes": top_counter_items(status_counts),
        "top_client_ips": top_counter_items(ip_counts),
        "top_ip_bursts": ip_bursts[:20],
        "suspicious_ip_bursts": suspicious_ip_bursts,
        "hottest_buckets": hottest_buckets,
        "llm_ready_summary": {
            "peak_load_bucket": hottest_buckets[0] if hottest_buckets else None,
            "most_active_ip": top_counter_items(ip_counts, limit=1)[0] if ip_counts else None,
            "largest_ip_burst": ip_bursts[0] if ip_bursts else None,
            "dominant_component": top_counter_items(component_counts, limit=1)[0] if component_counts else None,
            "dominant_operation": top_counter_items(operation_counts, limit=1)[0] if operation_counts else None,
        },
    }


def write_heatmap_timeseries_csv(path: Path, rows: List[dict]) -> None:
    fieldnames = ["bucket_start", "component", "operation", "hits", "qps", "p95_latency_ms"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_heatmap_findings_json(path: Path, findings: dict) -> None:
    path.write_text(json.dumps(findings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_top_hotspots_md(path: Path, rows: List[dict], events: List[Event]) -> None:
    component_counts = Counter(normalize_text(event.component) for event in events)
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
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = build_heatmap_rows(events)
    findings = build_heatmap_findings(events, rows)

    timeseries_path = output_dir / "heatmap_timeseries.csv"
    hotspots_path = output_dir / "top_hotspots.md"
    findings_path = output_dir / "heatmap_findings.json"

    write_heatmap_timeseries_csv(timeseries_path, rows)
    write_top_hotspots_md(hotspots_path, rows, events)
    write_heatmap_findings_json(findings_path, findings)

    return {
        "rows": rows,
        "findings": findings,
        "artifact_paths": {
            "heatmap_timeseries_csv": str(timeseries_path),
            "top_hotspots_md": str(hotspots_path),
            "heatmap_findings_json": str(findings_path),
        },
        "summary": {
            "bucket_count": len(rows),
            "hottest_bucket": rows[0] if rows else None,
            "llm_ready_summary": findings["llm_ready_summary"],
        },
    }
