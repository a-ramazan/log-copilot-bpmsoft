from __future__ import annotations

"""Heatmap profile: aggregate per-minute hotspots and operational findings."""

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import quantiles
from typing import Iterable, List
from urllib.parse import urlsplit

from ..domain import Event


_PATH_ID_RE = re.compile(r"/(?:(?:\d+)|(?:[0-9a-fA-F]{8,})|(?:[0-9a-fA-F-]{8,}))(?:/|$)")
_WHITESPACE_RE = re.compile(r"\s+")


def _write_markdown(path: Path, lines: List[str]) -> None:
    """Write Markdown content using the module's newline convention."""
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def normalize_text(value: str | None) -> str:
    """Normalize free-form values used in heatmap dimensions.

    Args:
        value: Raw value to normalize.

    Returns:
        Compact normalized string or `"unknown"`.
    """
    if not value:
        return "unknown"
    normalized = _WHITESPACE_RE.sub(" ", value).strip()
    return normalized[:120] if normalized else "unknown"


def normalize_path(path: str | None) -> str | None:
    """Normalize request paths by masking volatile identifiers.

    Args:
        path: Raw request path or URL.

    Returns:
        Normalized path, or `None` when input is empty.
    """
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
    """Convert a counter into a compact top-N list payload.

    Args:
        counter: Counter to serialize.
        limit: Maximum number of items to return.

    Returns:
        List of `{value, hits}` dictionaries.
    """
    return [{"value": value, "hits": hits} for value, hits in counter.most_common(limit)]


def minute_bucket(timestamp: datetime | None) -> str:
    """Convert a timestamp into a per-minute aggregation bucket.

    Args:
        timestamp: Event timestamp.

    Returns:
        Minute bucket string or `"unknown"`.
    """
    if timestamp is None:
        return "unknown"
    return timestamp.replace(second=0, microsecond=0).isoformat(sep=" ")


def percentile_95(values: List[float]) -> float | None:
    """Compute the inclusive 95th percentile for latency samples.

    Args:
        values: Numeric sample values.

    Returns:
        Rounded 95th percentile, or `None` when samples are absent.
    """
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 3)
    return round(quantiles(values, n=100, method="inclusive")[94], 3)


def derive_operation(event: Event) -> str:
    """Derive an operation label for heatmap aggregation.

    Args:
        event: Canonical event to summarize.

    Returns:
        Operation label used in heatmap rows.
    """
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
    """Aggregate events into heatmap time buckets.

    Args:
        events: Canonical events to aggregate.

    Returns:
        Sorted heatmap metric rows.
    """
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


def _build_ip_bursts(events: Iterable[Event]) -> List[dict]:
    """Aggregate per-minute request bursts for each client IP."""
    per_ip_bucket = defaultdict(int)
    for event in events:
        ip = getattr(event, "client_ip", None)
        if not ip:
            continue
        bucket = minute_bucket(event.timestamp)
        per_ip_bucket[(bucket, ip)] += 1
    return [
        {"bucket_start": bucket, "client_ip": ip, "hits": hits}
        for (bucket, ip), hits in sorted(per_ip_bucket.items(), key=lambda item: item[1], reverse=True)
    ]


def _build_suspicious_ip_bursts(ip_bursts: List[dict], limit: int = 10) -> List[dict]:
    """Return the highest-volume suspicious IP bursts."""
    suspicious = []
    for item in ip_bursts:
        if item["hits"] >= 20:
            suspicious.append({**item, "reason": "high_requests_per_minute"})
        if len(suspicious) >= limit:
            break
    return suspicious


def _build_hottest_buckets(rows: List[dict], limit: int = 10) -> List[dict]:
    """Return the hottest heatmap buckets in compact JSON-ready form."""
    return [
        {
            "bucket_start": row["bucket_start"],
            "component": row["component"],
            "operation": row["operation"],
            "hits": row["hits"],
            "qps": row["qps"],
            "p95_latency_ms": row["p95_latency_ms"],
        }
        for row in rows[:limit]
    ]


def build_heatmap_findings(events: List[Event], rows: List[dict]) -> dict:
    """Build a JSON findings payload for the heatmap profile.

    Args:
        events: Canonical events used by the profile.
        rows: Aggregated heatmap rows.

    Returns:
        Findings payload with counts, bursts and LLM-ready highlights.
    """
    component_counts = Counter(normalize_text(event.component) for event in events)
    operation_counts = Counter(derive_operation(event) for event in events)
    status_counts = Counter(
        str(status)
        for status in (getattr(event, "http_status", None) for event in events)
        if status is not None
    )
    ip_counts = Counter(ip for ip in (getattr(event, "client_ip", None) for event in events) if ip)
    ip_bursts = _build_ip_bursts(events)
    suspicious_ip_bursts = _build_suspicious_ip_bursts(ip_bursts)
    hottest_buckets = _build_hottest_buckets(rows)

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
    """Write heatmap rows to CSV.

    Args:
        path: Destination CSV path.
        rows: Heatmap rows to serialize.

    Returns:
        None.
    """
    fieldnames = ["bucket_start", "component", "operation", "hits", "qps", "p95_latency_ms"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_heatmap_findings_json(path: Path, findings: dict) -> None:
    """Write heatmap findings to JSON.

    Args:
        path: Destination JSON path.
        findings: Findings payload to serialize.

    Returns:
        None.
    """
    path.write_text(json.dumps(findings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_top_hotspots_md(path: Path, rows: List[dict], events: List[Event]) -> None:
    """Write a Markdown summary of the hottest heatmap buckets.

    Args:
        path: Destination Markdown path.
        rows: Heatmap rows to render.
        events: Canonical events for headline counts.

    Returns:
        None.
    """
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
    _write_markdown(path, lines)


def run_heatmap_profile(events: List[Event], output_dir: Path) -> dict:
    """Run the heatmap profile and write its artifacts.

    Args:
        events: Canonical events to analyze.
        output_dir: Directory where artifacts should be written.

    Returns:
        Profile payload with rows, findings, artifacts and summary metadata.
    """
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
