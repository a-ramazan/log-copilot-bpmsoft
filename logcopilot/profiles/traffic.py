from __future__ import annotations

"""Traffic profile: endpoint summaries, latency reports and anomaly detection."""

import csv
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean, quantiles
from typing import Iterable, List, Optional

from ..domain import Event


def _write_markdown(path: Path, lines: List[str]) -> None:
    """Write Markdown content using the module's newline convention."""
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _collect_client_ip_activity(events: List[Event]) -> tuple[defaultdict[str, set[str]], Counter[str]]:
    """Collect request counts and unique-path counts for each client IP."""
    path_by_ip = defaultdict(set)
    hits_by_ip: Counter[str] = Counter()
    for event in events:
        if not event.client_ip:
            continue
        hits_by_ip[event.client_ip] += 1
        if event.path:
            path_by_ip[event.client_ip].add(event.path)
    return path_by_ip, hits_by_ip


def _build_row_anomalies(rows: List[dict]) -> List[dict]:
    """Build anomaly payloads that depend only on aggregated traffic rows."""
    anomalies = []
    for row in rows:
        status = row["http_status"]
        if status is not None and status >= 500 and row["hits"] >= 1:
            anomalies.append(
                {
                    "anomaly_type": "server_errors",
                    "severity": "high" if row["hits"] >= 3 else "medium",
                    "title": f"5xx traffic on {row['method']} {row['path']}",
                    "details": f"{row['hits']} requests returned {status}",
                    "payload": row,
                }
            )
        if (row["p95_latency_ms"] or 0) >= 1000:
            anomalies.append(
                {
                    "anomaly_type": "latency",
                    "severity": "medium",
                    "title": f"Slow endpoint {row['method']} {row['path']}",
                    "details": f"p95 latency is {row['p95_latency_ms']} ms",
                    "payload": row,
                }
            )
    return anomalies


def _build_scan_like_anomalies(
    path_by_ip: defaultdict[str, set[str]],
    hits_by_ip: Counter[str],
) -> List[dict]:
    """Build anomaly payloads for scan-like client IP behavior."""
    anomalies = []
    for client_ip, unique_paths in path_by_ip.items():
        if len(unique_paths) >= 10 or hits_by_ip[client_ip] >= 20:
            anomalies.append(
                {
                    "anomaly_type": "scan_like",
                    "severity": "high",
                    "title": f"Potential scanning from {client_ip}",
                    "details": f"{hits_by_ip[client_ip]} requests across {len(unique_paths)} paths",
                    "payload": {
                        "client_ip": client_ip,
                        "request_count": hits_by_ip[client_ip],
                        "unique_paths": len(unique_paths),
                    },
                }
            )
    return anomalies


def percentile(values: List[float], rank: int) -> Optional[float]:
    """Compute an inclusive percentile for a numeric sample.

    Args:
        values: Numeric sample values.
        rank: Percentile rank from 1 to 100.

    Returns:
        Rounded percentile value, or `None` when samples are absent.
    """
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 3)
    return round(quantiles(values, n=100, method="inclusive")[rank - 1], 3)


def build_traffic_rows(events: Iterable[Event]) -> List[dict]:
    """Aggregate events into traffic summary rows by method, path and status.

    Args:
        events: Canonical events to aggregate.

    Returns:
        Sorted traffic summary rows.
    """
    grouped = defaultdict(list)
    for event in events:
        key = (event.method or "UNKNOWN", event.path or "unknown", event.http_status)
        grouped[key].append(event)

    rows = []
    for (method, path, http_status), bucket_events in grouped.items():
        latencies = [event.latency_ms for event in bucket_events if event.latency_ms is not None]
        sizes = [event.response_size for event in bucket_events if event.response_size is not None]
        unique_ips = {event.client_ip for event in bucket_events if event.client_ip}
        rows.append(
            {
                "method": method,
                "path": path,
                "http_status": http_status,
                "hits": len(bucket_events),
                "unique_ips": len(unique_ips),
                "p95_latency_ms": percentile(latencies, 95),
                "p99_latency_ms": percentile(latencies, 99),
                "avg_response_size": round(mean(sizes), 3) if sizes else None,
            }
        )
    rows.sort(key=lambda item: (item["hits"], item["p95_latency_ms"] or 0), reverse=True)
    return rows


def build_traffic_anomalies(events: List[Event], rows: List[dict]) -> List[dict]:
    """Detect suspicious traffic patterns from events and summary rows.

    Args:
        events: Canonical events for anomaly detection.
        rows: Aggregated traffic rows.

    Returns:
        Derived anomaly payloads.
    """
    path_by_ip, hits_by_ip = _collect_client_ip_activity(events)
    anomalies = _build_row_anomalies(rows)
    anomalies.extend(_build_scan_like_anomalies(path_by_ip, hits_by_ip))
    return anomalies


def write_traffic_summary_csv(path: Path, rows: List[dict]) -> None:
    """Write traffic summary rows to CSV.

    Args:
        path: Destination CSV path.
        rows: Traffic rows to serialize.

    Returns:
        None.
    """
    fieldnames = [
        "method",
        "path",
        "http_status",
        "hits",
        "unique_ips",
        "p95_latency_ms",
        "p99_latency_ms",
        "avg_response_size",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_latency_report_md(path: Path, rows: List[dict]) -> None:
    """Write a Markdown latency report for the slowest endpoints.

    Args:
        path: Destination Markdown path.
        rows: Traffic rows to render.

    Returns:
        None.
    """
    lines = ["# Traffic Latency Report", "", "## Top endpoints by latency", ""]
    if not rows:
        lines.append("No traffic rows were produced.")
    else:
        sorted_rows = sorted(rows, key=lambda item: item["p95_latency_ms"] or 0, reverse=True)
        for index, row in enumerate(sorted_rows[:10], start=1):
            lines.extend(
                [
                    f"### {index}. {row['method']} {row['path']}",
                    f"- hits: {row['hits']}",
                    f"- status: {row['http_status'] if row['http_status'] is not None else 'n/a'}",
                    f"- p95 latency: {row['p95_latency_ms'] if row['p95_latency_ms'] is not None else 'n/a'}",
                    f"- p99 latency: {row['p99_latency_ms'] if row['p99_latency_ms'] is not None else 'n/a'}",
                    "",
                ]
            )
    _write_markdown(path, lines)


def write_suspicious_traffic_md(path: Path, anomalies: List[dict]) -> None:
    """Write a Markdown report for suspicious traffic anomalies.

    Args:
        path: Destination Markdown path.
        anomalies: Traffic anomalies to render.

    Returns:
        None.
    """
    lines = ["# Suspicious Traffic", ""]
    if not anomalies:
        lines.append("No suspicious patterns were detected.")
    else:
        for index, anomaly in enumerate(anomalies, start=1):
            lines.extend(
                [
                    f"## {index}. {anomaly['title']}",
                    f"- type: {anomaly['anomaly_type']}",
                    f"- severity: {anomaly['severity']}",
                    f"- details: {anomaly['details']}",
                    "",
                ]
            )
    _write_markdown(path, lines)


def run_traffic_profile(events: List[Event], output_dir: Path) -> dict:
    """Run the traffic profile and write its artifacts.

    Args:
        events: Canonical events to analyze.
        output_dir: Directory where artifacts should be written.

    Returns:
        Profile payload with rows, anomalies, artifacts and summary metadata.
    """
    rows = build_traffic_rows(events)
    anomalies = build_traffic_anomalies(events, rows)
    summary_path = output_dir / "traffic_summary.csv"
    latency_path = output_dir / "latency_report.md"
    suspicious_path = output_dir / "suspicious_traffic.md"

    write_traffic_summary_csv(summary_path, rows)
    write_latency_report_md(latency_path, rows)
    write_suspicious_traffic_md(suspicious_path, anomalies)

    return {
        "rows": rows,
        "anomalies": anomalies,
        "artifact_paths": {
            "traffic_summary_csv": str(summary_path),
            "latency_report_md": str(latency_path),
            "suspicious_traffic_md": str(suspicious_path),
        },
        "summary": {
            "traffic_row_count": len(rows),
            "anomaly_count": len(anomalies),
        },
    }
