from __future__ import annotations

"""Traffic profile: endpoint summaries, latency reports and anomaly detection."""

from collections import Counter, defaultdict
from statistics import mean, quantiles
from typing import Iterable, List, Optional

from ..domain import Event


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


def run_traffic_profile(events: List[Event], output_dir) -> dict:
    """Compute the traffic profile result.

    Args:
        events: Canonical events to analyze.
        output_dir: Compatibility argument retained for existing callers.

    Returns:
        Profile payload with rows, anomalies and summary metadata.
    """
    del output_dir
    rows = build_traffic_rows(events)
    anomalies = build_traffic_anomalies(events, rows)

    return {
        "rows": rows,
        "anomalies": anomalies,
        "artifact_paths": {},
        "summary": {
            "traffic_row_count": len(rows),
            "anomaly_count": len(anomalies),
        },
    }
