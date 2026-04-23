from __future__ import annotations

"""Pipeline stage entrypoints for common artifact writing."""

import csv
import json
import logging
import re
import time
from pathlib import Path
from typing import List
from urllib.parse import urlsplit

from ..domain import Event, PipelineContext
from .reporting import (
    event_to_row,
    open_events_csv_writer,
    write_analysis_summary_json,
    write_clusters_csv,
    write_events_parquet,
    write_llm_ready_clusters_json,
    write_semantic_clusters_csv,
    write_top_clusters_md,
)

logger = logging.getLogger(__name__)

_PATH_ID_RE = re.compile(r"/(?:(?:\d+)|(?:[0-9a-fA-F]{8,})|(?:[0-9a-fA-F-]{8,}))(?:/|$)")
_WHITESPACE_RE = re.compile(r"\s+")


def run_write_events_csv(context: PipelineContext) -> PipelineContext:
    """Keep the legacy stage boundary without persisting events CSV by default."""
    if context.event_build_result is None:
        raise RuntimeError("Event building must run before events CSV writing.")
    context.timings["write_events_csv"] = 0.0
    logger.info("events_csv_skipped: run_id=%s mode=product_output_only", context.run_id)
    return context


def _write_markdown(path: Path, lines: List[str]) -> None:
    """Write Markdown content using the project's newline convention."""
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _normalize_heatmap_text(value: str | None) -> str:
    """Normalize free-form values used in heatmap artifact rendering."""
    if not value:
        return "unknown"
    normalized = _WHITESPACE_RE.sub(" ", value).strip()
    return normalized[:120] if normalized else "unknown"


def _normalize_heatmap_path(path: str | None) -> str | None:
    """Normalize request paths by masking volatile identifiers."""
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


def _derive_heatmap_operation(event: Event) -> str:
    """Derive an operation label for heatmap artifact rendering."""
    normalized_path = _normalize_heatmap_path(event.path)
    if normalized_path:
        method = _normalize_heatmap_text(event.method).upper() if getattr(event, "method", None) else None
        return f"{method} {normalized_path}" if method else normalized_path
    if event.message:
        message_head = event.message.split(" - ", 1)[0]
        return _normalize_heatmap_text(message_head)
    if event.component:
        return _normalize_heatmap_text(event.component)
    return "unknown"


def _write_heatmap_timeseries_csv(path: Path, rows: List[dict]) -> None:
    """Write heatmap rows to CSV."""
    fieldnames = ["bucket_start", "component", "operation", "hits", "qps", "p95_latency_ms"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_heatmap_findings_json(path: Path, findings: dict) -> None:
    """Write heatmap findings to JSON."""
    path.write_text(json.dumps(findings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_top_hotspots_md(path: Path, rows: List[dict], events: List[Event], findings: dict) -> None:
    """Write a Markdown summary of the hottest heatmap buckets."""
    component_count = len({_normalize_heatmap_text(event.component) for event in events})
    operation_count = len({_derive_heatmap_operation(event) for event in events})
    top_components = findings.get("top_components", [])
    top_operations = findings.get("top_operations", [])
    lines = [
        "# Heatmap Hotspots",
        "",
        f"- Events: {len(events)}",
        f"- Components seen: {component_count}",
        f"- Operations seen: {operation_count}",
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
    for item in top_components:
        lines.append(f"- {item['value']}: {item['hits']}")
    lines.extend(["", "## Top operations", ""])
    for item in top_operations:
        lines.append(f"- {item['value']}: {item['hits']}")
    _write_markdown(path, lines)


def _write_traffic_summary_csv(path: Path, rows: List[dict]) -> None:
    """Write traffic summary rows to CSV."""
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


def _write_latency_report_md(path: Path, rows: List[dict]) -> None:
    """Write a Markdown latency report for the slowest endpoints."""
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


def _write_suspicious_traffic_md(path: Path, anomalies: List[dict]) -> None:
    """Write a Markdown report for suspicious traffic anomalies."""
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


def _write_incidents_artifacts(context: PipelineContext, payload: dict) -> dict[str, str]:
    """Write incidents profile artifacts and return their paths."""
    clusters_path = context.run_dir / "clusters.csv"
    semantic_path = context.run_dir / "semantic_clusters.csv"
    top_incidents_path = context.run_dir / "top_incidents.md"
    llm_path = context.run_dir / "llm_ready_clusters.json"
    analysis_path = context.run_dir / "analysis_summary.json"

    write_clusters_csv(clusters_path, payload["clusters"])
    write_semantic_clusters_csv(semantic_path, payload["semantic_clusters"])
    write_llm_ready_clusters_json(llm_path, payload["top_clusters"])
    write_top_clusters_md(
        top_incidents_path,
        payload["top_clusters"],
        event_count=len(context.events),
        cluster_count=len(payload["clusters"]),
        analysis_summary=payload["analysis_summary"],
        semantic_note=payload["semantic_note"],
    )
    write_analysis_summary_json(analysis_path, payload["analysis_summary"])
    return {
        "clusters_csv": str(clusters_path),
        "semantic_clusters_csv": str(semantic_path),
        "top_incidents_md": str(top_incidents_path),
        "llm_ready_clusters_json": str(llm_path),
        "analysis_summary_json": str(analysis_path),
    }


def _write_heatmap_artifacts(context: PipelineContext, payload: dict) -> dict[str, str]:
    """Write heatmap profile artifacts and return their paths."""
    timeseries_path = context.run_dir / "heatmap_timeseries.csv"
    hotspots_path = context.run_dir / "top_hotspots.md"
    findings_path = context.run_dir / "heatmap_findings.json"

    _write_heatmap_timeseries_csv(timeseries_path, payload["rows"])
    _write_top_hotspots_md(hotspots_path, payload["rows"], context.events, payload["findings"])
    _write_heatmap_findings_json(findings_path, payload["findings"])
    return {
        "heatmap_timeseries_csv": str(timeseries_path),
        "top_hotspots_md": str(hotspots_path),
        "heatmap_findings_json": str(findings_path),
    }


def _write_traffic_artifacts(context: PipelineContext, payload: dict) -> dict[str, str]:
    """Write traffic profile artifacts and return their paths."""
    summary_path = context.run_dir / "traffic_summary.csv"
    latency_path = context.run_dir / "latency_report.md"
    suspicious_path = context.run_dir / "suspicious_traffic.md"

    _write_traffic_summary_csv(summary_path, payload["rows"])
    _write_latency_report_md(latency_path, payload["rows"])
    _write_suspicious_traffic_md(suspicious_path, payload["anomalies"])
    return {
        "traffic_summary_csv": str(summary_path),
        "latency_report_md": str(latency_path),
        "suspicious_traffic_md": str(suspicious_path),
    }


def run_artifact_generation(context: PipelineContext) -> PipelineContext:
    """Keep the legacy stage boundary without persisting profile debug artifacts by default."""
    profile_result = context.profile_result
    if profile_result is None:
        raise RuntimeError("Profile computation must run before artifact generation.")

    profile_result.payload["artifact_paths"] = {}
    context.timings["write_profile_artifacts"] = 0.0
    context.parquet_written = False
    context.timings["write_parquet"] = 0.0
    logger.info(
        "profile_artifacts_skipped: run_id=%s profile=%s mode=product_output_only",
        context.run_id,
        profile_result.profile,
    )
    return context
