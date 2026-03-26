import csv
from contextlib import contextmanager
import json
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from .models import AnalysisSummary, ClusterSummary, Event, SemanticClusterSummary


def format_timestamp(value) -> str:
    return value.isoformat(sep=" ") if value else ""


def event_to_row(event: Event) -> dict:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "source_file": event.source_file,
        "parser_profile": event.parser_profile,
        "timestamp": format_timestamp(event.timestamp),
        "level": event.level or "",
        "component": event.component or "",
        "message": event.message,
        "stacktrace": event.stacktrace,
        "raw_text": event.raw_text,
        "line_count": event.line_count,
        "normalized_message": event.normalized_message,
        "signature_hash": event.signature_hash,
        "embedding_text": event.embedding_text,
        "request_id": event.request_id or "",
        "trace_id": event.trace_id or "",
        "exception_type": event.exception_type or "",
        "stack_frames": " | ".join(event.stack_frames),
        "http_status": event.http_status or "",
        "method": event.method or "",
        "path": event.path or "",
        "latency_ms": "" if event.latency_ms is None else event.latency_ms,
        "response_size": "" if event.response_size is None else event.response_size,
        "client_ip": event.client_ip or "",
        "user_agent": event.user_agent or "",
        "is_incident": str(event.is_incident).lower(),
    }


def write_events_csv(path: Path, events: Iterable[Event]) -> None:
    fieldnames = [
        "event_id",
        "run_id",
        "source_file",
        "parser_profile",
        "timestamp",
        "level",
        "component",
        "message",
        "stacktrace",
        "raw_text",
        "line_count",
        "normalized_message",
        "signature_hash",
        "embedding_text",
        "request_id",
        "trace_id",
        "exception_type",
        "stack_frames",
        "http_status",
        "method",
        "path",
        "latency_ms",
        "response_size",
        "client_ip",
        "user_agent",
        "is_incident",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow(event_to_row(event))


@contextmanager
def open_events_csv_writer(path: Path) -> Iterator[csv.DictWriter]:
    fieldnames = [
        "event_id",
        "run_id",
        "source_file",
        "parser_profile",
        "timestamp",
        "level",
        "component",
        "message",
        "stacktrace",
        "raw_text",
        "line_count",
        "normalized_message",
        "signature_hash",
        "embedding_text",
        "request_id",
        "trace_id",
        "exception_type",
        "stack_frames",
        "http_status",
        "method",
        "path",
        "latency_ms",
        "response_size",
        "client_ip",
        "user_agent",
        "is_incident",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        yield writer


def write_clusters_csv(path: Path, clusters: Iterable[ClusterSummary]) -> None:
    fieldnames = [
        "cluster_id",
        "hits",
        "first_seen",
        "last_seen",
        "parser_profiles",
        "source_files",
        "sample_messages",
        "example_exception",
        "exception_type",
        "top_stack_frames",
        "representative_raw",
        "representative_normalized",
        "representative_signature_text",
        "levels",
        "incident_hits",
        "confidence_score",
        "confidence_label",
        "clustering_method",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for cluster in clusters:
            writer.writerow(
                {
                    "cluster_id": cluster.cluster_id,
                    "hits": cluster.hits,
                    "first_seen": format_timestamp(cluster.first_seen),
                    "last_seen": format_timestamp(cluster.last_seen),
                    "parser_profiles": cluster.parser_profiles,
                    "source_files": cluster.source_files,
                    "sample_messages": cluster.sample_messages,
                    "example_exception": cluster.example_exception or "",
                    "exception_type": cluster.example_exception or "",
                    "top_stack_frames": cluster.top_stack_frames,
                    "representative_raw": cluster.representative_raw,
                    "representative_normalized": cluster.representative_normalized,
                    "representative_signature_text": cluster.representative_signature_text,
                    "levels": cluster.levels,
                    "incident_hits": cluster.incident_hits,
                    "confidence_score": cluster.confidence_score,
                    "confidence_label": cluster.confidence_label,
                    "clustering_method": cluster.clustering_method,
                }
            )


def write_top_clusters_md(
    path: Path,
    clusters: List[ClusterSummary],
    event_count: int,
    cluster_count: int,
    analysis_summary: Optional[AnalysisSummary] = None,
    semantic_note: Optional[str] = None,
) -> None:
    lines = [
        "# LogCopilot Top Clusters",
        "",
        f"- Events: {event_count}",
        f"- Signature clusters: {cluster_count}",
    ]
    if analysis_summary:
        lines.extend(
            [
                f"- Parser quality: {analysis_summary.parser_quality_label} ({analysis_summary.parser_quality_score:.2f})",
                f"- Timestamp coverage: {analysis_summary.timestamp_coverage:.1%}",
                f"- Level coverage: {analysis_summary.level_coverage:.1%}",
                f"- Exception coverage: {analysis_summary.exception_coverage:.1%}",
                f"- Stacktrace coverage: {analysis_summary.stacktrace_coverage:.1%}",
                f"- Fallback parser rate: {analysis_summary.fallback_profile_rate:.1%}",
            ]
        )
    if semantic_note:
        lines.append(f"- Semantic clustering: {semantic_note}")
    lines.extend(["", "## Top-10 incidents", ""])

    if not clusters:
        lines.append("No incident-like clusters were found.")
    else:
        for index, cluster in enumerate(clusters[:10], start=1):
            lines.append(f"### {index}. {cluster.cluster_id}")
            lines.append(f"- Hits: {cluster.hits}")
            lines.append(f"- Incident hits: {cluster.incident_hits}")
            lines.append(f"- First seen: {format_timestamp(cluster.first_seen) or 'n/a'}")
            lines.append(f"- Last seen: {format_timestamp(cluster.last_seen) or 'n/a'}")
            lines.append(f"- Confidence: {cluster.confidence_label} ({cluster.confidence_score:.2f})")
            lines.append(f"- Parser profiles: {cluster.parser_profiles or 'n/a'}")
            lines.append(f"- Levels: {cluster.levels or 'n/a'}")
            lines.append(f"- Exception: {cluster.example_exception or 'n/a'}")
            lines.append(f"- Source files: {cluster.source_files or 'n/a'}")
            lines.append("- Sample messages:")
            for sample in cluster.sample_messages.split(" || "):
                lines.append(f"  - {sample}")
            lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_semantic_clusters_csv(
    path: Path, clusters: Iterable[SemanticClusterSummary]
) -> None:
    fieldnames = [
        "semantic_cluster_id",
        "signature_hash",
        "hits",
        "representative_text",
        "member_signature_hashes",
        "avg_cosine_similarity",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for cluster in clusters:
            writer.writerow(
                {
                    "semantic_cluster_id": cluster.semantic_cluster_id,
                    "signature_hash": cluster.signature_hash,
                    "hits": cluster.hits,
                    "representative_text": cluster.representative_text,
                    "member_signature_hashes": cluster.member_signature_hashes,
                    "avg_cosine_similarity": cluster.avg_cosine_similarity,
                }
            )


def write_events_parquet(path: Path, events: List[Event]) -> bool:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return False

    rows = []
    for event in events:
        rows.append(
            {
                "event_id": event.event_id,
                "source_file": event.source_file,
                "parser_profile": event.parser_profile,
                "timestamp": format_timestamp(event.timestamp),
                "level": event.level or "",
                "component": event.component or "",
                "message": event.message,
                "stacktrace": event.stacktrace,
                "raw_text": event.raw_text,
                "line_count": event.line_count,
                "normalized_message": event.normalized_message,
                "signature_hash": event.signature_hash,
                "embedding_text": event.embedding_text,
                "request_id": event.request_id or "",
                "trace_id": event.trace_id or "",
                "exception_type": event.exception_type or "",
                "stack_frames": event.stack_frames,
                "http_status": event.http_status,
                "method": event.method or "",
                "path": event.path or "",
                "latency_ms": event.latency_ms,
                "response_size": event.response_size,
                "client_ip": event.client_ip or "",
                "user_agent": event.user_agent or "",
                "is_incident": event.is_incident,
            }
        )
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)
    return True


def write_analysis_summary_json(path: Path, summary: AnalysisSummary) -> None:
    payload = {
        "source_name": summary.source_name,
        "event_count": summary.event_count,
        "cluster_count": summary.cluster_count,
        "incident_event_count": summary.incident_event_count,
        "timestamp_coverage": summary.timestamp_coverage,
        "level_coverage": summary.level_coverage,
        "component_coverage": summary.component_coverage,
        "exception_coverage": summary.exception_coverage,
        "stacktrace_coverage": summary.stacktrace_coverage,
        "request_id_coverage": summary.request_id_coverage,
        "trace_id_coverage": summary.trace_id_coverage,
        "fallback_profile_rate": summary.fallback_profile_rate,
        "parser_quality_score": summary.parser_quality_score,
        "parser_quality_label": summary.parser_quality_label,
        "parser_profiles": summary.parser_profiles,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_llm_ready_clusters_json(path: Path, clusters: List[ClusterSummary]) -> None:
    payload = []
    for cluster in clusters:
        payload.append(
            {
                "cluster_id": cluster.cluster_id,
                "hits": cluster.hits,
                "incident_hits": cluster.incident_hits,
                "first_seen": format_timestamp(cluster.first_seen),
                "last_seen": format_timestamp(cluster.last_seen),
                "confidence_score": cluster.confidence_score,
                "confidence_label": cluster.confidence_label,
                "parser_profiles": cluster.parser_profiles,
                "levels": cluster.levels,
                "exception_type": cluster.example_exception,
                "top_stack_frames": cluster.top_stack_frames,
                "representative_raw": cluster.representative_raw,
                "representative_normalized": cluster.representative_normalized,
                "representative_signature_text": cluster.representative_signature_text,
                "source_files": cluster.source_files,
                "sample_messages": [
                    sample for sample in cluster.sample_messages.split(" || ") if sample
                ],
            }
        )
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_debug_samples_md(path: Path, events: List[Event]) -> None:
    lines = ["# Debug Samples", ""]
    if not events:
        lines.append("No debug samples were captured.")
    for index, event in enumerate(events, start=1):
        lines.extend(
            [
                f"## Sample {index}",
                "",
                f"- source_file: {event.source_file}",
                f"- parser_profile: {event.parser_profile}",
                f"- cluster_id: {event.signature_hash}",
                f"- level: {event.level or 'n/a'}",
                f"- component: {event.component or 'n/a'}",
                "",
                "### Raw",
                "",
                "```text",
                event.raw_text[:3000],
                "```",
                "",
                "### Normalized",
                "",
                "```text",
                event.normalized_message,
                "```",
                "",
                "### Signature / Embedding Text",
                "",
                "```text",
                event.embedding_text,
                "```",
                "",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_trace_summary_json(path: Path, trace_summary: dict) -> None:
    path.write_text(json.dumps(trace_summary, indent=2), encoding="utf-8")


def write_manifest_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_run_summary_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
