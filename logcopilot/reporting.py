import csv
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

from .models import ClusterSummary, Event, SemanticClusterSummary


def format_timestamp(value) -> str:
    return value.isoformat(sep=" ") if value else ""


def event_to_row(event: Event) -> dict:
    return {
        "event_id": event.event_id,
        "source_file": event.source_file,
        "timestamp": format_timestamp(event.timestamp),
        "level": event.level or "",
        "message": event.message,
        "stacktrace": event.stacktrace,
        "normalized_message": event.normalized_message,
        "signature_hash": event.signature_hash,
        "request_id": event.request_id or "",
        "trace_id": event.trace_id or "",
        "exception_type": event.exception_type or "",
        "stack_frames": " | ".join(event.stack_frames),
        "http_status": event.http_status or "",
        "is_incident": str(event.is_incident).lower(),
    }


def write_events_csv(path: Path, events: Iterable[Event]) -> None:
    fieldnames = [
        "event_id",
        "source_file",
        "timestamp",
        "level",
        "message",
        "stacktrace",
        "normalized_message",
        "signature_hash",
        "request_id",
        "trace_id",
        "exception_type",
        "stack_frames",
        "http_status",
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
        "source_file",
        "timestamp",
        "level",
        "message",
        "stacktrace",
        "normalized_message",
        "signature_hash",
        "request_id",
        "trace_id",
        "exception_type",
        "stack_frames",
        "http_status",
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
        "source_files",
        "sample_messages",
        "example_exception",
        "levels",
        "incident_hits",
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
                    "source_files": cluster.source_files,
                    "sample_messages": cluster.sample_messages,
                    "example_exception": cluster.example_exception or "",
                    "levels": cluster.levels,
                    "incident_hits": cluster.incident_hits,
                }
            )


def write_top_clusters_md(
    path: Path,
    clusters: List[ClusterSummary],
    event_count: int,
    cluster_count: int,
    semantic_note: Optional[str] = None,
) -> None:
    lines = [
        "# LogCopilot Top Clusters",
        "",
        f"- Events: {event_count}",
        f"- Signature clusters: {cluster_count}",
    ]
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
                "timestamp": format_timestamp(event.timestamp),
                "level": event.level or "",
                "message": event.message,
                "stacktrace": event.stacktrace,
                "normalized_message": event.normalized_message,
                "signature_hash": event.signature_hash,
                "request_id": event.request_id or "",
                "trace_id": event.trace_id or "",
                "exception_type": event.exception_type or "",
                "stack_frames": event.stack_frames,
                "http_status": event.http_status,
                "is_incident": event.is_incident,
            }
        )
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)
    return True
