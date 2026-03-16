import argparse
from pathlib import Path
from typing import List
import uuid

from .clustering import ClusterAccumulator, top_incident_clusters
from .models import Event
from .parsing import iter_events
from .reporting import (
    event_to_row,
    open_events_csv_writer,
    write_clusters_csv,
    write_events_parquet,
    write_semantic_clusters_csv,
    write_top_clusters_md,
)
from .semantic import cluster_signatures_semantically
from .signatures import build_signature, make_event_signature


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LogCopilot MVP pipeline")
    parser.add_argument("--input", required=True, help="Path to unzipped logs root")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument(
        "--semantic",
        choices=("off", "auto", "on"),
        default="off",
        help="Enable optional embedding-based semantic clustering",
    )
    parser.add_argument(
        "--semantic-model",
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="Sentence-transformers model name",
    )
    parser.add_argument(
        "--semantic-min-cluster-size",
        type=int,
        default=3,
        help="Minimum cluster size for semantic clustering",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.out).expanduser().resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    events_for_optional_outputs: List[Event] = []
    keep_events = _should_keep_events(args.semantic)
    event_count = 0
    accumulator = ClusterAccumulator()

    with open_events_csv_writer(output_path / "events.csv") as events_writer:
        for raw_event in iter_events(input_path):
            normalized_message, exception_type, stack_frames, is_incident = make_event_signature(
                raw_event
            )
            event = Event(
                event_id=str(uuid.uuid4()),
                source_file=raw_event.source_file,
                timestamp=raw_event.timestamp,
                level=raw_event.level,
                message=raw_event.message,
                stacktrace=raw_event.stacktrace,
                raw_text="",
                normalized_message=normalized_message,
                signature_hash=build_signature(
                    normalized_message, exception_type, stack_frames
                ),
                exception_type=exception_type,
                stack_frames=stack_frames,
                request_id=raw_event.request_id,
                trace_id=raw_event.trace_id,
                http_status=raw_event.http_status,
                is_incident=is_incident,
            )
            events_writer.writerow(event_to_row(event))
            accumulator.add(event)
            event_count += 1
            if keep_events:
                events_for_optional_outputs.append(event)

    clusters = accumulator.build_summaries()
    top_clusters = top_incident_clusters(clusters, limit=10)

    write_clusters_csv(output_path / "clusters.csv", clusters)
    parquet_written = write_events_parquet(
        output_path / "events.parquet", events_for_optional_outputs
    ) if events_for_optional_outputs else False

    semantic_clusters, semantic_note = cluster_signatures_semantically(
        events=events_for_optional_outputs or accumulator.representatives(),
        enabled=args.semantic,
        model_name=args.semantic_model,
        min_cluster_size=args.semantic_min_cluster_size,
    )
    if semantic_clusters:
        write_semantic_clusters_csv(output_path / "semantic_clusters.csv", semantic_clusters)
    write_top_clusters_md(
        output_path / "top_clusters.md",
        top_clusters,
        event_count=event_count,
        cluster_count=len(clusters),
        semantic_note=semantic_note if args.semantic != "off" else None,
    )

    print(f"Input: {input_path}")
    print(f"Events: {event_count}")
    print(f"Signature clusters: {len(clusters)}")
    print(f"Incident-like clusters in top report: {len(top_clusters)}")
    print(f"events.csv: {output_path / 'events.csv'}")
    print(f"clusters.csv: {output_path / 'clusters.csv'}")
    print(f"top_clusters.md: {output_path / 'top_clusters.md'}")
    if parquet_written:
        print(f"events.parquet: {output_path / 'events.parquet'}")
    else:
        print("events.parquet: skipped (pyarrow not installed)")
    if args.semantic != "off":
        if semantic_clusters:
            print(f"semantic_clusters.csv: {output_path / 'semantic_clusters.csv'}")
        else:
            print(f"semantic clustering: {semantic_note}")


def _should_keep_events(semantic_mode: str) -> bool:
    if semantic_mode != "off":
        return True
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


if __name__ == "__main__":
    main()
