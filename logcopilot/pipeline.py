import argparse
import logging
from pathlib import Path
import random
import shutil
import time
from typing import Dict, List, Optional
import uuid

from .clustering import ClusterAccumulator, top_incident_clusters
from .models import Event, PipelineRunResult
from .normalization import NormalizationStats
from .parsing import discover_log_files, iter_events
from .quality import AnalysisQualityAccumulator
from .reporting import (
    event_to_row,
    open_events_csv_writer,
    write_analysis_summary_json,
    write_clusters_csv,
    write_debug_samples_md,
    write_events_parquet,
    write_llm_ready_clusters_json,
    write_semantic_clusters_csv,
    write_top_clusters_md,
    write_trace_summary_json,
)
from .semantic import cluster_signatures_semantically
from .signatures import (
    build_embedding_text,
    build_signature,
    make_event_signature,
)

LOGGER = logging.getLogger("logcopilot")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LogCopilot pipeline")
    parser.add_argument("--input", required=True, help="Path to log file or directory")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument(
        "--clean-out",
        action="store_true",
        help="Delete existing output directory contents before running",
    )
    parser.add_argument(
        "--log-level",
        choices=("INFO", "DEBUG"),
        default="INFO",
        help="CLI log verbosity",
    )
    parser.add_argument(
        "--sample-events",
        type=int,
        default=0,
        help="Write N sampled events to debug_samples.md",
    )
    parser.add_argument(
        "--semantic",
        choices=("off", "auto", "on"),
        default="on",
        help="Enable semantic clustering",
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
    parser.add_argument(
        "--semantic-min-samples",
        type=int,
        default=None,
        help="Optional min_samples override for semantic clustering",
    )
    return parser.parse_args()


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def clean_output_dir(output_path: Path) -> None:
    if not output_path.exists():
        return
    for child in output_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _update_reservoir_sample(sampled_events: List[Event], sample_size: int, event: Event, seen: int) -> None:
    if sample_size <= 0:
        return
    if len(sampled_events) < sample_size:
        sampled_events.append(event)
        return
    replacement_index = random.randint(0, seen - 1)
    if replacement_index < sample_size:
        sampled_events[replacement_index] = event


def _build_trace_summary(
    input_path: Path,
    output_path: Path,
    source_file_count: int,
    event_count: int,
    multiline_merges: int,
    clusters_created: int,
    semantic_cluster_count: int,
    timings: Dict[str, float],
    normalization_stats: NormalizationStats,
) -> dict:
    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "source_file_count": source_file_count,
        "events_parsed": event_count,
        "multiline_merges": multiline_merges,
        "clusters_created": clusters_created,
        "semantic_clusters_created": semantic_cluster_count,
        "timings_seconds": {name: round(value, 3) for name, value in timings.items()},
        "normalization": normalization_stats.snapshot(top_n=15),
    }


def run_pipeline(
    input_path: str,
    out_dir: str,
    clean_out: bool = False,
    log_level: str = "INFO",
    sample_events: int = 0,
    semantic: str = "on",
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    semantic_min_cluster_size: int = 3,
    semantic_min_samples: Optional[int] = None,
) -> PipelineRunResult:
    configure_logging(log_level)
    input_path_obj = Path(input_path).expanduser().resolve()
    output_path = Path(out_dir).expanduser().resolve()
    if clean_out:
        clean_output_dir(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    source_files = discover_log_files(input_path_obj)
    LOGGER.info("Starting pipeline for %s source file(s)", len(source_files))

    accumulator = ClusterAccumulator()
    quality = AnalysisQualityAccumulator(source_name=input_path_obj.name)
    normalization_stats = NormalizationStats()
    sampled_events: List[Event] = []
    event_count = 0
    multiline_merges = 0
    semantic_clusters = []
    semantic_note = "disabled"
    timings: Dict[str, float] = {}

    parse_started = time.perf_counter()
    with open_events_csv_writer(output_path / "events.csv") as events_writer:
        for raw_event in iter_events(input_path_obj):
            normalized_message, exception_type, stack_frames, is_incident = make_event_signature(
                raw_event,
                normalization_stats=normalization_stats,
            )
            event = Event(
                event_id=str(uuid.uuid4()),
                source_file=raw_event.source_file,
                parser_profile=raw_event.parser_profile,
                timestamp=raw_event.timestamp,
                level=raw_event.level,
                message=raw_event.message,
                stacktrace=raw_event.stacktrace,
                raw_text=raw_event.raw_text,
                line_count=raw_event.line_count,
                normalized_message=normalized_message,
                signature_hash=build_signature(
                    normalized_message,
                    exception_type,
                    stack_frames,
                ),
                embedding_text="",
                exception_type=exception_type,
                stack_frames=stack_frames,
                component=raw_event.component,
                request_id=raw_event.request_id,
                trace_id=raw_event.trace_id,
                http_status=raw_event.http_status,
                is_incident=is_incident,
            )
            event.embedding_text = build_embedding_text(event)
            events_writer.writerow(event_to_row(event))
            accumulator.add(event)
            quality.add(event)
            event_count += 1
            if event.line_count > 1:
                multiline_merges += 1
            _update_reservoir_sample(sampled_events, sample_events, event, event_count)
    timings["parse_and_write_events"] = time.perf_counter() - parse_started

    cluster_started = time.perf_counter()
    clusters = accumulator.build_summaries()
    top_clusters = top_incident_clusters(clusters, limit=10)
    analysis_summary = quality.build_summary(cluster_count=len(clusters))
    timings["signature_clustering"] = time.perf_counter() - cluster_started

    semantic_started = time.perf_counter()
    if semantic != "off":
        semantic_clusters, semantic_note = cluster_signatures_semantically(
            events=accumulator.representatives(),
            enabled=semantic,
            model_name=semantic_model,
            min_cluster_size=semantic_min_cluster_size,
            min_samples=semantic_min_samples,
        )
        if semantic_clusters:
            write_semantic_clusters_csv(output_path / "semantic_clusters.csv", semantic_clusters)
    timings["semantic_clustering"] = time.perf_counter() - semantic_started

    reporting_started = time.perf_counter()
    write_clusters_csv(output_path / "clusters.csv", clusters)
    write_analysis_summary_json(output_path / "analysis_summary.json", analysis_summary)
    write_llm_ready_clusters_json(output_path / "llm_ready_clusters.json", top_clusters)
    write_top_clusters_md(
        output_path / "top_clusters.md",
        top_clusters,
        event_count=event_count,
        cluster_count=len(clusters),
        analysis_summary=analysis_summary,
        semantic_note=semantic_note if semantic != "off" else None,
    )
    if sample_events > 0:
        write_debug_samples_md(output_path / "debug_samples.md", sampled_events)
    parquet_written = False
    timings["reporting"] = time.perf_counter() - reporting_started

    trace_summary = _build_trace_summary(
        input_path=input_path_obj,
        output_path=output_path,
        source_file_count=len(source_files),
        event_count=event_count,
        multiline_merges=multiline_merges,
        clusters_created=len(clusters),
        semantic_cluster_count=len(semantic_clusters),
        timings=timings,
        normalization_stats=normalization_stats,
    )
    write_trace_summary_json(output_path / "trace_summary.json", trace_summary)

    LOGGER.info("Pipeline finished: %s events, %s clusters", event_count, len(clusters))
    if log_level.upper() == "DEBUG":
        LOGGER.debug("Timings: %s", trace_summary["timings_seconds"])
        LOGGER.debug(
            "Counters: events=%s multiline_merges=%s clusters=%s semantic_clusters=%s",
            event_count,
            multiline_merges,
            len(clusters),
            len(semantic_clusters),
        )
        LOGGER.debug(
            "Normalization mask counts: %s",
            trace_summary["normalization"]["mask_counts"],
        )

    artifact_paths = {
        "events_csv": str(output_path / "events.csv"),
        "clusters_csv": str(output_path / "clusters.csv"),
        "analysis_summary_json": str(output_path / "analysis_summary.json"),
        "llm_ready_clusters_json": str(output_path / "llm_ready_clusters.json"),
        "top_clusters_md": str(output_path / "top_clusters.md"),
        "trace_summary_json": str(output_path / "trace_summary.json"),
    }
    if semantic_clusters:
        artifact_paths["semantic_clusters_csv"] = str(output_path / "semantic_clusters.csv")
    if sample_events > 0:
        artifact_paths["debug_samples_md"] = str(output_path / "debug_samples.md")
    if parquet_written:
        artifact_paths["events_parquet"] = str(output_path / "events.parquet")

    return PipelineRunResult(
        output_dir=str(output_path),
        event_count=event_count,
        cluster_count=len(clusters),
        semantic_cluster_count=len(semantic_clusters),
        analysis_summary=analysis_summary,
        semantic_note=semantic_note,
        artifact_paths=artifact_paths,
        debug_trace=trace_summary,
    )


def main() -> None:
    args = parse_args()
    result = run_pipeline(
        input_path=args.input,
        out_dir=args.out,
        clean_out=args.clean_out,
        log_level=args.log_level,
        sample_events=args.sample_events,
        semantic=args.semantic,
        semantic_model=args.semantic_model,
        semantic_min_cluster_size=args.semantic_min_cluster_size,
        semantic_min_samples=args.semantic_min_samples,
    )
    print(f"Input: {args.input}")
    print(f"Events: {result.event_count}")
    print(f"Signature clusters: {result.cluster_count}")
    print(f"Semantic clusters: {result.semantic_cluster_count}")
    print(
        f"Parser quality: {result.analysis_summary.parser_quality_label} "
        f"({result.analysis_summary.parser_quality_score:.2f})"
    )
    for artifact_name, artifact_path in result.artifact_paths.items():
        print(f"{artifact_name}: {artifact_path}")


if __name__ == "__main__":
    main()
