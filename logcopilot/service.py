from __future__ import annotations

import argparse
from pathlib import Path
import random
import shutil
import time
from typing import Dict, List, Optional
import uuid

from .core import build_event
from .models import AnalysisSummary, PipelineRunResult, RunResult
from .normalization import NormalizationStats
from .parsing import discover_log_files, iter_events
from .profiles import run_heatmap_profile, run_incidents_profile, run_traffic_profile
from .reporting import (
    event_to_row,
    open_events_csv_writer,
    write_events_parquet,
    write_manifest_json,
    write_run_summary_json,
)
from .storage import StorageRepository


def ensure_single_log_file(input_path: Path) -> Path:
    if input_path.is_file():
        if input_path.suffix.lower() != ".log":
            raise ValueError("MVP accepts a single .log file as input.")
        return input_path
    raise ValueError("MVP accepts exactly one .log file per run.")


def clean_output_dir(output_path: Path) -> None:
    if not output_path.exists():
        return
    for child in output_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _update_reservoir_sample(sampled_events: List, sample_size: int, event, seen: int) -> None:
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
    timings: Dict[str, float],
    normalization_stats: NormalizationStats,
) -> dict:
    return {
        "input_path": str(input_path),
        "output_path": str(output_path),
        "source_file_count": source_file_count,
        "events_parsed": event_count,
        "multiline_merges": multiline_merges,
        "timings_seconds": {name: round(value, 3) for name, value in timings.items()},
        "normalization": normalization_stats.snapshot(top_n=15),
    }


def _resolve_output_paths(out_dir: Optional[str], run_id: str) -> tuple[Path, Path]:
    base_dir = Path(out_dir or "out").expanduser().resolve()
    run_dir = base_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return base_dir, run_dir


def run_profile(
    input_path: str,
    profile: str,
    out_dir: Optional[str] = None,
    clean_out: bool = False,
    sample_events: int = 0,
    semantic: str = "on",
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    semantic_min_cluster_size: int = 3,
    semantic_min_samples: Optional[int] = None,
) -> RunResult:
    input_path_obj = ensure_single_log_file(Path(input_path).expanduser().resolve())
    run_id = uuid.uuid4().hex
    base_output_dir, run_dir = _resolve_output_paths(out_dir, run_id)
    if clean_out:
        clean_output_dir(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

    repository = StorageRepository(base_output_dir / "logcopilot.sqlite")
    repository.create_run(run_id, str(input_path_obj), profile, str(run_dir))

    normalization_stats = NormalizationStats()
    sampled_events: List = []
    events = []
    timings: Dict[str, float] = {}
    event_count = 0
    multiline_merges = 0
    source_files = discover_log_files(input_path_obj)

    parse_started = time.perf_counter()
    with open_events_csv_writer(run_dir / "events.csv") as events_writer:
        for raw_event in iter_events(input_path_obj):
            event = build_event(raw_event, run_id=run_id, normalization_stats=normalization_stats)
            events.append(event)
            events_writer.writerow(event_to_row(event))
            event_count += 1
            if event.line_count > 1:
                multiline_merges += 1
            _update_reservoir_sample(sampled_events, sample_events, event, event_count)
    timings["parse_and_write_events"] = time.perf_counter() - parse_started

    storage_started = time.perf_counter()
    repository.insert_events(events)
    timings["store_events"] = time.perf_counter() - storage_started

    profile_started = time.perf_counter()
    if profile == "incidents":
        profile_result = run_incidents_profile(
            events,
            run_dir,
            source_name=input_path_obj.name,
            semantic=semantic,
            semantic_model=semantic_model,
            semantic_min_cluster_size=semantic_min_cluster_size,
            semantic_min_samples=semantic_min_samples,
        )
        repository.insert_incident_clusters(run_id, profile_result["clusters"])
        repository.insert_semantic_clusters(run_id, profile_result["semantic_clusters"])
    elif profile == "heatmap":
        profile_result = run_heatmap_profile(events, run_dir)
        repository.insert_heatmap_metrics(run_id, profile_result["rows"])
    elif profile == "traffic":
        profile_result = run_traffic_profile(events, run_dir)
        repository.insert_traffic_metrics(run_id, profile_result["rows"])
        repository.insert_traffic_anomalies(run_id, profile_result["anomalies"])
    else:
        raise ValueError(f"Unsupported profile: {profile}")
    timings["profile_compute"] = time.perf_counter() - profile_started

    parquet_started = time.perf_counter()
    parquet_written = write_events_parquet(run_dir / "events.parquet", events)
    timings["write_parquet"] = time.perf_counter() - parquet_started

    trace_summary = _build_trace_summary(
        input_path=input_path_obj,
        output_path=run_dir,
        source_file_count=len(source_files),
        event_count=event_count,
        multiline_merges=multiline_merges,
        timings=timings,
        normalization_stats=normalization_stats,
    )

    artifact_paths = {
        "events_csv": str(run_dir / "events.csv"),
        "run_summary_json": str(run_dir / "run_summary.json"),
        "manifest_json": str(run_dir / "manifest.json"),
        **profile_result["artifact_paths"],
    }
    if parquet_written:
        artifact_paths["events_parquet"] = str(run_dir / "events.parquet")

    run_summary = {
        "run_id": run_id,
        "profile": profile,
        "status": "completed",
        "input_path": str(input_path_obj),
        "output_dir": str(run_dir),
        "event_count": event_count,
        "trace_summary": trace_summary,
        "profile_summary": profile_result["summary"],
    }
    manifest = {
        "run_id": run_id,
        "profile": profile,
        "db_path": str(repository.db_path),
        "artifacts": artifact_paths,
    }

    write_run_summary_json(run_dir / "run_summary.json", run_summary)
    write_manifest_json(run_dir / "manifest.json", manifest)

    repository.register_artifact(run_id, "events_csv", "table", artifact_paths["events_csv"])
    repository.register_artifact(run_id, "run_summary_json", "summary", artifact_paths["run_summary_json"])
    repository.register_artifact(run_id, "manifest_json", "manifest", artifact_paths["manifest_json"])
    if parquet_written:
        repository.register_artifact(run_id, "events_parquet", "table", artifact_paths["events_parquet"])
    for artifact_name, artifact_path in profile_result["artifact_paths"].items():
        artifact_type = "report" if artifact_path.endswith(".md") else "table"
        if artifact_path.endswith(".json"):
            artifact_type = "json"
        repository.register_artifact(run_id, artifact_name, artifact_type, artifact_path)

    repository.complete_run(run_id, status="completed", event_count=event_count, summary=run_summary)

    return RunResult(
        run_id=run_id,
        profile=profile,
        status="completed",
        output_dir=str(run_dir),
        db_path=str(repository.db_path),
        event_count=event_count,
        artifact_paths=artifact_paths,
        run_summary=run_summary,
    )


def build_legacy_pipeline_result(run_result: RunResult) -> PipelineRunResult:
    summary = run_result.run_summary["profile_summary"]
    analysis_summary_payload = summary.get("analysis_summary")
    if analysis_summary_payload is None:
        analysis_summary = AnalysisSummary(
            source_name=Path(run_result.run_summary["input_path"]).name,
            event_count=run_result.event_count,
            cluster_count=summary.get("cluster_count", 0),
            incident_event_count=summary.get("incident_event_count", 0),
            timestamp_coverage=0.0,
            level_coverage=0.0,
            component_coverage=0.0,
            exception_coverage=0.0,
            stacktrace_coverage=0.0,
            request_id_coverage=0.0,
            trace_id_coverage=0.0,
            fallback_profile_rate=0.0,
            parser_quality_score=0.0,
            parser_quality_label="n/a",
            parser_profiles="",
        )
    else:
        analysis_summary = AnalysisSummary(**analysis_summary_payload)
    return PipelineRunResult(
        run_id=run_result.run_id,
        profile=run_result.profile,
        status=run_result.status,
        output_dir=run_result.output_dir,
        db_path=run_result.db_path,
        event_count=run_result.event_count,
        cluster_count=summary.get("cluster_count", 0),
        semantic_cluster_count=summary.get("semantic_cluster_count", 0),
        analysis_summary=analysis_summary,
        semantic_note=summary.get("semantic_note", "disabled"),
        artifact_paths=run_result.artifact_paths,
        debug_trace=run_result.run_summary["trace_summary"],
    )


def parse_run_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a LogCopilot processing profile")
    parser.add_argument("run", nargs="?")
    parser.add_argument("--input", required=True, help="Path to a single .log file")
    parser.add_argument("--profile", required=True, choices=("heatmap", "incidents", "traffic"))
    parser.add_argument("--out", default="out", help="Base output directory")
    return parser.parse_args(argv)
