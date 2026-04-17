from __future__ import annotations

import argparse
from collections import Counter
import logging
from pathlib import Path
import shutil
import time
from typing import Dict, List, Optional
import uuid

from .core import build_event
from .models import AnalysisSummary, PipelineRunResult, RunResult
from .normalization import NormalizationStats
from .parsing import canonical_to_raw_event, discover_log_files, parse_file
from .profiles import run_heatmap_profile, run_incidents_profile, run_traffic_profile
from .quality import AnalysisQualityAccumulator, assess_profile_fit
from .reporting import (
    event_to_row,
    open_events_csv_writer,
    write_events_parquet,
    write_manifest_json,
    write_run_summary_json,
)
from .storage import StorageRepository

STORE_BATCH_SIZE = 1000
logger = logging.getLogger(__name__)


def ensure_single_log_file(input_path: Path) -> Path:
    """MVP-контракт: один запуск принимает ровно один .log файл."""
    if input_path.is_file():
        if input_path.suffix.lower() != ".log":
            raise ValueError("MVP accepts a single .log file as input.")
        return input_path
    raise ValueError("MVP accepts exactly one .log file per run.")


def clean_output_dir(output_path: Path) -> None:
    """Очищает директорию запуска перед перезаписью артефактов."""
    if not output_path.exists():
        return
    for child in output_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


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
    """Возвращает базовую и run-директорию и гарантирует их наличие."""
    base_dir = Path(out_dir or "out").expanduser().resolve()
    run_dir = base_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return base_dir, run_dir


def _print_phase(message: str) -> None:
    # Сохраняем текущий консольный формат для CLI + дублируем этап в logging.
    logger.info("run_phase: %s", message)
    print(f"[logcopilot] {message}")


def _flush_event_batch(repository: StorageRepository, batch: List) -> float:
    if not batch:
        return 0.0
    started = time.perf_counter()
    logger.debug("flush_event_batch: size=%d", len(batch))
    repository.insert_events(batch)
    elapsed = time.perf_counter() - started
    batch.clear()
    return elapsed


def _build_parser_diagnostics(file_results: list[dict], events: List, analysis_summary) -> dict:
    parser_counts = Counter(event.parser_profile for event in events)
    total_lines = sum(int(item["stats"].get("total_lines", 0)) for item in file_results)
    total_events = sum(int(item["stats"].get("total_events", 0)) for item in file_results)
    fallback_ratio = (
        sum(float(item["stats"].get("fallback_ratio", 0.0)) * max(int(item["stats"].get("total_events", 0)), 1) for item in file_results)
        / max(total_events, 1)
    )
    warnings = []
    for item in file_results:
        for warning in item.get("warnings", []):
            warnings.append(f"{item['source_file']}: {warning}")
            if len(warnings) >= 8:
                break
        if len(warnings) >= 8:
            break
    return {
        "selected_parsers": dict(parser_counts),
        "dominant_parser": parser_counts.most_common(1)[0][0] if parser_counts else "unknown",
        "mean_parser_confidence": round(getattr(analysis_summary, "mean_parser_confidence", 0.0), 3),
        "total_lines": total_lines,
        "total_events": total_events,
        "fallback_ratio": round(fallback_ratio, 3),
        "parse_quality": {
            "score": round(getattr(analysis_summary, "parse_quality_score", 0.0), 3),
            "label": getattr(analysis_summary, "parse_quality_label", "low"),
        },
        "incident_signal_quality": {
            "score": round(getattr(analysis_summary, "incident_signal_score", 0.0), 3),
            "label": getattr(analysis_summary, "incident_signal_label", "low"),
        },
        "warning_count": sum(len(item.get("warnings", [])) for item in file_results),
        "warnings_sample": warnings,
        "files": [
            {
                "source_file": item["source_file"],
                "parser_name": item["parser_name"],
                "confidence": round(item["confidence"], 3),
                "stats": item["stats"],
            }
            for item in file_results[:10]
        ],
    }


def _build_analysis_summary(events: List, source_name: str, summary: dict) -> AnalysisSummary:
    quality = AnalysisQualityAccumulator(source_name=source_name)
    for event in events:
        quality.add(event)
    cluster_like_count = (
        summary.get("cluster_count")
        or summary.get("bucket_count")
        or summary.get("traffic_row_count")
        or 0
    )
    return quality.build_summary(cluster_count=int(cluster_like_count))


def _start_run(
    input_path: str,
    profile: str,
    out_dir: Optional[str],
    clean_out: bool,
) -> tuple[Path, str, Path, Path, StorageRepository]:
    """
    Инициализирует запуск:
    - валидирует вход;
    - создает run_id и директории;
    - открывает storage и создает запись runs.
    """
    input_path_obj = ensure_single_log_file(Path(input_path).expanduser().resolve())
    run_id = uuid.uuid4().hex
    base_output_dir, run_dir = _resolve_output_paths(out_dir, run_id)
    if clean_out:
        clean_output_dir(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
    repository = StorageRepository(base_output_dir / "logcopilot.sqlite")
    repository.create_run(run_id, str(input_path_obj), profile, str(run_dir))
    return input_path_obj, run_id, base_output_dir, run_dir, repository


def _parse_events_stage(
    input_path_obj: Path,
    profile: str,
    run_id: str,
    run_dir: Path,
    repository: StorageRepository,
    normalization_stats: NormalizationStats,
) -> dict:
    """
    Этап parse + build Event + первичная запись событий.

    Возвращает:
    - events: список канонических Event для profile-этапа;
    - file_results: диагностика parse по каждому файлу;
    - event_count/multiline_merges;
    - timings для parse/store.
    """
    events = []
    db_batch: List = []
    file_results: list[dict] = []
    event_count = 0
    multiline_merges = 0
    source_files = discover_log_files(input_path_obj)
    timings: Dict[str, float] = {}

    parse_started = time.perf_counter()
    store_elapsed = 0.0
    _print_phase(f"parse started: profile={profile} input={input_path_obj.name}")
    with open_events_csv_writer(run_dir / "events.csv") as events_writer:
        for path in source_files:
            source_file = path.name if input_path_obj.is_file() else str(path.relative_to(input_path_obj))
            logger.debug("parse_file_started: run_id=%s source_file=%s", run_id, source_file)
            parse_result = parse_file(path, input_path_obj)
            file_results.append(
                {
                    "source_file": source_file,
                    "parser_name": parse_result.parser_name,
                    "confidence": parse_result.confidence,
                    "stats": dict(parse_result.stats),
                    "warnings": list(parse_result.warnings),
                }
            )
            _print_phase(
                f"parsed {source_file}: parser={parse_result.parser_name} "
                f"confidence={parse_result.confidence:.2f} events={len(parse_result.events)}"
            )
            if parse_result.warnings:
                logger.info(
                    "parse_warnings: run_id=%s source_file=%s warnings=%d",
                    run_id,
                    source_file,
                    len(parse_result.warnings),
                )
            for canonical_event in parse_result.events:
                # Центральный переход из parsing-контракта в core-контракт.
                raw_event = canonical_to_raw_event(canonical_event, source_file=source_file)
                event = build_event(raw_event, run_id=run_id, normalization_stats=normalization_stats)
                events.append(event)
                db_batch.append(event)
                events_writer.writerow(event_to_row(event))
                event_count += 1
                if event.line_count > 1:
                    multiline_merges += 1
                if len(db_batch) >= STORE_BATCH_SIZE:
                    store_elapsed += _flush_event_batch(repository, db_batch)

    timings["parse_and_write_events"] = time.perf_counter() - parse_started
    store_elapsed += _flush_event_batch(repository, db_batch)
    timings["store_events"] = round(store_elapsed, 3)
    _print_phase(
        f"parse finished: files={len(source_files)} events={event_count} "
        f"parse_time={timings['parse_and_write_events']:.3f}s store_time={timings['store_events']:.3f}s"
    )
    return {
        "events": events,
        "file_results": file_results,
        "event_count": event_count,
        "multiline_merges": multiline_merges,
        "source_files": source_files,
        "timings": timings,
    }


def _run_profile_stage(
    profile: str,
    events: List,
    run_dir: Path,
    input_path_obj: Path,
    base_output_dir: Path,
    repository: StorageRepository,
    semantic: str,
    semantic_model: str,
    semantic_min_cluster_size: int,
    semantic_min_samples: Optional[int],
    run_id: str,
) -> tuple[dict, float]:
    """Этап профильных вычислений и записи профильных таблиц в SQLite."""
    profile_started = time.perf_counter()
    _print_phase(f"profile compute started: profile={profile} events={len(events)}")
    if profile == "incidents":
        profile_result = run_incidents_profile(
            events,
            run_dir,
            source_name=input_path_obj.name,
            semantic=semantic,
            semantic_model=semantic_model,
            semantic_min_cluster_size=semantic_min_cluster_size,
            semantic_min_samples=semantic_min_samples,
            semantic_cache_dir=base_output_dir / ".semantic_cache",
            semantic_max_signatures=2500,
            progress_callback=lambda message: _print_phase(message),
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
    duration = time.perf_counter() - profile_started
    _print_phase(f"profile compute finished: profile={profile} duration={duration:.3f}s")
    return profile_result, duration


def _write_parquet_stage(run_dir: Path, events: List, run_id: str) -> tuple[bool, float]:
    """Пишет optional parquet-артефакт и логирует, почему этап был пропущен."""
    parquet_started = time.perf_counter()
    parquet_written = write_events_parquet(run_dir / "events.parquet", events)
    parquet_duration = time.perf_counter() - parquet_started
    if parquet_written:
        logger.info(
            "events_parquet_written: run_id=%s path=%s duration=%.3fs",
            run_id,
            str(run_dir / "events.parquet"),
            parquet_duration,
        )
    else:
        logger.info("events_parquet_skipped: run_id=%s reason=pyarrow_not_installed", run_id)
    return parquet_written, parquet_duration


def _ensure_profile_analysis_summary(events: List, source_name: str, profile_result: dict) -> AnalysisSummary:
    """
    Гарантирует наличие analysis_summary в profile_result.
    Это нужно для единого формата run_summary независимо от профиля.
    """
    analysis_summary_payload = profile_result["summary"].get("analysis_summary")
    if analysis_summary_payload is None:
        analysis_summary = _build_analysis_summary(
            events,
            source_name=source_name,
            summary=profile_result["summary"],
        )
        profile_result["summary"]["analysis_summary"] = {
            "source_name": analysis_summary.source_name,
            "event_count": analysis_summary.event_count,
            "cluster_count": analysis_summary.cluster_count,
            "incident_event_count": analysis_summary.incident_event_count,
            "timestamp_coverage": analysis_summary.timestamp_coverage,
            "level_coverage": analysis_summary.level_coverage,
            "component_coverage": analysis_summary.component_coverage,
            "exception_coverage": analysis_summary.exception_coverage,
            "stacktrace_coverage": analysis_summary.stacktrace_coverage,
            "request_id_coverage": analysis_summary.request_id_coverage,
            "trace_id_coverage": analysis_summary.trace_id_coverage,
            "fallback_profile_rate": analysis_summary.fallback_profile_rate,
            "parser_quality_score": analysis_summary.parser_quality_score,
            "parser_quality_label": analysis_summary.parser_quality_label,
            "parse_quality_score": analysis_summary.parse_quality_score,
            "parse_quality_label": analysis_summary.parse_quality_label,
            "incident_signal_score": analysis_summary.incident_signal_score,
            "incident_signal_label": analysis_summary.incident_signal_label,
            "mean_parser_confidence": analysis_summary.mean_parser_confidence,
            "parser_profiles": analysis_summary.parser_profiles,
        }
        return analysis_summary
    return AnalysisSummary(**analysis_summary_payload)


def _build_artifact_paths(run_dir: Path, profile_result: dict, parquet_written: bool) -> Dict[str, str]:
    """Собирает единый список артефактов запуска."""
    artifact_paths = {
        "events_csv": str(run_dir / "events.csv"),
        "run_summary_json": str(run_dir / "run_summary.json"),
        "manifest_json": str(run_dir / "manifest.json"),
        **profile_result["artifact_paths"],
    }
    if parquet_written:
        artifact_paths["events_parquet"] = str(run_dir / "events.parquet")
    return artifact_paths


def _register_artifacts(
    repository: StorageRepository,
    run_id: str,
    artifact_paths: Dict[str, str],
    profile_result: dict,
    parquet_written: bool,
) -> None:
    """Регистрирует общие и профильные артефакты в таблице artifacts."""
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
    """Главный runtime-проход: parse -> build Event -> profile -> storage/artifacts."""
    del sample_events
    input_path_obj, run_id, base_output_dir, run_dir, repository = _start_run(
        input_path=input_path,
        profile=profile,
        out_dir=out_dir,
        clean_out=clean_out,
    )
    logger.info(
        "run_started: run_id=%s profile=%s input=%s output_dir=%s db_path=%s semantic=%s",
        run_id,
        profile,
        str(input_path_obj),
        str(run_dir),
        str(repository.db_path),
        semantic,
    )

    normalization_stats = NormalizationStats()
    parse_payload = _parse_events_stage(
        input_path_obj = input_path_obj,
        profile = profile,
        run_id = run_id,
        run_dir = run_dir,
        repository = repository,
        normalization_stats=normalization_stats,
    )
    events = parse_payload["events"]
    file_results = parse_payload["file_results"]
    event_count = parse_payload["event_count"]
    multiline_merges = parse_payload["multiline_merges"]
    source_files = parse_payload["source_files"]
    timings: Dict[str, float] = parse_payload["timings"]

    profile_result, profile_duration = _run_profile_stage(
        profile=profile,
        events=events,
        run_dir=run_dir,
        input_path_obj=input_path_obj,
        base_output_dir=base_output_dir,
        repository=repository,
        semantic=semantic,
        semantic_model=semantic_model,
        semantic_min_cluster_size=semantic_min_cluster_size,
        semantic_min_samples=semantic_min_samples,
        run_id=run_id,
    )
    timings["profile_compute"] = profile_duration

    parquet_written, parquet_duration = _write_parquet_stage(run_dir, events, run_id=run_id)
    timings["write_parquet"] = parquet_duration

    trace_summary = _build_trace_summary(
        input_path=input_path_obj,
        output_path=run_dir,
        source_file_count=len(source_files),
        event_count=event_count,
        multiline_merges=multiline_merges,
        timings=timings,
        normalization_stats=normalization_stats,
    )
    analysis_summary = _ensure_profile_analysis_summary(
        events=events,
        source_name=input_path_obj.name,
        profile_result=profile_result,
    )
    parser_diagnostics = _build_parser_diagnostics(file_results, events, analysis_summary)
    profile_fit = assess_profile_fit(events, selected_profile=profile)

    artifact_paths = _build_artifact_paths(
        run_dir=run_dir,
        profile_result=profile_result,
        parquet_written=parquet_written,
    )

    run_summary = {
        "run_id": run_id,
        "profile": profile,
        "status": "completed",
        "input_path": str(input_path_obj),
        "output_dir": str(run_dir),
        "event_count": event_count,
        "trace_summary": trace_summary,
        "parser_diagnostics": parser_diagnostics,
        "profile_fit": profile_fit,
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
    logger.info(
        "run_summary_written: run_id=%s run_summary=%s manifest=%s",
        run_id,
        str(run_dir / "run_summary.json"),
        str(run_dir / "manifest.json"),
    )

    _register_artifacts(
        repository=repository,
        run_id=run_id,
        artifact_paths=artifact_paths,
        profile_result=profile_result,
        parquet_written=parquet_written,
    )

    repository.complete_run(run_id, status="completed", event_count=event_count, summary=run_summary)
    logger.info(
        "run_completed: run_id=%s profile=%s events=%d artifacts=%d",
        run_id,
        profile,
        event_count,
        len(artifact_paths),
    )

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
