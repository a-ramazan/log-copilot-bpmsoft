from __future__ import annotations

"""Pipeline stage entrypoints for storage writes."""

import logging
from pathlib import Path
import shutil
import time
from typing import List
import uuid

from ..domain import PipelineConfig, PipelineContext, StoreEventsStageResult
from ..text import NormalizationStats
from .sqlite import StorageRepository

STORE_BATCH_SIZE = 1000
logger = logging.getLogger(__name__)

_ARTIFACT_TYPES_BY_NAME = {
    "events_csv": "table",
    "events_parquet": "table",
    "run_summary_json": "summary",
    "manifest_json": "manifest",
}


def ensure_single_log_file(input_path: Path) -> Path:
    """Validate that the run input is a single `.log` file."""
    if input_path.is_file():
        if input_path.suffix.lower() != ".log":
            raise ValueError("MVP accepts a single .log file as input.")
        return input_path
    raise ValueError("MVP accepts exactly one .log file per run.")


def clean_output_dir(output_path: Path) -> None:
    """Remove all files and directories from an existing run output directory."""
    if not output_path.exists():
        return
    for child in output_path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def _resolve_output_paths(out_dir: str | None, run_id: str) -> tuple[Path, Path]:
    """Resolve the base output directory and create the run-specific directory."""
    base_dir = Path(out_dir or "out").expanduser().resolve()
    run_dir = base_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return base_dir, run_dir


def _artifact_type_for_path(artifact_path: str) -> str:
    """Infer the registered artifact type from its file extension."""
    if artifact_path.endswith(".png"):
        return "chart"
    if artifact_path.endswith(".md"):
        return "report"
    if artifact_path.endswith(".json"):
        return "json"
    return "table"


def _current_event_count(context: PipelineContext) -> int:
    """Return the best available event count for lifecycle status updates."""
    if context.event_build_result is not None:
        return context.event_build_result.event_count
    return len(context.events)


def _build_failed_run_summary(context: PipelineContext, error: BaseException) -> dict:
    """Build a compact failure payload for storage lifecycle completion."""
    return {
        "run_id": context.run_id,
        "profile": context.config.profile,
        "status": "failed",
        "input_path": str(context.input_path),
        "output_dir": str(context.run_dir),
        "event_count": _current_event_count(context),
        "error": {
            "type": type(error).__name__,
            "message": str(error),
        },
        "trace_summary": {
            "timings_seconds": {
                name: round(value, 3) for name, value in context.timings.items()
            },
        },
    }


def run_start_run(config: PipelineConfig) -> PipelineContext:
    """
    Подготавливает новый запуск пайплайна.

    Проверяет входной лог-файл, создает run_id, директорию вывода,
    SQLite-репозиторий и начальный контекст для следующих стадий.

    :param config: настройки запуска пайплайна
    :return: начальный контекст пайплайна
    """
    input_path_obj = ensure_single_log_file(config.input_path.resolve()) # проверяем входной лог-файл
    run_id = uuid.uuid4().hex                                            # уникальный id запуска
    base_output_dir, run_dir = _resolve_output_paths(config.out_dir, run_id) # пути для результатов

    if config.clean_out:
        clean_output_dir(run_dir)               # очищаем папку запуска, если это указано в настройках
        run_dir.mkdir(parents=True, exist_ok=True)

    repository = StorageRepository(base_output_dir / "logcopilot.sqlite") # хранилище запусков и результатов
    repository.create_run(run_id, str(input_path_obj), config.profile, str(run_dir)) # запись о запуске

    return PipelineContext(
        config = config,
        input_path=input_path_obj,
        run_id=run_id,
        base_output_dir=base_output_dir,
        run_dir=run_dir,
        repository=repository,
        normalization_stats=NormalizationStats(), # статистика нормализации будет заполняться дальше
    )


def _flush_event_batch(repository: StorageRepository, batch: List) -> float:
    """Persist a pending batch of events and clear the in-memory buffer."""
    if not batch:
        return 0.0
    started = time.perf_counter()
    logger.debug("flush_event_batch: size=%d", len(batch))
    repository.insert_events(batch)
    elapsed = time.perf_counter() - started
    batch.clear()
    return elapsed


def run_store_events(context: PipelineContext) -> PipelineContext:
    """Persist built events for the current pipeline run."""
    if context.event_build_result is None:
        raise RuntimeError("Event building must run before event storage.")

    db_batch: List = []
    store_elapsed = 0.0
    for event in context.events:
        db_batch.append(event)
        if len(db_batch) >= STORE_BATCH_SIZE:
            store_elapsed += _flush_event_batch(context.repository, db_batch)
    store_elapsed += _flush_event_batch(context.repository, db_batch)

    context.store_events_result = StoreEventsStageResult(
        stored_event_count=len(context.events),
        duration_seconds=store_elapsed,
    )
    context.timings["store_events"] = round(store_elapsed, 3)
    logger.info(
        "store_events_finished: run_id=%s events=%d duration=%.3fs",
        context.run_id,
        len(context.events),
        store_elapsed,
    )
    return context


def run_store_aggregates(context: PipelineContext) -> PipelineContext:
    """Persist profile aggregate rows for the current pipeline run."""
    profile_result = context.profile_result
    if profile_result is None:
        raise RuntimeError("Profile computation must run before aggregate storage.")

    profile = profile_result.profile
    payload = profile_result.payload
    started = time.perf_counter()
    aggregate_count = 0
    if profile == "incidents":
        context.repository.insert_incident_clusters(context.run_id, payload["clusters"])
        context.repository.insert_semantic_clusters(context.run_id, payload["semantic_clusters"])
        aggregate_count = len(payload["clusters"]) + len(payload["semantic_clusters"])
    elif profile == "heatmap":
        context.repository.insert_heatmap_metrics(context.run_id, payload["rows"])
        aggregate_count = len(payload["rows"])
    elif profile == "traffic":
        context.repository.insert_traffic_metrics(context.run_id, payload["rows"])
        context.repository.insert_traffic_anomalies(context.run_id, payload["anomalies"])
        aggregate_count = len(payload["rows"]) + len(payload["anomalies"])
    else:
        raise ValueError(f"Unsupported profile: {profile}")

    duration = time.perf_counter() - started
    context.timings["store_aggregates"] = duration
    logger.info(
        "store_aggregates_finished: run_id=%s profile=%s aggregates=%d duration=%.3fs",
        context.run_id,
        profile,
        aggregate_count,
        duration,
    )
    return context


def run_register_artifacts(context: PipelineContext) -> PipelineContext:
    """Register generated artifact metadata for the current pipeline run."""
    if not context.artifact_paths:
        raise RuntimeError("Artifact metadata must be built before artifact registration.")

    for artifact_name, artifact_path in context.artifact_paths.items():
        artifact_type = _ARTIFACT_TYPES_BY_NAME.get(
            artifact_name,
            _artifact_type_for_path(artifact_path),
        )
        context.repository.register_artifact(
            context.run_id,
            artifact_name,
            artifact_type,
            artifact_path,
        )
    logger.info(
        "register_artifacts_finished: run_id=%s artifacts=%d",
        context.run_id,
        len(context.artifact_paths),
    )
    return context


def run_store_agent_result(context: PipelineContext) -> PipelineContext:
    """Persist agent result metadata and register agent-generated artifacts."""
    agent_result = context.agent_result
    if agent_result is None or not agent_result.enabled:
        return context
    if agent_result.status != "completed":
        raise RuntimeError(f"Agent result cannot be stored with status: {agent_result.status}")

    input_context = context.agent_input_context.as_dict() if context.agent_input_context is not None else {}
    context.repository.store_agent_result(context.run_id, agent_result.as_dict(), input_context=input_context)
    for artifact_name, artifact_path in agent_result.artifact_paths.items():
        context.repository.register_artifact(
            context.run_id,
            artifact_name,
            _artifact_type_for_path(artifact_path),
            artifact_path,
        )
    logger.info(
        "store_agent_result_finished: run_id=%s artifacts=%d",
        context.run_id,
        len(agent_result.artifact_paths),
    )
    return context


def run_finalize_run(context: PipelineContext) -> PipelineContext:
    """Mark the current pipeline run as completed in storage."""
    if context.run_summary is None:
        raise RuntimeError("Run summary must be built before run finalization.")

    event_count = _current_event_count(context)
    context.repository.complete_run(
        context.run_id,
        status="completed",
        event_count=event_count,
        summary=context.run_summary,
    )
    logger.info(
        "finalize_run_finished: run_id=%s status=completed events=%d",
        context.run_id,
        event_count,
    )
    return context


def run_fail_run(context: PipelineContext, error: BaseException) -> PipelineContext:
    """Mark the current pipeline run as failed after a critical stage error."""
    failure_summary = _build_failed_run_summary(context, error)
    context.run_summary = failure_summary
    event_count = _current_event_count(context)
    context.repository.complete_run(
        context.run_id,
        status="failed",
        event_count=event_count,
        summary=failure_summary,
    )
    logger.error(
        "fail_run_finished: run_id=%s status=failed error_type=%s",
        context.run_id,
        type(error).__name__,
    )
    return context
