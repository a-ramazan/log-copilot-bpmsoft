from __future__ import annotations

"""SQLite persistence layer for pipeline runs and artifacts."""

from .sqlite import StorageRepository
from .stage import (
    clean_output_dir,
    ensure_single_log_file,
    run_fail_run,
    run_finalize_run,
    run_register_artifacts,
    run_start_run,
    run_store_agent_result,
    run_store_aggregates,
    run_store_events,
)

__all__ = [
    "StorageRepository",
    "clean_output_dir",
    "ensure_single_log_file",
    "run_fail_run",
    "run_finalize_run",
    "run_register_artifacts",
    "run_start_run",
    "run_store_agent_result",
    "run_store_aggregates",
    "run_store_events",
]
