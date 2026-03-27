from __future__ import annotations

from pathlib import Path
from typing import Optional
from langchain_core.tools import tool

from ..storage import StorageRepository


def _repo(db_path: str) -> StorageRepository:
    return StorageRepository(Path(db_path))


@tool
def get_run_summary(run_id: str, db_path: str = "out/logcopilot.sqlite") -> dict | None:
    """Return summary metadata for a processed log run."""
    return _repo(db_path).get_run_summary(run_id)


@tool
def get_top_incidents(
    run_id: str,
    limit: int = 10,
    db_path: str = "out/logcopilot.sqlite",
) -> list[dict]:
    """Return top incident clusters for a processed log run."""
    return _repo(db_path).get_top_incidents(run_id, limit=limit)


@tool
def find_incident_cluster(
    run_id: str,
    cluster_id: str,
    db_path: str = "out/logcopilot.sqlite",
) -> dict | None:
    """Find one incident cluster by cluster_id in a processed log run."""
    return _repo(db_path).find_incident_cluster(run_id, cluster_id)


@tool
def get_heatmap(
    run_id: str,
    limit: int = 50,
    db_path: str = "out/logcopilot.sqlite",
) -> list[dict]:
    """Return top latency or traffic hotspots from the run heatmap."""
    return _repo(db_path).get_heatmap(run_id, limit=limit)


@tool
def get_traffic_summary(
    run_id: str,
    status: Optional[int] = None,
    limit: int = 50,
    db_path: str = "out/logcopilot.sqlite",
) -> list[dict]:
    """Return traffic summary by endpoint. Optionally filter by HTTP status."""
    return _repo(db_path).get_traffic_summary(run_id, status=status, limit=limit)


@tool
def get_traffic_anomalies(
    run_id: str,
    limit: int = 20,
    db_path: str = "out/logcopilot.sqlite",
) -> list[dict]:
    """Return suspicious traffic anomalies detected in the processed run."""
    return _repo(db_path).get_traffic_anomalies(run_id, limit=limit)