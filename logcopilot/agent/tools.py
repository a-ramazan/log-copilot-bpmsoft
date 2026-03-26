from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..storage import StorageRepository


def _repo(db_path: str) -> StorageRepository:
    return StorageRepository(Path(db_path))


def get_run_summary(run_id: str, db_path: str = "out/logcopilot.sqlite") -> Optional[dict]:
    return _repo(db_path).get_run_summary(run_id)


def get_top_incidents(run_id: str, limit: int = 10, db_path: str = "out/logcopilot.sqlite") -> list[dict]:
    return _repo(db_path).get_top_incidents(run_id, limit=limit)


def find_incident_cluster(
    run_id: str,
    cluster_id: str,
    db_path: str = "out/logcopilot.sqlite",
) -> Optional[dict]:
    return _repo(db_path).find_incident_cluster(run_id, cluster_id)


def get_heatmap(run_id: str, limit: int = 50, db_path: str = "out/logcopilot.sqlite") -> list[dict]:
    return _repo(db_path).get_heatmap(run_id, limit=limit)


def get_traffic_summary(
    run_id: str,
    status: Optional[int] = None,
    limit: int = 50,
    db_path: str = "out/logcopilot.sqlite",
) -> list[dict]:
    return _repo(db_path).get_traffic_summary(run_id, status=status, limit=limit)


def get_traffic_anomalies(
    run_id: str,
    limit: int = 20,
    db_path: str = "out/logcopilot.sqlite",
) -> list[dict]:
    return _repo(db_path).get_traffic_anomalies(run_id, limit=limit)


def open_artifact(run_id: str, artifact_name: str, db_path: str = "out/logcopilot.sqlite") -> Optional[str]:
    summary = _repo(db_path).get_run_summary(run_id)
    if summary is None:
        return None
    for artifact in summary["artifacts"]:
        if artifact["artifact_name"] == artifact_name:
            path = Path(artifact["path"])
            if not path.exists():
                return None
            return path.read_text(encoding="utf-8", errors="replace")
    return None
