from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from ..models import ClusterSummary, Event, SemanticClusterSummary
from ..reporting import format_timestamp


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class StorageRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    input_path TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    output_dir TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    event_count INTEGER DEFAULT 0,
                    summary_json TEXT DEFAULT '{}'
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    parser_profile TEXT NOT NULL,
                    timestamp TEXT,
                    level TEXT,
                    component TEXT,
                    message TEXT NOT NULL,
                    stacktrace TEXT NOT NULL,
                    raw_text TEXT NOT NULL,
                    line_count INTEGER NOT NULL,
                    normalized_message TEXT NOT NULL,
                    signature_hash TEXT NOT NULL,
                    embedding_text TEXT NOT NULL,
                    exception_type TEXT,
                    stack_frames TEXT DEFAULT '',
                    request_id TEXT,
                    trace_id TEXT,
                    http_status INTEGER,
                    method TEXT,
                    path TEXT,
                    latency_ms REAL,
                    response_size INTEGER,
                    client_ip TEXT,
                    user_agent TEXT,
                    is_incident INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    artifact_name TEXT NOT NULL,
                    artifact_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(run_id, artifact_name),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS incident_clusters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    cluster_id TEXT NOT NULL,
                    hits INTEGER NOT NULL,
                    incident_hits INTEGER NOT NULL,
                    confidence_score REAL NOT NULL,
                    confidence_label TEXT NOT NULL,
                    first_seen TEXT,
                    last_seen TEXT,
                    representative_text TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    UNIQUE(run_id, cluster_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS semantic_clusters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    semantic_cluster_id INTEGER NOT NULL,
                    signature_hash TEXT NOT NULL,
                    hits INTEGER NOT NULL,
                    representative_text TEXT NOT NULL,
                    avg_cosine_similarity REAL NOT NULL,
                    member_signature_hashes TEXT NOT NULL,
                    UNIQUE(run_id, semantic_cluster_id),
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS heatmap_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    bucket_start TEXT NOT NULL,
                    component TEXT,
                    operation TEXT,
                    hits INTEGER NOT NULL,
                    qps REAL NOT NULL,
                    p95_latency_ms REAL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS traffic_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    method TEXT,
                    path TEXT,
                    http_status INTEGER,
                    hits INTEGER NOT NULL,
                    unique_ips INTEGER NOT NULL,
                    p95_latency_ms REAL,
                    p99_latency_ms REAL,
                    avg_response_size REAL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );

                CREATE TABLE IF NOT EXISTS traffic_anomalies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    anomaly_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    title TEXT NOT NULL,
                    details TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES runs(run_id)
                );
                """
            )

    def create_run(self, run_id: str, input_path: str, profile: str, output_dir: str) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs (run_id, input_path, profile, output_dir, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, input_path, profile, output_dir, utc_now(), "running"),
            )

    def complete_run(self, run_id: str, status: str, event_count: int, summary: dict) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET status = ?, completed_at = ?, event_count = ?, summary_json = ?
                WHERE run_id = ?
                """,
                (status, utc_now(), event_count, json.dumps(summary, indent=2), run_id),
            )

    def insert_events(self, events: Iterable[Event]) -> None:
        rows = [
            (
                event.event_id,
                event.run_id,
                event.source_file,
                event.parser_profile,
                format_timestamp(event.timestamp),
                event.level,
                event.component,
                event.message,
                event.stacktrace,
                event.raw_text,
                event.line_count,
                event.normalized_message,
                event.signature_hash,
                event.embedding_text,
                event.exception_type,
                " | ".join(event.stack_frames),
                event.request_id,
                event.trace_id,
                event.http_status,
                event.method,
                event.path,
                event.latency_ms,
                event.response_size,
                event.client_ip,
                event.user_agent,
                int(event.is_incident),
            )
            for event in events
        ]
        if not rows:
            return
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO events (
                    event_id, run_id, source_file, parser_profile, timestamp, level, component,
                    message, stacktrace, raw_text, line_count, normalized_message, signature_hash,
                    embedding_text, exception_type, stack_frames, request_id, trace_id, http_status,
                    method, path, latency_ms, response_size, client_ip, user_agent, is_incident
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def register_artifact(self, run_id: str, artifact_name: str, artifact_type: str, path: str) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO artifacts (run_id, artifact_name, artifact_type, path, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id, artifact_name)
                DO UPDATE SET artifact_type = excluded.artifact_type, path = excluded.path
                """,
                (run_id, artifact_name, artifact_type, path, utc_now()),
            )

    def insert_incident_clusters(self, run_id: str, clusters: Iterable[ClusterSummary]) -> None:
        rows = []
        for cluster in clusters:
            rows.append(
                (
                    run_id,
                    cluster.cluster_id,
                    cluster.hits,
                    cluster.incident_hits,
                    cluster.confidence_score,
                    cluster.confidence_label,
                    format_timestamp(cluster.first_seen),
                    format_timestamp(cluster.last_seen),
                    cluster.representative_signature_text or cluster.representative_normalized,
                    json.dumps(
                        {
                            "parser_profiles": cluster.parser_profiles,
                            "source_files": cluster.source_files,
                            "sample_messages": cluster.sample_messages.split(" || "),
                            "exception_type": cluster.example_exception,
                            "levels": cluster.levels,
                            "top_stack_frames": cluster.top_stack_frames,
                            "representative_raw": cluster.representative_raw,
                            "representative_normalized": cluster.representative_normalized,
                            "representative_signature_text": cluster.representative_signature_text,
                        }
                    ),
                )
            )
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO incident_clusters (
                    run_id, cluster_id, hits, incident_hits, confidence_score, confidence_label,
                    first_seen, last_seen, representative_text, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def insert_semantic_clusters(
        self,
        run_id: str,
        clusters: Iterable[SemanticClusterSummary],
    ) -> None:
        rows = [
            (
                run_id,
                cluster.semantic_cluster_id,
                cluster.signature_hash,
                cluster.hits,
                cluster.representative_text,
                cluster.avg_cosine_similarity,
                cluster.member_signature_hashes,
            )
            for cluster in clusters
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO semantic_clusters (
                    run_id, semantic_cluster_id, signature_hash, hits, representative_text,
                    avg_cosine_similarity, member_signature_hashes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def insert_heatmap_metrics(self, run_id: str, rows: Iterable[dict]) -> None:
        values = [
            (
                run_id,
                row["bucket_start"],
                row.get("component"),
                row.get("operation"),
                row["hits"],
                row["qps"],
                row.get("p95_latency_ms"),
            )
            for row in rows
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO heatmap_metrics (
                    run_id, bucket_start, component, operation, hits, qps, p95_latency_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def insert_traffic_metrics(self, run_id: str, rows: Iterable[dict]) -> None:
        values = [
            (
                run_id,
                row.get("method"),
                row.get("path"),
                row.get("http_status"),
                row["hits"],
                row["unique_ips"],
                row.get("p95_latency_ms"),
                row.get("p99_latency_ms"),
                row.get("avg_response_size"),
            )
            for row in rows
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO traffic_metrics (
                    run_id, method, path, http_status, hits, unique_ips, p95_latency_ms,
                    p99_latency_ms, avg_response_size
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def insert_traffic_anomalies(self, run_id: str, rows: Iterable[dict]) -> None:
        values = [
            (
                run_id,
                row["anomaly_type"],
                row["severity"],
                row["title"],
                row["details"],
                json.dumps(row.get("payload", {}), ensure_ascii=False),
            )
            for row in rows
        ]
        with self.connect() as connection:
            connection.executemany(
                """
                INSERT INTO traffic_anomalies (
                    run_id, anomaly_type, severity, title, details, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                values,
            )

    def list_runs(self, limit: int = 20) -> List[sqlite3.Row]:
        with self.connect() as connection:
            return connection.execute(
                """
                SELECT run_id, input_path, profile, output_dir, created_at, completed_at, status, event_count
                FROM runs
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def get_run_summary(self, run_id: str) -> Optional[dict]:
        with self.connect() as connection:
            run = connection.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run is None:
                return None
            artifacts = connection.execute(
                """
                SELECT artifact_name, artifact_type, path
                FROM artifacts
                WHERE run_id = ?
                ORDER BY artifact_name
                """,
                (run_id,),
            ).fetchall()
        summary = dict(run)
        summary["summary_json"] = json.loads(summary["summary_json"] or "{}")
        summary["artifacts"] = [dict(item) for item in artifacts]
        return summary

    def get_top_incidents(self, run_id: str, limit: int = 10) -> List[dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT cluster_id, hits, incident_hits, confidence_score, confidence_label,
                       first_seen, last_seen, representative_text, payload_json
                FROM incident_clusters
                WHERE run_id = ?
                ORDER BY incident_hits DESC, hits DESC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        payload = []
        for row in rows:
            item = dict(row)
            item["payload_json"] = json.loads(item["payload_json"] or "{}")
            payload.append(item)
        return payload

    def find_incident_cluster(self, run_id: str, cluster_id: str) -> Optional[dict]:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT cluster_id, hits, incident_hits, confidence_score, confidence_label,
                       first_seen, last_seen, representative_text, payload_json
                FROM incident_clusters
                WHERE run_id = ? AND cluster_id = ?
                """,
                (run_id, cluster_id),
            ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["payload_json"] = json.loads(item["payload_json"] or "{}")
        return item

    def get_heatmap(self, run_id: str, limit: int = 100) -> List[dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT bucket_start, component, operation, hits, qps, p95_latency_ms
                FROM heatmap_metrics
                WHERE run_id = ?
                ORDER BY hits DESC, bucket_start DESC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_traffic_summary(
        self,
        run_id: str,
        status: Optional[int] = None,
        limit: int = 100,
    ) -> List[dict]:
        query = """
            SELECT method, path, http_status, hits, unique_ips, p95_latency_ms, p99_latency_ms,
                   avg_response_size
            FROM traffic_metrics
            WHERE run_id = ?
        """
        params: List[object] = [run_id]
        if status is not None:
            query += " AND http_status = ?"
            params.append(status)
        query += " ORDER BY hits DESC, p95_latency_ms DESC LIMIT ?"
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_traffic_anomalies(self, run_id: str, limit: int = 50) -> List[dict]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT anomaly_type, severity, title, details, payload_json
                FROM traffic_anomalies
                WHERE run_id = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (run_id, limit),
            ).fetchall()
        payload = []
        for row in rows:
            item = dict(row)
            item["payload_json"] = json.loads(item["payload_json"] or "{}")
            payload.append(item)
        return payload
