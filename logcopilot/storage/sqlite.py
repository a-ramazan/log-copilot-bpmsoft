from __future__ import annotations

"""SQLite repository for pipeline runs, artifacts and profile aggregates."""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from ..domain import ClusterSummary, Event, SemanticClusterSummary
from ..output import format_timestamp

logger = logging.getLogger(__name__)


def utc_now() -> str:
    """Return the current UTC timestamp in compact ISO-8601 form.

    Returns:
        UTC timestamp string truncated to whole seconds.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class StorageRepository:
    """SQLite repository for pipeline runs, artifacts, and profile aggregates."""

    def __init__(self, db_path: Path) -> None:
        """Initialize the repository and ensure the database schema exists.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        """Open a SQLite connection configured to return rows by column name.

        Returns:
            SQLite connection with `sqlite3.Row` row factory.
        """
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        """Create the database schema and backfill columns required by newer runs."""
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
                    parser_confidence REAL NOT NULL DEFAULT 0,
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
                    attributes_json TEXT DEFAULT '{}',
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
            self._ensure_column(connection, "events", "parser_confidence", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(connection, "events", "attributes_json", "TEXT DEFAULT '{}'")

    def _ensure_column(self, connection: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
        """Add one missing column to an existing table when needed."""
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name not in columns:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")

    def _decode_json_payload_rows(
        self,
        rows: Iterable[sqlite3.Row],
        column_name: str = "payload_json",
    ) -> List[dict]:
        """Convert SQLite rows to dictionaries and decode one JSON payload column."""
        payload = []
        for row in rows:
            item = dict(row)
            item[column_name] = json.loads(item[column_name] or "{}")
            payload.append(item)
        return payload

    def _decode_json_payload_row(
        self,
        row: Optional[sqlite3.Row],
        column_name: str = "payload_json",
    ) -> Optional[dict]:
        """Convert one SQLite row to a dictionary and decode its JSON payload column."""
        if row is None:
            return None
        item = dict(row)
        item[column_name] = json.loads(item[column_name] or "{}")
        return item

    def _dict_rows(self, rows: Iterable[sqlite3.Row]) -> List[dict]:
        """Convert SQLite rows into plain dictionaries."""
        return [dict(row) for row in rows]

    def _fetchall(self, query: str, params: Iterable[object] = ()) -> List[sqlite3.Row]:
        """Execute one query and return all matching rows."""
        with self.connect() as connection:
            return connection.execute(query, tuple(params)).fetchall()

    def _fetchone(self, query: str, params: Iterable[object] = ()) -> Optional[sqlite3.Row]:
        """Execute one query and return the first matching row."""
        with self.connect() as connection:
            return connection.execute(query, tuple(params)).fetchone()

    def _executemany(self, query: str, rows: List[tuple[object, ...]]) -> None:
        """Execute a bulk insert or update for precomputed row tuples."""
        if not rows:
            return
        with self.connect() as connection:
            connection.executemany(query, rows)

    def create_run(self, run_id: str, input_path: str, profile: str, output_dir: str) -> None:
        """Persist the start of a pipeline run.

        Args:
            run_id: Unique run identifier.
            input_path: Source log file path.
            profile: Selected processing profile.
            output_dir: Output directory for run artifacts.

        Returns:
            None.
        """
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs (run_id, input_path, profile, output_dir, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, input_path, profile, output_dir, utc_now(), "running"),
            )
        logger.info(
            "storage_create_run: run_id=%s profile=%s output_dir=%s",
            run_id,
            profile,
            output_dir,
        )

    def complete_run(self, run_id: str, status: str, event_count: int, summary: dict) -> None:
        """Persist the completion status and summary for a run.

        Args:
            run_id: Unique run identifier.
            status: Final run status.
            event_count: Number of parsed events in the run.
            summary: Final run summary payload.

        Returns:
            None.
        """
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE runs
                SET status = ?, completed_at = ?, event_count = ?, summary_json = ?
                WHERE run_id = ?
                """,
                (status, utc_now(), event_count, json.dumps(summary, indent=2), run_id),
            )
        logger.info(
            "storage_complete_run: run_id=%s status=%s event_count=%d",
            run_id,
            status,
            event_count,
        )

    def insert_events(self, events: Iterable[Event]) -> None:
        """Insert a batch of canonical events into storage.

        Args:
            events: Events to persist.

        Returns:
            None.
        """
        rows = [
            (
                event.event_id,
                event.run_id,
                event.source_file,
                event.parser_profile,
                event.parser_confidence,
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
                json.dumps(event.attributes, ensure_ascii=False, sort_keys=True),
                int(event.is_incident),
            )
            for event in events
        ]
        self._executemany(
            """
            INSERT INTO events (
                event_id, run_id, source_file, parser_profile, parser_confidence, timestamp, level, component,
                message, stacktrace, raw_text, line_count, normalized_message, signature_hash,
                embedding_text, exception_type, stack_frames, request_id, trace_id, http_status,
                method, path, latency_ms, response_size, client_ip, user_agent, attributes_json, is_incident
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.debug("storage_insert_events: rows=%d", len(rows))

    def register_artifact(self, run_id: str, artifact_name: str, artifact_type: str, path: str) -> None:
        """Register or update a run artifact entry.

        Args:
            run_id: Unique run identifier.
            artifact_name: Logical artifact name.
            artifact_type: Artifact category used by the UI and storage.
            path: Filesystem path to the artifact.

        Returns:
            None.
        """
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
        logger.info(
            "storage_register_artifact: run_id=%s artifact=%s type=%s path=%s",
            run_id,
            artifact_name,
            artifact_type,
            path,
        )

    def insert_incident_clusters(self, run_id: str, clusters: Iterable[ClusterSummary]) -> None:
        """Insert signature-based incident cluster summaries.

        Args:
            run_id: Unique run identifier.
            clusters: Incident cluster summaries to persist.

        Returns:
            None.
        """
        rows = [
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
            for cluster in clusters
        ]
        self._executemany(
            """
            INSERT INTO incident_clusters (
                run_id, cluster_id, hits, incident_hits, confidence_score, confidence_label,
                first_seen, last_seen, representative_text, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info("storage_insert_incident_clusters: run_id=%s rows=%d", run_id, len(rows))

    def insert_semantic_clusters(
        self,
        run_id: str,
        clusters: Iterable[SemanticClusterSummary],
    ) -> None:
        """Insert semantic cluster summaries for a run.

        Args:
            run_id: Unique run identifier.
            clusters: Semantic cluster summaries to persist.

        Returns:
            None.
        """
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
        self._executemany(
            """
            INSERT INTO semantic_clusters (
                run_id, semantic_cluster_id, signature_hash, hits, representative_text,
                avg_cosine_similarity, member_signature_hashes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        logger.info("storage_insert_semantic_clusters: run_id=%s rows=%d", run_id, len(rows))

    def insert_heatmap_metrics(self, run_id: str, rows: Iterable[dict]) -> None:
        """Insert aggregated heatmap rows for a run.

        Args:
            run_id: Unique run identifier.
            rows: Heatmap metric rows to persist.

        Returns:
            None.
        """
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
        self._executemany(
            """
            INSERT INTO heatmap_metrics (
                run_id, bucket_start, component, operation, hits, qps, p95_latency_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        logger.info("storage_insert_heatmap_metrics: run_id=%s rows=%d", run_id, len(values))

    def insert_traffic_metrics(self, run_id: str, rows: Iterable[dict]) -> None:
        """Insert aggregated traffic summary rows for a run.

        Args:
            run_id: Unique run identifier.
            rows: Traffic metric rows to persist.

        Returns:
            None.
        """
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
        self._executemany(
            """
            INSERT INTO traffic_metrics (
                run_id, method, path, http_status, hits, unique_ips, p95_latency_ms,
                p99_latency_ms, avg_response_size
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        logger.info("storage_insert_traffic_metrics: run_id=%s rows=%d", run_id, len(values))

    def insert_traffic_anomalies(self, run_id: str, rows: Iterable[dict]) -> None:
        """Insert derived traffic anomalies for a run.

        Args:
            run_id: Unique run identifier.
            rows: Traffic anomaly rows to persist.

        Returns:
            None.
        """
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
        self._executemany(
            """
            INSERT INTO traffic_anomalies (
                run_id, anomaly_type, severity, title, details, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        logger.info("storage_insert_traffic_anomalies: run_id=%s rows=%d", run_id, len(values))

    def list_runs(self, limit: int = 20) -> List[sqlite3.Row]:
        """List recent pipeline runs.

        Args:
            limit: Maximum number of runs to return.

        Returns:
            Recent run rows ordered by creation time descending.
        """
        return self._fetchall(
            """
            SELECT run_id, input_path, profile, output_dir, created_at, completed_at, status, event_count
            FROM runs
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )

    def get_run_summary(self, run_id: str) -> Optional[dict]:
        """Fetch run metadata together with its registered artifacts.

        Args:
            run_id: Unique run identifier.

        Returns:
            Run summary dictionary, or `None` when the run does not exist.
        """
        run = self._fetchone("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        if run is None:
            return None
        artifacts = self._fetchall(
            """
            SELECT artifact_name, artifact_type, path
            FROM artifacts
            WHERE run_id = ?
            ORDER BY artifact_name
            """,
            (run_id,),
        )
        summary = dict(run)
        summary["summary_json"] = json.loads(summary["summary_json"] or "{}")
        summary["artifacts"] = self._dict_rows(artifacts)
        return summary

    def get_artifact(self, run_id: str, artifact_name: str) -> Optional[dict]:
        """Fetch metadata for a single artifact registered by a run.

        Args:
            run_id: Unique run identifier.
            artifact_name: Logical artifact name.

        Returns:
            Artifact metadata dictionary, or `None` if not found.
        """
        row = self._fetchone(
            """
            SELECT artifact_name, artifact_type, path
            FROM artifacts
            WHERE run_id = ? AND artifact_name = ?
            """,
            (run_id, artifact_name),
        )
        return dict(row) if row is not None else None

    def get_event_field_stats(self, run_id: str) -> dict:
        """Compute field coverage statistics for persisted events.

        Args:
            run_id: Unique run identifier.

        Returns:
            Aggregate event field statistics for the run.
        """
        row = self._fetchone(
            """
            SELECT
                COUNT(*) AS total_events,
                SUM(CASE WHEN timestamp IS NOT NULL THEN 1 ELSE 0 END) AS timestamp_count,
                SUM(CASE WHEN level IS NOT NULL AND level != '' THEN 1 ELSE 0 END) AS level_count,
                SUM(CASE WHEN component IS NOT NULL AND component != '' THEN 1 ELSE 0 END) AS component_count,
                SUM(CASE WHEN method IS NOT NULL AND method != '' THEN 1 ELSE 0 END) AS method_count,
                SUM(CASE WHEN path IS NOT NULL AND path != '' THEN 1 ELSE 0 END) AS path_count,
                SUM(CASE WHEN http_status IS NOT NULL THEN 1 ELSE 0 END) AS http_status_count,
                SUM(CASE WHEN latency_ms IS NOT NULL THEN 1 ELSE 0 END) AS latency_count,
                SUM(CASE WHEN client_ip IS NOT NULL AND client_ip != '' THEN 1 ELSE 0 END) AS client_ip_count,
                AVG(parser_confidence) AS avg_parser_confidence
            FROM events
            WHERE run_id = ?
            """,
            (run_id,),
        )
        return dict(row) if row is not None else {}

    def get_top_incidents(self, run_id: str, limit: int = 10) -> List[dict]:
        """Return top incident clusters ordered by severity and volume.

        Args:
            run_id: Unique run identifier.
            limit: Maximum number of incident clusters to return.

        Returns:
            Incident cluster payloads with decoded JSON details.
        """
        rows = self._fetchall(
            """
            SELECT cluster_id, hits, incident_hits, confidence_score, confidence_label,
                   first_seen, last_seen, representative_text, payload_json
            FROM incident_clusters
            WHERE run_id = ?
            ORDER BY incident_hits DESC, hits DESC
            LIMIT ?
            """,
            (run_id, limit),
        )
        return self._decode_json_payload_rows(rows)

    def find_incident_cluster(self, run_id: str, cluster_id: str) -> Optional[dict]:
        """Fetch a specific incident cluster by its cluster id.

        Args:
            run_id: Unique run identifier.
            cluster_id: Signature-based cluster identifier.

        Returns:
            Incident cluster payload, or `None` when absent.
        """
        row = self._fetchone(
            """
            SELECT cluster_id, hits, incident_hits, confidence_score, confidence_label,
                   first_seen, last_seen, representative_text, payload_json
            FROM incident_clusters
            WHERE run_id = ? AND cluster_id = ?
            """,
            (run_id, cluster_id),
        )
        return self._decode_json_payload_row(row)

    def get_heatmap(self, run_id: str, limit: Optional[int] = 100) -> List[dict]:
        """Return persisted heatmap rows for a run.

        Args:
            run_id: Unique run identifier.
            limit: Optional maximum number of rows to return.

        Returns:
            Heatmap rows ordered by hottest buckets first.
        """
        query = """
            SELECT bucket_start, component, operation, hits, qps, p95_latency_ms
            FROM heatmap_metrics
            WHERE run_id = ?
            ORDER BY hits DESC, bucket_start DESC
        """
        params: List[object] = [run_id]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._fetchall(query, params)
        return self._dict_rows(rows)

    def get_traffic_summary(
        self,
        run_id: str,
        status: Optional[int] = None,
        limit: int = 100,
    ) -> List[dict]:
        """Return persisted traffic rows for a run.

        Args:
            run_id: Unique run identifier.
            status: Optional HTTP status code filter.
            limit: Maximum number of rows to return.

        Returns:
            Traffic summary rows ordered by frequency and latency.
        """
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
        rows = self._fetchall(query, params)
        return self._dict_rows(rows)

    def get_traffic_anomalies(self, run_id: str, limit: int = 50) -> List[dict]:
        """Return persisted traffic anomalies for a run.

        Args:
            run_id: Unique run identifier.
            limit: Maximum number of anomalies to return.

        Returns:
            Traffic anomaly payloads with decoded JSON details.
        """
        rows = self._fetchall(
            """
            SELECT anomaly_type, severity, title, details, payload_json
            FROM traffic_anomalies
            WHERE run_id = ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (run_id, limit),
        )
        return self._decode_json_payload_rows(rows)
