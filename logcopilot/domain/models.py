from __future__ import annotations

"""Domain dataclasses for raw events, enriched events, clusters and run results."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class RawEvent:
    """Raw parsed log event before canonical enrichment and signature generation."""

    source_file: str
    parser_profile: str
    parser_confidence: float
    timestamp: Optional[datetime]
    level: Optional[str]
    message: str
    stacktrace: str
    raw_text: str
    line_count: int = 1
    component: Optional[str] = None
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    http_status: Optional[int] = None
    method: Optional[str] = None
    path: Optional[str] = None
    latency_ms: Optional[float] = None
    response_size: Optional[int] = None
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Event:
    """Canonical enriched event used by profiles, reports and storage."""

    event_id: str
    source_file: str
    parser_profile: str
    parser_confidence: float
    timestamp: Optional[datetime]
    level: Optional[str]
    message: str
    stacktrace: str
    raw_text: str
    line_count: int
    normalized_message: str
    signature_hash: str
    embedding_text: str
    run_id: str = ""
    exception_type: Optional[str] = None
    stack_frames: List[str] = field(default_factory=list)
    component: Optional[str] = None
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    http_status: Optional[int] = None
    method: Optional[str] = None
    path: Optional[str] = None
    latency_ms: Optional[float] = None
    response_size: Optional[int] = None
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    is_incident: bool = False


@dataclass
class ClusterSummary:
    """Signature-based cluster summary for grouped incident-like events."""

    cluster_id: str
    hits: int
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    parser_profiles: str
    source_files: str
    sample_messages: str
    example_exception: Optional[str] = None
    levels: str = ""
    incident_hits: int = 0
    confidence_score: float = 0.0
    confidence_label: str = "low"
    clustering_method: str = "signature"
    representative_raw: str = ""
    representative_normalized: str = ""
    representative_signature_text: str = ""
    top_stack_frames: str = ""


@dataclass
class SemanticClusterSummary:
    """Semantic grouping summary built from representative signature events."""

    semantic_cluster_id: int
    signature_hash: str
    hits: int
    representative_text: str
    member_signature_hashes: str = ""
    avg_cosine_similarity: float = 0.0


@dataclass
class AnalysisSummary:
    """Coverage and signal quality metrics for one analyzed source."""

    source_name: str
    event_count: int
    cluster_count: int
    incident_event_count: int
    timestamp_coverage: float
    level_coverage: float
    component_coverage: float
    exception_coverage: float
    stacktrace_coverage: float
    request_id_coverage: float
    trace_id_coverage: float
    fallback_profile_rate: float
    parser_quality_score: float
    parser_quality_label: str
    parser_profiles: str
    parse_quality_score: float = 0.0
    parse_quality_label: str = "low"
    incident_signal_score: float = 0.0
    incident_signal_label: str = "low"
    mean_parser_confidence: float = 0.0


@dataclass
class RunArtifact:
    """Metadata entry describing one artifact produced by a pipeline run."""

    run_id: str
    artifact_name: str
    artifact_type: str
    path: str


@dataclass
class RunResult:
    """Modern pipeline run result with artifact paths and full run summary."""

    run_id: str
    profile: str
    status: str
    output_dir: str
    db_path: str
    event_count: int
    artifact_paths: Dict[str, str]
    run_summary: Dict[str, object]


@dataclass
class PipelineRunResult:
    """Legacy pipeline compatibility result returned by `run_pipeline`."""

    run_id: str
    profile: str
    status: str
    output_dir: str
    db_path: str
    event_count: int
    cluster_count: int
    semantic_cluster_count: int
    analysis_summary: AnalysisSummary
    semantic_note: str
    artifact_paths: Dict[str, str]
    debug_trace: Dict[str, object]
