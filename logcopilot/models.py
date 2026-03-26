from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class RawEvent:
    source_file: str
    parser_profile: str
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


@dataclass
class Event:
    event_id: str
    source_file: str
    parser_profile: str
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
    is_incident: bool = False


@dataclass
class ClusterSummary:
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
    semantic_cluster_id: int
    signature_hash: str
    hits: int
    representative_text: str
    member_signature_hashes: str = ""
    avg_cosine_similarity: float = 0.0


@dataclass
class AnalysisSummary:
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


@dataclass
class RunArtifact:
    run_id: str
    artifact_name: str
    artifact_type: str
    path: str


@dataclass
class RunResult:
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
