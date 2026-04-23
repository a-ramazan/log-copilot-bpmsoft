from __future__ import annotations

"""Domain dataclasses for raw events, enriched events, clusters and run results."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class FindingCard:
    """Product-level finding card returned by every completed pipeline run."""

    card_type: str
    title: str
    severity: str
    confidence: float
    summary: str
    evidence: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)
    source_refs: List[Dict[str, Any]] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready finding card payload."""
        return {
            "card_type": self.card_type,
            "title": self.title,
            "severity": self.severity,
            "confidence": round(float(self.confidence), 3),
            "summary": self.summary,
            "evidence": list(self.evidence),
            "recommended_actions": list(self.recommended_actions),
            "limitations": list(self.limitations),
            "source_refs": list(self.source_refs),
            "payload": dict(self.payload),
        }


@dataclass
class ExecutionQuality:
    """Validation result for how trustworthy and useful one pipeline run is."""

    status: str
    score: float
    signals: Dict[str, Any] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready execution quality payload."""
        return {
            "status": self.status,
            "score": round(float(self.score), 3),
            "signals": dict(self.signals),
            "reasons": list(self.reasons),
            "recommendations": list(self.recommendations),
        }


@dataclass
class RunSummary:
    """Product-level summary returned by every completed pipeline run."""

    run_id: str
    profile: str
    status: str
    event_count: int
    quality_status: str
    short_summary: str
    technical_summary: str
    business_summary: str
    parser_diagnostics: Dict[str, Any] = field(default_factory=dict)
    profile_fit: Dict[str, Any] = field(default_factory=dict)
    key_metrics: Dict[str, Any] = field(default_factory=dict)
    key_findings: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)
    quality: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready product summary payload."""
        return {
            "run_id": self.run_id,
            "profile": self.profile,
            "status": self.status,
            "event_count": self.event_count,
            "quality_status": self.quality_status,
            "short_summary": self.short_summary,
            "technical_summary": self.technical_summary,
            "business_summary": self.business_summary,
            "parser_diagnostics": dict(self.parser_diagnostics),
            "profile_fit": dict(self.profile_fit),
            "key_metrics": dict(self.key_metrics),
            "key_findings": list(self.key_findings),
            "recommended_actions": list(self.recommended_actions),
            "limitations": list(self.limitations),
            "quality": dict(self.quality),
        }


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
class RunResult:
    """Modern pipeline run result with product output and final persisted files."""

    run_id: str
    profile: str
    status: str
    output_dir: str
    db_path: str
    event_count: int
    summary: RunSummary
    findings: List[FindingCard]
    quality: ExecutionQuality
    artifact_paths: Dict[str, str]
    run_summary: Dict[str, object]
    agent_result: Optional[Dict[str, Any]] = None
