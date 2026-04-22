from __future__ import annotations

"""Typed contracts for the current pipeline orchestration path."""

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from .models import Event

if TYPE_CHECKING:
    from logcopilot.parsing.models import CanonicalEvent
    from logcopilot.storage import StorageRepository
    from logcopilot.text import NormalizationStats


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for one pipeline run."""

    input_path: Path
    profile: str = "incidents"
    out_dir: Optional[str] = None
    clean_out: bool = False
    semantic: str = "on"
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    semantic_min_cluster_size: int = 3
    semantic_min_samples: Optional[int] = None
    agent: str = "off"
    agent_question: Optional[str] = None
    agent_provider: str = "none"

    def __post_init__(self) -> None:
        object.__setattr__(self, "input_path", Path(self.input_path).expanduser())
        agent = self.agent.lower()
        agent_provider = self.agent_provider.lower()
        if agent not in {"off", "on"}:
            raise ValueError("agent must be 'off' or 'on'.")
        if agent_provider not in {"none", "yandex"}:
            raise ValueError("agent_provider must be 'none' or 'yandex'.")
        object.__setattr__(self, "agent", agent)
        object.__setattr__(self, "agent_provider", agent_provider)


@dataclass
class ParsedLogRecord:
    """Parsed canonical log record with its pipeline source label."""

    source_file: str
    event: CanonicalEvent


@dataclass
class ParseFileDiagnostics:
    """Parser diagnostics for one source file in a pipeline run."""

    source_file: str
    parser_name: str
    confidence: float
    stats: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        """Return the legacy diagnostics payload shape."""
        return {
            "source_file": self.source_file,
            "parser_name": self.parser_name,
            "confidence": self.confidence,
            "stats": dict(self.stats),
            "warnings": list(self.warnings),
        }


@dataclass
class ParseStageResult:
    """Typed result produced by the parse-only stage."""

    parsed_records: List[ParsedLogRecord] = field(default_factory=list)
    file_results: List[ParseFileDiagnostics] = field(default_factory=list)
    event_count: int = 0
    source_files: List[Path] = field(default_factory=list)
    timings: Dict[str, float] = field(default_factory=dict)


@dataclass
class EventBuildStageResult:
    """Typed result produced by the event-building stage."""

    events: List[Event] = field(default_factory=list)
    event_count: int = 0
    multiline_merges: int = 0
    timings: Dict[str, float] = field(default_factory=dict)


@dataclass
class StoreEventsStageResult:
    """Typed result produced by the event storage stage."""

    stored_event_count: int = 0
    duration_seconds: float = 0.0


@dataclass
class ProfileStageResult:
    """Typed wrapper around the current profile payload dictionary."""

    profile: str
    payload: Dict[str, Any]
    duration_seconds: float

    @property
    def artifact_paths(self) -> Dict[str, str]:
        """Profile artifact paths in the legacy payload shape."""
        return self.payload.setdefault("artifact_paths", {})

    @property
    def summary(self) -> Dict[str, Any]:
        """Profile summary in the legacy payload shape."""
        return self.payload["summary"]


@dataclass
class AgentInputContext:
    """Compact, deterministic facts passed to the optional pipeline agent."""

    profile: str
    run_id: str
    run_summary: Dict[str, Any] = field(default_factory=dict)
    parser_diagnostics: Dict[str, Any] = field(default_factory=dict)
    profile_fit: Dict[str, Any] = field(default_factory=dict)
    facts: Dict[str, Any] = field(default_factory=dict)
    limits: Dict[str, Any] = field(default_factory=dict)
    requested_focus: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready compact input payload."""
        payload = asdict(self)
        if self.requested_focus is None:
            payload.pop("requested_focus", None)
        return payload


@dataclass
class IncidentCard:
    """Structured incident interpretation card for one important cluster."""

    title: str = ""
    severity: str = "medium"
    confidence: float = 0.0
    cluster_id: str = ""
    hits: int = 0
    incident_hits: int = 0
    first_seen: str = ""
    last_seen: str = ""
    exception_type: Optional[str] = None
    summary: str = ""
    evidence: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)
    card_type: str = field(default="incident", init=False)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready incident card."""
        return asdict(self)


@dataclass
class HeatmapCard:
    """Structured heatmap interpretation card for one hotspot."""

    title: str = ""
    severity: str = "medium"
    confidence: float = 0.0
    bucket_start: str = ""
    component: str = ""
    operation: str = ""
    hits: int = 0
    qps: float = 0.0
    p95_latency_ms: Optional[float] = None
    summary: str = ""
    evidence: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)
    card_type: str = field(default="heatmap", init=False)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready heatmap card."""
        return asdict(self)


@dataclass
class TrafficCard:
    """Structured traffic interpretation card for one anomaly or endpoint pattern."""

    title: str = ""
    severity: str = "medium"
    confidence: float = 0.0
    pattern_type: str = ""
    method: str = ""
    path: str = ""
    http_status: Optional[int] = None
    hits: int = 0
    unique_ips: int = 0
    p95_latency_ms: Optional[float] = None
    summary: str = ""
    evidence: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)
    card_type: str = field(default="traffic", init=False)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready traffic card."""
        return asdict(self)


AgentCard = Union[IncidentCard, HeatmapCard, TrafficCard]


@dataclass
class AgentResult:
    """Structured interpretation produced by the optional pipeline agent stage."""

    enabled: bool
    status: str
    profile: str
    overall_status: str = "unknown"
    confidence: float = 0.0
    short_summary: str = ""
    technical_summary: str = ""
    business_summary: str = ""
    key_findings: List[str] = field(default_factory=list)
    recommended_actions: List[str] = field(default_factory=list)
    limitations: List[str] = field(default_factory=list)
    cards: List[AgentCard] = field(default_factory=list)
    provider: str = "none"
    model: str = ""
    artifact_paths: Dict[str, str] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-ready payload for storage and output artifacts."""
        payload: Dict[str, Any] = {
            "enabled": self.enabled,
            "status": self.status,
            "profile": self.profile,
            "overall_status": self.overall_status,
            "confidence": round(float(self.confidence), 3),
            "short_summary": self.short_summary,
            "technical_summary": self.technical_summary,
            "business_summary": self.business_summary,
            "key_findings": list(self.key_findings),
            "recommended_actions": list(self.recommended_actions),
            "limitations": list(self.limitations),
            "cards": [card.as_dict() for card in self.cards],
            "provider": self.provider,
            "model": self.model,
            "artifact_paths": self.artifact_paths,
            "duration_seconds": round(self.duration_seconds, 3),
        }
        if self.error:
            payload["error"] = self.error
        return payload


@dataclass
class PipelineContext:
    """Mutable state passed through the current pipeline stages."""

    config: PipelineConfig
    input_path: Path
    run_id: str
    base_output_dir: Path
    run_dir: Path
    repository: StorageRepository
    normalization_stats: NormalizationStats
    parsed_records: List[ParsedLogRecord] = field(default_factory=list)
    events: List[Event] = field(default_factory=list)
    parse_result: Optional[ParseStageResult] = None
    event_build_result: Optional[EventBuildStageResult] = None
    store_events_result: Optional[StoreEventsStageResult] = None
    profile_result: Optional[ProfileStageResult] = None
    agent_input_context: Optional[AgentInputContext] = None
    agent_result: Optional[AgentResult] = None
    artifact_paths: Dict[str, str] = field(default_factory=dict)
    run_summary: Optional[Dict[str, object]] = None
    manifest: Optional[Dict[str, object]] = None
    timings: Dict[str, float] = field(default_factory=dict)
    parquet_written: bool = False
