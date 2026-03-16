from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class RawEvent:
    source_file: str
    timestamp: Optional[datetime]
    level: Optional[str]
    message: str
    stacktrace: str
    raw_text: str
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    http_status: Optional[int] = None


@dataclass
class Event:
    event_id: str
    source_file: str
    timestamp: Optional[datetime]
    level: Optional[str]
    message: str
    stacktrace: str
    raw_text: str
    normalized_message: str
    signature_hash: str
    exception_type: Optional[str] = None
    stack_frames: List[str] = field(default_factory=list)
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    http_status: Optional[int] = None
    is_incident: bool = False


@dataclass
class ClusterSummary:
    cluster_id: str
    hits: int
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    source_files: str
    sample_messages: str
    example_exception: Optional[str] = None
    levels: str = ""
    incident_hits: int = 0


@dataclass
class SemanticClusterSummary:
    semantic_cluster_id: int
    signature_hash: str
    hits: int
    representative_text: str
