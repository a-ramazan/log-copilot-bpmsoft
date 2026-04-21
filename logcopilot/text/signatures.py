from __future__ import annotations

"""Signature and embedding text builders for log events."""

import hashlib
import re
from typing import List, Optional

from ..domain import Event, RawEvent
from .normalization import NormalizationStats, normalize_text

EXCEPTION_RE = re.compile(r"\b([A-Za-z_][\w.]+(?:Exception|Error))\b")
STACK_FRAME_RE = re.compile(r"^\s*at\s+(.+?)(?:\(|\s+in\s+|$)")
INCIDENT_KEYWORD_RE = re.compile(
    r"\b(exception|error|failed|failure|fatal|timeout|timed out|refused|denied|unavailable|panic|crash|terminated|killed)\b",
    re.IGNORECASE,
)


def extract_exception_type(*chunks: str) -> Optional[str]:
    """Extract an exception or error type name from text fragments.

    Args:
        *chunks: Candidate text chunks such as message and stacktrace.

    Returns:
        First matched exception type, or `None`.
    """
    for chunk in chunks:
        if not chunk:
            continue
        match = EXCEPTION_RE.search(chunk)
        if match:
            return match.group(1)
    return None


def extract_stack_frames(stacktrace: str, top_n: int = 3) -> List[str]:
    """Extract the top stack frames from a stacktrace string.

    Args:
        stacktrace: Raw stacktrace text.
        top_n: Maximum number of frames to keep.

    Returns:
        Normalized top stack frames.
    """
    frames: List[str] = []
    for line in (stacktrace or "").splitlines():
        match = STACK_FRAME_RE.match(line)
        if not match:
            continue
        frame = re.sub(r"\s+", " ", match.group(1)).strip()
        frame = re.sub(r"`\d+", "", frame)
        frames.append(frame)
        if len(frames) >= top_n:
            break
    return frames


def build_signature(
    normalized_message: str,
    exception_type: Optional[str],
    stack_frames: List[str],
) -> str:
    """Build a stable signature hash for a normalized event.

    Args:
        normalized_message: Normalized event message.
        exception_type: Extracted exception type, if any.
        stack_frames: Extracted top stack frames.

    Returns:
        SHA-1 signature hash.
    """
    payload = "||".join(
        [normalized_message or "", exception_type or "", *stack_frames]
    ).encode("utf-8", errors="ignore")
    return hashlib.sha1(payload).hexdigest()


def is_incident_candidate(event: RawEvent, exception_type: Optional[str]) -> bool:
    """Decide whether a raw event should be treated as incident-like.

    Args:
        event: Raw parsed event.
        exception_type: Extracted exception type, if any.

    Returns:
        `True` when the event has incident characteristics.
    """
    level = (event.level or "").upper()
    if level in {"ERROR", "FATAL"}:
        return True
    if exception_type:
        return True
    if event.stacktrace.strip():
        return True
    if event.http_status is not None and event.http_status >= 500:
        return True
    if level in {"WARN", "WARNING"} and INCIDENT_KEYWORD_RE.search(event.message):
        return True
    return bool(INCIDENT_KEYWORD_RE.search(event.message) or INCIDENT_KEYWORD_RE.search(event.raw_text))


def make_event_signature(
    event: RawEvent, normalization_stats: Optional[NormalizationStats] = None
) -> tuple[str, Optional[str], List[str], bool]:
    """Build signature ingredients for a raw event.

    Args:
        event: Raw parsed event.
        normalization_stats: Optional normalization stats accumulator.

    Returns:
        Tuple of normalized message, exception type, stack frames and incident flag.
    """
    normalized_message = normalize_text(event.message, normalization_stats)
    exception_type = extract_exception_type(event.stacktrace, event.message)
    stack_frames = extract_stack_frames(event.stacktrace)
    is_incident = is_incident_candidate(event, exception_type)
    return normalized_message, exception_type, stack_frames, is_incident


def build_embedding_text(event: Event) -> str:
    """Build embedding text used by semantic clustering models.

    Args:
        event: Canonical event to describe.

    Returns:
        Concise text representation for semantic embeddings.
    """
    parts = [
        f"profile={event.parser_profile}",
        f"level={(event.level or 'unknown').lower()}",
        f"component={event.component or 'unknown'}",
        f"message={event.normalized_message or normalize_text(event.message)}",
    ]
    if event.exception_type:
        parts.append(f"exception={event.exception_type}")
    if event.stack_frames:
        parts.append("stack=" + " | ".join(event.stack_frames))
    elif event.stacktrace.strip():
        parts.append("stack=" + normalize_text(event.stacktrace))
    if event.http_status is not None:
        parts.append(f"http_status={event.http_status}")
    if event.request_id:
        parts.append("has_request_id=true")
    if event.trace_id:
        parts.append("has_trace_id=true")
    raw_fallback = normalize_text(event.raw_text) if event.raw_text else ""
    if raw_fallback and raw_fallback not in " ".join(parts):
        parts.append(f"raw={raw_fallback}")
    return " || ".join(part for part in parts if part)


def build_signature_text(
    normalized_message: str,
    exception_type: Optional[str],
    stack_frames: List[str],
) -> str:
    """Build a readable signature text description for reports and storage.

    Args:
        normalized_message: Normalized event message.
        exception_type: Extracted exception type, if any.
        stack_frames: Extracted top stack frames.

    Returns:
        Human-readable signature descriptor string.
    """
    parts = [f"normalized_message={normalized_message}"]
    if exception_type:
        parts.append(f"exception_type={exception_type}")
    if stack_frames:
        parts.append("top_stack_frames=" + " | ".join(stack_frames))
    return " || ".join(parts)
