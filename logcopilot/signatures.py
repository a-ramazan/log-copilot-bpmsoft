import hashlib
import re
from typing import List, Optional

from .models import Event, RawEvent
from .normalization import normalize_text

EXCEPTION_RE = re.compile(r"\b([A-Za-z_][\w.]+(?:Exception|Error))\b")
STACK_FRAME_RE = re.compile(r"^\s*at\s+(.+?)(?:\(|\s+in\s+|$)")


def extract_exception_type(*chunks: str) -> Optional[str]:
    for chunk in chunks:
        if not chunk:
            continue
        match = EXCEPTION_RE.search(chunk)
        if match:
            return match.group(1)
    return None


def extract_stack_frames(stacktrace: str, top_n: int = 3) -> List[str]:
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
    payload = "||".join(
        [normalized_message or "", exception_type or "", *stack_frames]
    ).encode("utf-8", errors="ignore")
    return hashlib.sha1(payload).hexdigest()


def is_incident_candidate(event: RawEvent, exception_type: Optional[str]) -> bool:
    level = (event.level or "").upper()
    if level in {"ERROR", "FATAL"}:
        return True
    if exception_type:
        return True
    if event.stacktrace.strip():
        return True
    if event.http_status is not None and event.http_status >= 500:
        return True
    message = event.message.lower()
    return any(token in message for token in (" exception", " failed", " error"))


def make_event_signature(event: RawEvent) -> tuple[str, Optional[str], List[str], bool]:
    normalized_message = normalize_text(event.message)
    exception_type = extract_exception_type(event.stacktrace, event.message)
    stack_frames = extract_stack_frames(event.stacktrace)
    signature_hash = build_signature(normalized_message, exception_type, stack_frames)
    is_incident = is_incident_candidate(event, exception_type)
    return normalized_message, exception_type, stack_frames, is_incident


def build_embedding_text(event: Event) -> str:
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
