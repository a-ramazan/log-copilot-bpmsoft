from datetime import datetime
from pathlib import Path
import re
from typing import Iterator, List, Optional

from .models import RawEvent

LOG4NET_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[,.]\d+)?) "
    r"\[(?P<thread>[^\]]+)\] (?P<level>[A-Z]+)\s+(?P<context>.*?) - (?P<body>.*)$"
)
PLAIN_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[,.]\d+)?)\s+"
    r"(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\s+(?P<context>.*?) - (?P<body>.*)$"
)
W3C_LINE_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<body>.+)$"
)
REQUEST_ID_RE = re.compile(r"\brequestid\b\s*[:=]?\s*(?P<value>\S+)", re.IGNORECASE)
TRACE_ID_RE = re.compile(
    r"\b(?:traceid|correlationid|activityid|connectionid)\b\s*[:=]?\s*(?P<value>\S+)",
    re.IGNORECASE,
)
HTTP_STATUS_RE = re.compile(r"\s(?P<status>\d{3})\s+\d+\s*$")

TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S,%f",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
)


def discover_log_files(root: Path) -> List[Path]:
    return sorted(path for path in root.rglob("*.log") if path.is_file())


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def is_event_start(line: str) -> bool:
    return bool(LOG4NET_RE.match(line) or PLAIN_RE.match(line) or W3C_LINE_RE.match(line))


def parse_ids(raw_text: str) -> tuple[Optional[str], Optional[str]]:
    request_match = REQUEST_ID_RE.search(raw_text)
    trace_match = TRACE_ID_RE.search(raw_text)
    request_id = request_match.group("value") if request_match else None
    trace_id = trace_match.group("value") if trace_match else None
    return request_id, trace_id


def parse_http_status(source_file: str, message: str) -> Optional[int]:
    if not source_file.endswith("Request.log"):
        return None
    match = HTTP_STATUS_RE.search(message)
    if not match:
        return None
    try:
        return int(match.group("status"))
    except ValueError:
        return None


def split_message_and_stack(body: str, continuation_lines: List[str]) -> tuple[str, str]:
    stack_parts: List[str] = []
    message = (body or "").strip()
    if "|" in message:
        lead, tail = message.split("|", 1)
        message = lead.strip() or tail.strip()
        if tail.strip():
            stack_parts.append(tail.strip())
    continuation = "\n".join(line.rstrip() for line in continuation_lines).strip()
    if continuation:
        stack_parts.append(continuation)
    stacktrace = "\n".join(part for part in stack_parts if part).strip()
    return message, stacktrace


def parse_buffer(lines: List[str], source_file: str) -> RawEvent:
    first_line = lines[0].rstrip()
    rest = [line.rstrip("\n") for line in lines[1:]]
    timestamp: Optional[datetime] = None
    level: Optional[str] = None
    body = first_line

    for pattern in (LOG4NET_RE, PLAIN_RE, W3C_LINE_RE):
        match = pattern.match(first_line)
        if match:
            timestamp = parse_timestamp(match.groupdict().get("timestamp"))
            level = match.groupdict().get("level")
            body = match.groupdict().get("body") or first_line
            break

    message, stacktrace = split_message_and_stack(body, rest)
    raw_text = "\n".join(line.rstrip("\n") for line in lines).strip()
    request_id, trace_id = parse_ids(raw_text)
    http_status = parse_http_status(source_file, raw_text)
    return RawEvent(
        source_file=source_file,
        timestamp=timestamp,
        level=level,
        message=message or body.strip(),
        stacktrace=stacktrace,
        raw_text=raw_text,
        request_id=request_id,
        trace_id=trace_id,
        http_status=http_status,
    )


def iter_events_for_file(path: Path, root: Path) -> Iterator[RawEvent]:
    source_file = str(path.relative_to(root))
    buffer: List[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line.strip() and not buffer:
                continue
            if line.startswith("#"):
                if buffer:
                    yield parse_buffer(buffer, source_file)
                    buffer = []
                continue
            if is_event_start(line):
                if buffer:
                    yield parse_buffer(buffer, source_file)
                buffer = [line]
            else:
                if buffer:
                    buffer.append(line)
                elif line.strip():
                    buffer = [line]
    if buffer:
        yield parse_buffer(buffer, source_file)


def iter_events(root: Path) -> Iterator[RawEvent]:
    for path in discover_log_files(root):
        yield from iter_events_for_file(path, root)

