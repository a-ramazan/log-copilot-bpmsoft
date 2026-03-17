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
STACK_CONTINUATION_RE = re.compile(
    r"^\s+(?:at\s+|--- End of|---|inner exception|caused by:|severity:|sqlstate:|detail:|messagetext:|schemaname:|tablename:|constraintname:|file:|line:|routine:)",
    re.IGNORECASE,
)


def discover_log_files(root: Path) -> List[Path]:
    if root.is_file():
        return [root]
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


def sniff_profile(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for _ in range(25):
            line = handle.readline()
            if not line:
                break
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#Date:") or stripped.startswith("#Fields:"):
                return "iis_w3c"
            if LOG4NET_RE.match(stripped):
                return "log4net_like"
            if PLAIN_RE.match(stripped):
                return "plain_text"
            if W3C_LINE_RE.match(stripped):
                return "w3c_line"
    return "generic_text"


def is_event_start(line: str, parser_profile: str) -> bool:
    if parser_profile == "generic_text":
        if not line.strip():
            return False
        if STACK_CONTINUATION_RE.match(line):
            return False
        return True
    if parser_profile == "iis_w3c":
        return bool(W3C_LINE_RE.match(line))
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


def extract_component(context: Optional[str], source_file: str, parser_profile: str) -> Optional[str]:
    if parser_profile == "iis_w3c":
        return "request"
    if context:
        compact = re.sub(r"\s+", " ", context).strip()
        if compact:
            return compact
    stem = Path(source_file).stem
    return stem or None


def parse_buffer(lines: List[str], source_file: str, parser_profile: str) -> RawEvent:
    first_line = lines[0].rstrip()
    rest = [line.rstrip("\n") for line in lines[1:]]
    timestamp: Optional[datetime] = None
    level: Optional[str] = None
    body = first_line
    context: Optional[str] = None

    for pattern in (LOG4NET_RE, PLAIN_RE, W3C_LINE_RE):
        match = pattern.match(first_line)
        if match:
            timestamp = parse_timestamp(match.groupdict().get("timestamp"))
            level = match.groupdict().get("level")
            context = match.groupdict().get("context")
            body = match.groupdict().get("body") or first_line
            break

    message, stacktrace = split_message_and_stack(body, rest)
    raw_text = "\n".join(line.rstrip("\n") for line in lines).strip()
    request_id, trace_id = parse_ids(raw_text)
    http_status = parse_http_status(source_file, raw_text)
    return RawEvent(
        source_file=source_file,
        parser_profile=parser_profile,
        timestamp=timestamp,
        level=level,
        message=message or body.strip(),
        stacktrace=stacktrace,
        raw_text=raw_text,
        component=extract_component(context, source_file, parser_profile),
        request_id=request_id,
        trace_id=trace_id,
        http_status=http_status,
    )


def iter_events_for_file(path: Path, root: Path) -> Iterator[RawEvent]:
    source_file = path.name if root.is_file() else str(path.relative_to(root))
    parser_profile = sniff_profile(path)
    buffer: List[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line.strip() and not buffer:
                continue
            if line.startswith("#"):
                if buffer:
                    yield parse_buffer(buffer, source_file, parser_profile)
                    buffer = []
                continue
            if is_event_start(line, parser_profile):
                if buffer:
                    yield parse_buffer(buffer, source_file, parser_profile)
                buffer = [line]
            else:
                if buffer:
                    buffer.append(line)
                elif line.strip():
                    buffer = [line]
    if buffer:
        yield parse_buffer(buffer, source_file, parser_profile)


def iter_events(root: Path) -> Iterator[RawEvent]:
    for path in discover_log_files(root):
        yield from iter_events_for_file(path, root)
