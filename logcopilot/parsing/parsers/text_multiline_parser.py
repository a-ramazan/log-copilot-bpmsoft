from __future__ import annotations

from dataclasses import dataclass
import re

from ..base import BaseParser
from ..models import CanonicalEvent
from ..utils import build_generic_event, clamp_confidence, extract_http_tokens, extract_ids, normalize_level, parse_timestamp, summarize_parse_result

START_PATTERNS = (
    re.compile(
        r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[,.]\d+)?) "
        r"\[(?P<thread>[^\]]+)\] (?P<level>[A-Z]+)\s+(?P<context>.*?) - (?P<body>.*)$"
    ),
    re.compile(
        r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:[,.]\d+)?)\s+"
        r"(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\s+(?P<context>.*?) - (?P<body>.*)$"
    ),
    re.compile(
        r"^(?P<timestamp>\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+"
        r"(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\s+"
        r"(?P<context>[^:]+):\s*(?P<body>.*)$"
    ),
    re.compile(
        r"^(?P<timestamp>\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+"
        r"\[(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)\]\s+"
        r"(?P<context>[^:]+):\s*(?P<body>.*)$"
    ),
)
GENERIC_START_RE = re.compile(
    r"^(?P<timestamp>\d{4}[-/]\d{2}[-/]\d{2} \d{2}:\d{2}:\d{2}(?:[,.]\d+)?)?\s*"
    r"(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL)?\b",
    re.IGNORECASE,
)
STACK_CONTINUATION_RE = re.compile(
    r"^\s+(?:at\s+|--- End of|---|inner exception|caused by:|severity:|sqlstate:|detail:|messagetext:|schemaname:|tablename:|constraintname:|file:|line:|routine:)",
    re.IGNORECASE,
)


@dataclass
class ParsedFirstLine:
    timestamp: object
    level: str | None
    component: str | None
    body: str


def parse_first_line(line: str) -> ParsedFirstLine | None:
    stripped = line.rstrip()
    for pattern in START_PATTERNS:
        match = pattern.match(stripped)
        if match:
            return ParsedFirstLine(
                timestamp=parse_timestamp(match.group("timestamp")),
                level=normalize_level(match.groupdict().get("level")),
                component=(match.groupdict().get("context") or "").strip() or None,
                body=(match.groupdict().get("body") or stripped).strip(),
            )
    generic = GENERIC_START_RE.match(stripped)
    if not generic or not stripped:
        return None
    timestamp = parse_timestamp(generic.group("timestamp"))
    level = normalize_level(generic.group("level"))
    consumed = generic.end()
    body = stripped[consumed:].strip() or stripped
    component = None
    for separator in (" - ", ": ", " :: ", " | "):
        if separator in body:
            head, tail = body.split(separator, 1)
            if head.strip() and tail.strip():
                component = head.strip()
                body = tail.strip()
                break
    return ParsedFirstLine(timestamp=timestamp, level=level, component=component, body=body)


def is_start_line(line: str) -> bool:
    if not line.strip():
        return False
    if STACK_CONTINUATION_RE.match(line):
        return False
    if any(pattern.match(line) for pattern in START_PATTERNS):
        return True
    generic = GENERIC_START_RE.match(line)
    if not generic:
        return False
    return bool(generic.group("timestamp") or generic.group("level"))


class TextMultilineParser(BaseParser):
    """Parser for Java/.NET/plain text logs with multiline events."""

    name = "text_multiline"

    def can_parse(self, sample: str) -> float:
        lines = [line for line in sample.splitlines() if line.strip()]
        if not lines:
            return 0.0
        starts = sum(1 for line in lines if is_start_line(line))
        continuations = sum(1 for line in lines if STACK_CONTINUATION_RE.match(line))
        return min(1.0, (starts / len(lines)) + min(0.2, continuations / max(len(lines), 1)))

    def parse(self, text: str, source: str | None = None):
        events: list[CanonicalEvent] = []
        warnings: list[str] = []
        lines = text.splitlines()
        buffer: list[str] = []

        def flush_buffer() -> None:
            if not buffer:
                return
            raw_text = "\n".join(buffer).strip()
            first = parse_first_line(buffer[0])
            if first is None:
                events.append(build_generic_event(raw_text, parser_name="generic_fallback", parser_confidence=0.25, source=source, line_count=len(buffer)))
                buffer.clear()
                return
            message = first.body
            stack_parts: list[str] = []
            if "|" in message:
                lead, tail = message.split("|", 1)
                message = lead.strip() or tail.strip()
                if tail.strip():
                    stack_parts.append(tail.strip())
            if len(buffer) > 1:
                stack_parts.append("\n".join(line.rstrip() for line in buffer[1:]).strip())
            stacktrace = "\n".join(part for part in stack_parts if part).strip()
            request_id, trace_id = extract_ids(raw_text)
            http_tokens = extract_http_tokens(raw_text)
            component = first.component
            if component is None and source:
                component = source.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            base_confidence = 0.85
            if first.timestamp is None:
                base_confidence -= 0.15
            if first.level is None:
                base_confidence -= 0.1
            events.append(
                CanonicalEvent(
                    timestamp=first.timestamp,
                    level=first.level,
                    source=source,
                    component=component,
                    message=message or raw_text,
                    raw_text=raw_text,
                    stacktrace=stacktrace,
                    request_id=request_id,
                    trace_id=trace_id,
                    http_method=http_tokens["http_method"],
                    http_path=http_tokens["http_path"],
                    http_status=http_tokens["http_status"],
                    latency_ms=http_tokens["latency_ms"],
                    response_size=http_tokens["response_size"],
                    client_ip=http_tokens["client_ip"],
                    parser_name=self.name,
                    parser_confidence=clamp_confidence(base_confidence),
                    attributes={},
                    line_count=len(buffer),
                )
            )
            buffer.clear()

        for line in lines:
            if not line.strip() and not buffer:
                continue
            if is_start_line(line):
                flush_buffer()
                buffer.append(line)
            elif buffer:
                buffer.append(line)
            elif line.strip():
                buffer = [line]
        flush_buffer()

        fallback_events = sum(1 for event in events if event.parser_name == "generic_fallback")
        if fallback_events:
            warnings.append(f"{fallback_events} event(s) fell back to generic parsing")
        return summarize_parse_result(
            parser_name=self.name,
            events=events,
            total_lines=len(lines),
            warnings=warnings,
            fallback_events=fallback_events,
        )

