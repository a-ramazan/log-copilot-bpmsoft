from __future__ import annotations

import re

from ..base import BaseParser
from ..models import CanonicalEvent
from ..utils import (
    build_generic_event,
    clamp_confidence,
    extract_http_tokens,
    extract_ids,
    normalize_level,
    parse_logfmt_pairs,
    parse_timestamp,
    summarize_parse_result,
)

CONTINUATION_RE = re.compile(r"^\s+")
TIMESTAMP_PREFIX_RE = re.compile(
    r"^(?P<timestamp>"
    r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[,.]\d+)?"
    r"|\d{4}/\d{2}/\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[,.]\d+)?"
    r"|\d{2}/\d{2}/\d{2}[ T]\d{2}:\d{2}:\d{2}"
    r")"
)
LEVEL_RE = re.compile(r"\b(TRACE|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL)\b", re.IGNORECASE)
COMPONENT_SPLIT_RE = re.compile(r"^(?P<head>[^:|\-]{1,64})\s(?:-|:|\|)\s(?P<body>.+)$")


def _build_fallback_event(raw_text: str, *, source: str | None, line_count: int) -> CanonicalEvent:
    event = build_generic_event(
        raw_text,
        parser_name="generic_fallback",
        parser_confidence=0.3,
        source=source,
        line_count=line_count,
    )

    stripped = raw_text.strip()
    first_line = stripped.splitlines()[0] if stripped else ""
    attributes = dict(event.attributes)

    timestamp_match = TIMESTAMP_PREFIX_RE.match(first_line)
    if timestamp_match:
        event.timestamp = parse_timestamp(timestamp_match.group("timestamp"))

    level_match = LEVEL_RE.search(first_line)
    if level_match:
        event.level = normalize_level(level_match.group(1))

    if source and not event.source:
        event.source = source

    headless = first_line
    if timestamp_match:
        headless = headless[timestamp_match.end():].lstrip(" ,")
    if level_match:
        level_text = level_match.group(0)
        level_index = headless.upper().find(level_text.upper())
        if level_index != -1:
            headless = (headless[:level_index] + headless[level_index + len(level_text):]).strip(" []:-")

    component_match = COMPONENT_SPLIT_RE.match(headless)
    if component_match:
        head = component_match.group("head").strip()
        body = component_match.group("body").strip()
        if head and body and len(head.split()) <= 4:
            event.component = head
            event.message = body

    kv_pairs = parse_logfmt_pairs(first_line)
    if kv_pairs:
        attributes["kv_pairs"] = kv_pairs

    request_id, trace_id = extract_ids(raw_text)
    if request_id and not event.request_id:
        event.request_id = request_id
    if trace_id and not event.trace_id:
        event.trace_id = trace_id

    http_tokens = extract_http_tokens(raw_text)
    if http_tokens["http_method"] and not event.http_method:
        event.http_method = http_tokens["http_method"]
    if http_tokens["http_path"] and not event.http_path:
        event.http_path = http_tokens["http_path"]
    if http_tokens["http_status"] is not None and event.http_status is None:
        event.http_status = http_tokens["http_status"]
    if http_tokens["latency_ms"] is not None and event.latency_ms is None:
        event.latency_ms = http_tokens["latency_ms"]
    if http_tokens["client_ip"] and not event.client_ip:
        event.client_ip = http_tokens["client_ip"]

    confidence = 0.25
    if event.timestamp is not None:
        confidence += 0.1
    if event.level is not None:
        confidence += 0.05
    if event.component:
        confidence += 0.05
    if event.http_method or event.http_path or event.http_status is not None:
        confidence += 0.05
    if attributes:
        confidence += 0.05
    event.attributes = attributes
    event.parser_confidence = clamp_confidence(confidence, maximum=0.5)
    return event


class GenericFallbackParser(BaseParser):
    """Last-resort parser that preserves data without over-claiming structure."""

    name = "generic_fallback"

    def can_parse(self, sample: str) -> float:
        return 0.15 if sample.strip() else 0.0

    def parse(self, text: str, source: str | None = None):
        events = []
        lines = text.splitlines()
        buffer: list[str] = []

        def flush() -> None:
            if not buffer:
                return
            raw_text = "\n".join(buffer).strip()
            events.append(_build_fallback_event(raw_text, source=source, line_count=len(buffer)))
            buffer.clear()

        for line in lines:
            if not line.strip():
                flush()
                continue
            if CONTINUATION_RE.match(line) and buffer:
                buffer.append(line)
            else:
                flush()
                buffer.append(line)
        flush()

        return summarize_parse_result(
            parser_name=self.name,
            events=events,
            total_lines=len(lines),
            warnings=[],
            fallback_events=len(events),
            confidence_cap=0.4,
        )
