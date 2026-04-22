from __future__ import annotations

from datetime import datetime
import re

from ..base import BaseParser
from ..models import CanonicalEvent
from ..utils import build_generic_event, clamp_confidence, coerce_latency_ms, summarize_parse_result

ACCESS_RE = re.compile(
    r"^(?P<client_ip>\S+)\s+\S+\s+\S+\s+\[(?P<timestamp>[^\]]+)\]\s+"
    r'"(?P<method>[A-Z]+)\s+(?P<path>\S+)(?:\s+HTTP/\d+(?:\.\d+)?)?"\s+'
    r"(?P<status>\d{3})\s+(?P<size>\S+)(?:\s+\"(?P<referer>[^\"]*)\"\s+\"(?P<user_agent>[^\"]*)\")?"
    r"(?P<tail>.*)$"
)

ACCESS_TIME_FORMATS = ("%d/%b/%Y:%H:%M:%S %z", "%d/%b/%Y:%H:%M:%S")
LATENCY_PATTERNS = (
    re.compile(r"\brequest_time=(?P<value>\d+(?:\.\d+)?)\b"),
    re.compile(r"\b(?:latency|duration|rt)=(?P<value>\d+(?:\.\d+)?(?:ms|s)?)\b", re.IGNORECASE),
    re.compile(r"\b(?P<value>\d+(?:\.\d+)?)ms\b"),
)


def parse_access_timestamp(value: str) -> datetime | None:
    for fmt in ACCESS_TIME_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def parse_latency_ms(tail: str) -> float | None:
    for index, pattern in enumerate(LATENCY_PATTERNS):
        match = pattern.search(tail)
        if match:
            value = match.group("value")
            latency = coerce_latency_ms(value)
            if latency is not None:
                if index == 0 and not str(value).strip().lower().endswith(("ms", "s")):
                    return latency * 1000.0
                return latency
    return None


class WebAccessParser(BaseParser):
    """Parser for common or combined access-log style lines."""

    name = "web_access"

    def can_parse(self, sample: str) -> float:
        lines = [line for line in sample.splitlines() if line.strip()]
        if not lines:
            return 0.0
        matched = sum(1 for line in lines if ACCESS_RE.match(line))
        return matched / len(lines)

    def parse(self, text: str, source: str | None = None):
        events: list[CanonicalEvent] = []
        warnings: list[str] = []
        fallback_events = 0
        lines = text.splitlines()
        for index, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            match = ACCESS_RE.match(line)
            if not match:
                fallback_events += 1
                warnings.append(f"Line {index} does not match access-log pattern")
                events.append(build_generic_event(line, parser_name="generic_fallback", parser_confidence=0.25, source=source))
                continue
            tail = match.group("tail") or ""
            message = f"{match.group('method')} {match.group('path')}"
            response_size = None if match.group("size") == "-" else int(match.group("size"))
            latency_ms = parse_latency_ms(tail)
            events.append(
                CanonicalEvent(
                    timestamp=parse_access_timestamp(match.group("timestamp")),
                    level="INFO",
                    source=source,
                    component="access",
                    message=message,
                    raw_text=line,
                    http_method=match.group("method"),
                    http_path=match.group("path"),
                    http_status=int(match.group("status")),
                    latency_ms=latency_ms,
                    response_size=response_size,
                    client_ip=match.group("client_ip"),
                    user_agent=(match.group("user_agent") or "") or None,
                    parser_name=self.name,
                    parser_confidence=clamp_confidence(0.9 if latency_ms is not None else 0.82),
                    attributes={"referer": match.group("referer")} if match.group("referer") else {},
                    line_count=1,
                )
            )
        return summarize_parse_result(
            parser_name=self.name,
            events=events,
            total_lines=len(lines),
            warnings=warnings,
            fallback_events=fallback_events,
        )
