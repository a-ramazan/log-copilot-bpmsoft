from __future__ import annotations

from datetime import datetime
import re

from ..base import BaseParser
from ..models import CanonicalEvent
from ..utils import build_generic_event, clamp_confidence, extract_http_tokens, normalize_level, summarize_parse_result

SYSLOG_RE = re.compile(
    r"^(?:<\d+>)?(?P<timestamp>[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+"
    r"(?P<host>\S+)\s+(?P<app>[\w./-]+)(?:\[(?P<pid>\d+)\])?:\s*(?P<message>.*)$"
)
LEVEL_PREFIX_RE = re.compile(r"^(?P<level>TRACE|DEBUG|INFO|WARN|WARNING|ERROR|ERR|FATAL|CRITICAL)\b[: -]*", re.IGNORECASE)


def parse_syslog_timestamp(value: str) -> datetime | None:
    current_year = datetime.now().year
    try:
        return datetime.strptime(f"{current_year} {value}", "%Y %b %d %H:%M:%S")
    except ValueError:
        return None


class SyslogParser(BaseParser):
    """Parser for basic syslog-like lines."""

    name = "syslog"

    def can_parse(self, sample: str) -> float:
        lines = [line for line in sample.splitlines() if line.strip()]
        if not lines:
            return 0.0
        matched = sum(1 for line in lines if SYSLOG_RE.match(line))
        return matched / len(lines)

    def parse(self, text: str, source: str | None = None):
        events: list[CanonicalEvent] = []
        warnings: list[str] = []
        fallback_events = 0
        lines = text.splitlines()
        for index, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            match = SYSLOG_RE.match(line)
            if not match:
                fallback_events += 1
                warnings.append(f"Line {index} does not match syslog pattern")
                events.append(build_generic_event(line, parser_name="generic_fallback", parser_confidence=0.25, source=source))
                continue
            message = match.group("message").strip()
            level_match = LEVEL_PREFIX_RE.match(message)
            level = normalize_level(level_match.group("level")) if level_match else None
            if level_match:
                message = message[level_match.end():].strip()
            attributes = {"host": match.group("host")}
            if match.group("pid"):
                attributes["pid"] = int(match.group("pid"))
            http_tokens = extract_http_tokens(message)
            events.append(
                CanonicalEvent(
                    timestamp=parse_syslog_timestamp(match.group("timestamp")),
                    level=level,
                    source=source or match.group("host"),
                    component=match.group("app"),
                    message=message or line.strip(),
                    raw_text=line,
                    client_ip=http_tokens["client_ip"],
                    parser_name=self.name,
                    parser_confidence=clamp_confidence(0.82 if level else 0.72),
                    attributes=attributes,
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
