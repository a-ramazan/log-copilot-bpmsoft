from __future__ import annotations

from ..base import BaseParser
from ..utils import build_event_from_mapping, build_generic_event, non_empty_lines, parse_logfmt_pairs, summarize_parse_result


class LogfmtParser(BaseParser):
    """Parser for logfmt or generic key=value lines."""

    name = "logfmt"

    def can_parse(self, sample: str) -> float:
        lines = non_empty_lines(sample)
        if not lines:
            return 0.0
        scores = []
        for line in lines:
            coverage = sum(len(match.group(0)) for match in parse_logfmt_pairs_with_spans(line))
            pairs = parse_logfmt_pairs(line)
            if len(pairs) < 2:
                scores.append(0.0)
                continue
            scores.append(coverage / max(len(line), 1))
        return sum(scores) / len(scores)

    def parse(self, text: str, source: str | None = None):
        events = []
        warnings: list[str] = []
        fallback_events = 0
        lines = text.splitlines()
        for index, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            pairs = parse_logfmt_pairs(line)
            if pairs:
                events.append(
                    build_event_from_mapping(
                        pairs,
                        raw_text=line,
                        parser_name=self.name,
                        parser_confidence=0.85,
                        source=source,
                    )
                )
                continue
            fallback_events += 1
            warnings.append(f"Line {index} has no parseable key=value pairs")
            events.append(build_generic_event(line, parser_name="generic_fallback", parser_confidence=0.25, source=source))
        return summarize_parse_result(
            parser_name=self.name,
            events=events,
            total_lines=len(lines),
            warnings=warnings,
            fallback_events=fallback_events,
        )


def parse_logfmt_pairs_with_spans(line: str):
    from ..utils import LOGFMT_RE

    return list(LOGFMT_RE.finditer(line))
