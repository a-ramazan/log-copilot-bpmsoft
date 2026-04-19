from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from ..models import RawEvent
from .models import CanonicalEvent, ParseResult
from .parsers import (
    GenericFallbackParser,
    JsonParser,
    LogfmtParser,
    SyslogParser,
    TextMultilineParser,
    WebAccessParser,
    WindowsServicingParser,
)
from .registry import ParserRegistry

logger = logging.getLogger(__name__)


def discover_log_files(root: Path) -> list[Path]:
    """Возвращает список .log файлов для обработки."""
    if root.is_file():
        return [root]
    return sorted(path for path in root.rglob("*.log") if path.is_file())


def build_default_registry() -> ParserRegistry:
    """Регистрирует штатный набор парсеров в порядке приоритета."""
    registry = ParserRegistry()
    registry.register(JsonParser())
    registry.register(LogfmtParser())
    registry.register(WebAccessParser())
    registry.register(WindowsServicingParser())
    registry.register(SyslogParser())
    registry.register(TextMultilineParser())
    registry.register(GenericFallbackParser(), is_fallback=True)
    return registry


DEFAULT_REGISTRY = build_default_registry()


def canonical_to_raw_event(event: CanonicalEvent, source_file: str) -> RawEvent:
    """
    Адаптер совместимости:
    parsing слой отдает CanonicalEvent, а core слой сейчас принимает RawEvent.
    """
    return RawEvent(
        source_file=source_file,
        parser_profile=event.parser_name,
        parser_confidence=event.parser_confidence,
        timestamp=event.timestamp,
        level=event.level,
        message=event.message,
        stacktrace=event.stacktrace,
        raw_text=event.raw_text,
        line_count=event.line_count,
        component=event.component,
        request_id=event.request_id,
        trace_id=event.trace_id,
        http_status=event.http_status,
        method=event.http_method,
        path=event.http_path,
        latency_ms=event.latency_ms,
        response_size=event.response_size,
        client_ip=event.client_ip,
        user_agent=event.user_agent,
        attributes=dict(event.attributes),
    )


def parse_file(path: Path, root: Path, registry: ParserRegistry | None = None) -> ParseResult:
    """Полный parse одного файла: select parser -> parse -> нормализация source."""
    registry = registry or DEFAULT_REGISTRY
    text = path.read_text(encoding="utf-8", errors="replace")
    source_file = path.name if root.is_file() else str(path.relative_to(root))
    parser, selection = registry.select(text)

    # логирование
    logger.info(
        "parser_selected: source_file=%s parser=%s detector_confidence=%.3f fallback=%s",
        source_file,
        selection.parser_name,
        selection.confidence,
        selection.used_fallback,
    )


    #
    result = parser.parse(text, source=source_file)




    logger.info(
        "parse_result: source_file=%s parser=%s events=%d confidence=%.3f warnings=%d",
        source_file,
        result.parser_name,
        len(result.events),
        result.confidence,
        len(result.warnings),
    )
    for event in result.events:
        if not event.source:
            event.source = source_file
    return result


def iter_canonical_events(root: Path, registry: ParserRegistry | None = None) -> Iterator[CanonicalEvent]:
    for path in discover_log_files(root):
        result = parse_file(path, root, registry=registry)
        yield from result.events


def iter_events_for_file(path: Path, root: Path, registry: ParserRegistry | None = None) -> Iterator[RawEvent]:
    source_file = path.name if root.is_file() else str(path.relative_to(root))
    result = parse_file(path, root, registry=registry)
    for event in result.events:
        yield canonical_to_raw_event(event, source_file=source_file)


def iter_events(root: Path, registry: ParserRegistry | None = None) -> Iterator[RawEvent]:
    for path in discover_log_files(root):
        yield from iter_events_for_file(path, root, registry=registry)
