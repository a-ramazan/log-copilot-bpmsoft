from __future__ import annotations

import uuid
from typing import Optional

from ..models import Event, RawEvent
from ..normalization import NormalizationStats
from ..signatures import build_embedding_text, build_signature, make_event_signature


def build_event(
    raw_event: RawEvent,
    run_id: str,
    normalization_stats: Optional[NormalizationStats] = None,
) -> Event:
    """
    Строит канонический Event из RawEvent.

    Важно:
    - здесь выполняется общая нормализация текста;
    - часть полей (exception/stack/signature/is_incident) пока вычисляется здесь
      для обратной совместимости текущего incidents-flow.
    """
    normalized_message, exception_type, stack_frames, is_incident = make_event_signature(
        raw_event,
        normalization_stats=normalization_stats,
    )
    event = Event(
        event_id=str(uuid.uuid4()),
        run_id=run_id,
        source_file=raw_event.source_file,
        parser_profile=raw_event.parser_profile,
        parser_confidence=raw_event.parser_confidence,
        timestamp=raw_event.timestamp,
        level=raw_event.level,
        message=raw_event.message,
        stacktrace=raw_event.stacktrace,
        raw_text=raw_event.raw_text,
        line_count=raw_event.line_count,
        normalized_message=normalized_message,
        signature_hash=build_signature(
            normalized_message,
            exception_type,
            stack_frames,
        ),
        embedding_text="",
        exception_type=exception_type,
        stack_frames=stack_frames,
        component=raw_event.component,
        request_id=raw_event.request_id,
        trace_id=raw_event.trace_id,
        http_status=raw_event.http_status,
        method=raw_event.method,
        path=raw_event.path,
        latency_ms=raw_event.latency_ms,
        response_size=raw_event.response_size,
        client_ip=raw_event.client_ip,
        user_agent=raw_event.user_agent,
        attributes=dict(raw_event.attributes),
        is_incident=is_incident,
    )
    event.embedding_text = build_embedding_text(event)
    return event
