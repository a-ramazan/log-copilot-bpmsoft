from __future__ import annotations

"""Pipeline stage entrypoint for canonical event building."""

import logging
import time
from typing import Dict

from ..domain import EventBuildStageResult, PipelineContext
from ..parsing import canonical_to_raw_event
from .events import build_event

logger = logging.getLogger(__name__)


def _print_phase(message: str) -> None:
    """Emit one user-visible phase message and mirror it to structured logs."""
    logger.info("run_phase: %s", message)
    print(f"[logcopilot] {message}")


def run_event_building(context: PipelineContext) -> PipelineContext:
    """Build normalized canonical events from parsed records."""
    if context.parse_result is None:
        raise RuntimeError("Parsing must run before event building.")

    build_started = time.perf_counter()
    events = []
    multiline_merges = 0
    _print_phase(f"event building started: records={len(context.parsed_records)}")
    for parsed_record in context.parsed_records:
        raw_event = canonical_to_raw_event(parsed_record.event, source_file=parsed_record.source_file)


        event = build_event(
            raw_event,
            run_id=context.run_id,
            normalization_stats=context.normalization_stats,
        )


        events.append(event)
        if event.line_count > 1:
            multiline_merges += 1

    timings: Dict[str, float] = {"event_building": time.perf_counter() - build_started}
    _print_phase(
        f"event building finished: events={len(events)} "
        f"duration={timings['event_building']:.3f}s"
    )
    context.events = events
    context.event_build_result = EventBuildStageResult(
        events=events,
        event_count=len(events),
        multiline_merges=multiline_merges,
        timings=timings,
    )
    context.timings.update(timings)
    return context
