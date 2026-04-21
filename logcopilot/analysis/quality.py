from __future__ import annotations

"""Quality scoring helpers for analysis summaries and profile-fit estimation."""

from collections import Counter
import re

from ..domain import AnalysisSummary, Event


INCIDENT_HINT_RE = re.compile(
    r"(exception|failed|failure|fatal|timeout|refused|denied|panic|stack trace|hresult|manifest_invalid|error)",
    re.IGNORECASE,
)


def _event_ratio(events: list[Event], predicate) -> float:
    """Compute a coverage ratio for events matching a predicate."""
    total = len(events)
    return coverage_ratio(sum(1 for event in events if predicate(event)), total)


def _profile_fit_label(
    selected_profile: str,
    recommended_profile: str,
    selected_score: float,
    recommended_score: float,
) -> str:
    """Map profile scores onto a coarse fit label."""
    fit_delta = recommended_score - selected_score
    if recommended_profile == selected_profile:
        return "high"
    if recommended_score >= 0.5 and selected_score <= 0.35:
        return "low"
    if fit_delta <= 0.1:
        return "high"
    if fit_delta <= 0.2:
        return "medium"
    return "low"


def coverage_ratio(numerator: float, denominator: int) -> float:
    """Safely divide a coverage metric by its denominator.

    Args:
        numerator: Covered item count or accumulated score.
        denominator: Total item count.

    Returns:
        Coverage ratio in the `[0, 1]` range when denominator is positive.
    """
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def confidence_label(score: float) -> str:
    """Map a numeric score onto a coarse confidence label.

    Args:
        score: Confidence score in the `[0, 1]` range.

    Returns:
        Confidence label string.
    """
    if score >= 0.8:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


class AnalysisQualityAccumulator:
    """Accumulate field coverage and signal statistics for one source file."""

    def __init__(self, source_name: str) -> None:
        """Initialize an empty quality accumulator.

        Args:
            source_name: Display name for the analyzed source.
        """
        self.source_name = source_name
        self.event_count = 0
        self.incident_event_count = 0
        self.timestamp_count = 0
        self.level_count = 0
        self.component_count = 0
        self.exception_count = 0
        self.stacktrace_count = 0
        self.request_id_count = 0
        self.trace_id_count = 0
        self.fallback_count = 0
        self.parser_confidence_total = 0.0
        self.profile_counts: Counter[str] = Counter()

    def add(self, event: Event) -> None:
        """Add one canonical event to the quality statistics.

        Args:
            event: Event to account for.

        Returns:
            None.
        """
        self.event_count += 1
        self.profile_counts[event.parser_profile] += 1
        if event.is_incident:
            self.incident_event_count += 1
        if event.timestamp:
            self.timestamp_count += 1
        if event.level:
            self.level_count += 1
        if event.component:
            self.component_count += 1
        if event.exception_type:
            self.exception_count += 1
        if event.stacktrace.strip():
            self.stacktrace_count += 1
        if event.request_id:
            self.request_id_count += 1
        if event.trace_id:
            self.trace_id_count += 1
        if event.parser_profile in {"generic_text", "generic_fallback"}:
            self.fallback_count += 1
        self.parser_confidence_total += float(event.parser_confidence or 0.0)

    def build_summary(self, cluster_count: int) -> AnalysisSummary:
        """Build an immutable analysis summary from accumulated metrics.

        Args:
            cluster_count: Number of clusters produced for the analyzed source.

        Returns:
            Analysis summary with coverage and signal quality metrics.
        """
        timestamp_coverage = coverage_ratio(self.timestamp_count, self.event_count)
        level_coverage = coverage_ratio(self.level_count, self.event_count)
        component_coverage = coverage_ratio(self.component_count, self.event_count)
        exception_coverage = coverage_ratio(self.exception_count, self.event_count)
        stacktrace_coverage = coverage_ratio(self.stacktrace_count, self.event_count)
        request_id_coverage = coverage_ratio(self.request_id_count, self.event_count)
        trace_id_coverage = coverage_ratio(self.trace_id_count, self.event_count)
        fallback_profile_rate = coverage_ratio(self.fallback_count, self.event_count)
        incident_signal_ratio = coverage_ratio(self.incident_event_count, self.event_count)
        mean_parser_confidence = coverage_ratio(self.parser_confidence_total, self.event_count)

        parse_quality_score = (
            0.35 * timestamp_coverage
            + 0.25 * level_coverage
            + 0.2 * component_coverage
            + 0.1 * mean_parser_confidence
            + 0.1 * (1.0 - fallback_profile_rate)
        )
        incident_signal_score = (
            0.45 * incident_signal_ratio
            + 0.25 * exception_coverage
            + 0.2 * stacktrace_coverage
            + 0.1 * level_coverage
        )
        parse_quality_label = confidence_label(parse_quality_score)
        incident_signal_label = confidence_label(incident_signal_score)
        parser_profiles = ", ".join(
            f"{profile}:{count}" for profile, count in self.profile_counts.most_common()
        )
        return AnalysisSummary(
            source_name=self.source_name,
            event_count=self.event_count,
            cluster_count=cluster_count,
            incident_event_count=self.incident_event_count,
            timestamp_coverage=timestamp_coverage,
            level_coverage=level_coverage,
            component_coverage=component_coverage,
            exception_coverage=exception_coverage,
            stacktrace_coverage=stacktrace_coverage,
            request_id_coverage=request_id_coverage,
            trace_id_coverage=trace_id_coverage,
            fallback_profile_rate=fallback_profile_rate,
            parser_quality_score=parse_quality_score,
            parser_quality_label=parse_quality_label,
            parse_quality_score=parse_quality_score,
            parse_quality_label=parse_quality_label,
            incident_signal_score=incident_signal_score,
            incident_signal_label=incident_signal_label,
            mean_parser_confidence=mean_parser_confidence,
            parser_profiles=parser_profiles,
        )


def assess_profile_fit(events: list[Event], selected_profile: str) -> dict:
    """Estimate how well the selected profile matches the parsed event structure.

    Args:
        events: Parsed canonical events for the run.
        selected_profile: Profile chosen by the caller.

    Returns:
        Recommendation payload with per-profile scores and fit label.
    """
    total = len(events)
    if total <= 0:
        return {
            "selected_profile": selected_profile,
            "recommended_profile": selected_profile,
            "selected_score": 0.0,
            "recommended_score": 0.0,
            "fit_label": "low",
            "reason": "no events parsed",
            "scores": {"incidents": 0.0, "heatmap": 0.0, "traffic": 0.0},
        }

    timestamp_ratio = _event_ratio(events, lambda event: event.timestamp)
    level_ratio = _event_ratio(events, lambda event: event.level)
    component_ratio = _event_ratio(events, lambda event: event.component)
    method_ratio = _event_ratio(events, lambda event: event.method)
    path_ratio = _event_ratio(events, lambda event: event.path)
    status_ratio = _event_ratio(events, lambda event: event.http_status is not None)
    latency_ratio = _event_ratio(events, lambda event: event.latency_ms is not None)
    ip_ratio = _event_ratio(events, lambda event: event.client_ip)
    incident_ratio = _event_ratio(events, lambda event: event.is_incident)
    exception_ratio = _event_ratio(events, lambda event: event.exception_type)
    stacktrace_ratio = _event_ratio(events, lambda event: event.stacktrace.strip())
    severity_ratio = _event_ratio(
        events,
        lambda event: (event.level or "").upper() in {"WARN", "ERROR", "FATAL"},
    )
    incident_hint_ratio = _event_ratio(
        events,
        lambda event: INCIDENT_HINT_RE.search(event.message or ""),
    )
    windows_servicing_ratio = _event_ratio(
        events,
        lambda event: event.parser_profile == "windows_servicing",
    )
    web_access_ratio = _event_ratio(
        events,
        lambda event: event.parser_profile == "web_access",
    )
    fallback_ratio = _event_ratio(
        events,
        lambda event: event.parser_profile in {"generic_text", "generic_fallback"},
    )
    traffic_signal_ratio = (method_ratio + path_ratio + status_ratio + ip_ratio) / 4.0

    scores = {
        "incidents": round(
            max(
                0.0,
                min(
                    1.0,
                    0.2 * incident_ratio
                    + 0.15 * severity_ratio
                    + 0.15 * exception_ratio
                    + 0.1 * stacktrace_ratio
                    + 0.15 * incident_hint_ratio
                    + 0.1 * component_ratio
                    + 0.1 * timestamp_ratio
                    + 0.05 * level_ratio
                    + 0.1 * (1.0 - fallback_ratio)
                    + 0.15 * windows_servicing_ratio
                    - 0.15 * traffic_signal_ratio,
                ),
            ),
            3,
        ),
        "heatmap": round(
            0.25 * timestamp_ratio
            + 0.2 * component_ratio
            + 0.3 * latency_ratio
            + 0.15 * method_ratio
            + 0.1 * path_ratio,
            3,
        ),
        "traffic": round(
            0.3 * method_ratio
            + 0.25 * path_ratio
            + 0.15 * status_ratio
            + 0.15 * ip_ratio
            + 0.1 * latency_ratio
            + 0.05 * timestamp_ratio,
            3,
        ),
    }
    scores["traffic"] = round(
        min(1.0, scores["traffic"] + 0.1 * web_access_ratio),
        3,
    )
    recommended_profile = max(scores, key=scores.get)
    selected_score = scores.get(selected_profile, 0.0)
    recommended_score = scores[recommended_profile]
    fit_label = _profile_fit_label(
        selected_profile=selected_profile,
        recommended_profile=recommended_profile,
        selected_score=selected_score,
        recommended_score=recommended_score,
    )

    if fit_label == "high":
        reason = f"selected profile '{selected_profile}' matches extracted structure"
    else:
        reason = (
            f"selected profile '{selected_profile}' is weaker than '{recommended_profile}' "
            f"for extracted structure"
        )

    return {
        "selected_profile": selected_profile,
        "recommended_profile": recommended_profile,
        "selected_score": selected_score,
        "recommended_score": recommended_score,
        "fit_label": fit_label,
        "reason": reason,
        "scores": scores,
    }
