from collections import Counter

from .models import AnalysisSummary, Event


def coverage_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def confidence_label(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


class AnalysisQualityAccumulator:
    def __init__(self, source_name: str) -> None:
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
        self.profile_counts: Counter[str] = Counter()

    def add(self, event: Event) -> None:
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
        if event.parser_profile == "generic_text":
            self.fallback_count += 1

    def build_summary(self, cluster_count: int) -> AnalysisSummary:
        timestamp_coverage = coverage_ratio(self.timestamp_count, self.event_count)
        level_coverage = coverage_ratio(self.level_count, self.event_count)
        component_coverage = coverage_ratio(self.component_count, self.event_count)
        exception_coverage = coverage_ratio(self.exception_count, self.event_count)
        stacktrace_coverage = coverage_ratio(self.stacktrace_count, self.event_count)
        request_id_coverage = coverage_ratio(self.request_id_count, self.event_count)
        trace_id_coverage = coverage_ratio(self.trace_id_count, self.event_count)
        fallback_profile_rate = coverage_ratio(self.fallback_count, self.event_count)

        parser_quality_score = (
            0.25 * timestamp_coverage
            + 0.15 * level_coverage
            + 0.15 * component_coverage
            + 0.2 * exception_coverage
            + 0.15 * stacktrace_coverage
            + 0.1 * (1.0 - fallback_profile_rate)
        )
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
            parser_quality_score=parser_quality_score,
            parser_quality_label=confidence_label(parser_quality_score),
            parser_profiles=parser_profiles,
        )
