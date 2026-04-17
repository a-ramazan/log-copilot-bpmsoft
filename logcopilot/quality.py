from collections import Counter
import re

from .models import AnalysisSummary, Event


INCIDENT_HINT_RE = re.compile(
    r"(exception|failed|failure|fatal|timeout|refused|denied|panic|stack trace|hresult|manifest_invalid|error)",
    re.IGNORECASE,
)


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
        self.parser_confidence_total = 0.0
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
        if event.parser_profile in {"generic_text", "generic_fallback"}:
            self.fallback_count += 1
        self.parser_confidence_total += float(event.parser_confidence or 0.0)

    def build_summary(self, cluster_count: int) -> AnalysisSummary:
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
            parser_quality_label=confidence_label(parse_quality_score),
            parse_quality_score=parse_quality_score,
            parse_quality_label=confidence_label(parse_quality_score),
            incident_signal_score=incident_signal_score,
            incident_signal_label=confidence_label(incident_signal_score),
            mean_parser_confidence=mean_parser_confidence,
            parser_profiles=parser_profiles,
        )


def assess_profile_fit(events: list[Event], selected_profile: str) -> dict:
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

    timestamp_ratio = coverage_ratio(sum(1 for event in events if event.timestamp), total)
    level_ratio = coverage_ratio(sum(1 for event in events if event.level), total)
    component_ratio = coverage_ratio(sum(1 for event in events if event.component), total)
    method_ratio = coverage_ratio(sum(1 for event in events if event.method), total)
    path_ratio = coverage_ratio(sum(1 for event in events if event.path), total)
    status_ratio = coverage_ratio(sum(1 for event in events if event.http_status is not None), total)
    latency_ratio = coverage_ratio(sum(1 for event in events if event.latency_ms is not None), total)
    ip_ratio = coverage_ratio(sum(1 for event in events if event.client_ip), total)
    incident_ratio = coverage_ratio(sum(1 for event in events if event.is_incident), total)
    exception_ratio = coverage_ratio(sum(1 for event in events if event.exception_type), total)
    stacktrace_ratio = coverage_ratio(sum(1 for event in events if event.stacktrace.strip()), total)
    severity_ratio = coverage_ratio(
        sum(1 for event in events if (event.level or "").upper() in {"WARN", "ERROR", "FATAL"}),
        total,
    )
    incident_hint_ratio = coverage_ratio(
        sum(1 for event in events if INCIDENT_HINT_RE.search(event.message or "")),
        total,
    )
    windows_servicing_ratio = coverage_ratio(
        sum(1 for event in events if event.parser_profile == "windows_servicing"),
        total,
    )
    web_access_ratio = coverage_ratio(
        sum(1 for event in events if event.parser_profile == "web_access"),
        total,
    )
    fallback_ratio = coverage_ratio(
        sum(1 for event in events if event.parser_profile in {"generic_text", "generic_fallback"}),
        total,
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
    fit_delta = recommended_score - selected_score
    if recommended_profile == selected_profile:
        fit_label = "high"
    elif recommended_score >= 0.5 and selected_score <= 0.35:
        fit_label = "low"
    elif fit_delta <= 0.1:
        fit_label = "high"
    elif fit_delta <= 0.2:
        fit_label = "medium"
    else:
        fit_label = "low"

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
