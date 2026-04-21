from __future__ import annotations

"""Signature-based clustering helpers for incident-oriented event analysis."""

from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from ..domain import ClusterSummary, Event
from ..text import build_signature_text
from .quality import confidence_label


def choose_first_non_null(values: Iterable[Optional[str]]) -> Optional[str]:
    """Return the first truthy string from an iterable.

    Args:
        values: Candidate string values.

    Returns:
        First non-empty value, or `None` if none exists.
    """
    for value in values:
        if value:
            return value
    return None


def top_source_files(events: List[Event], limit: int = 5) -> str:
    """Summarize the most common source files for a cluster.

    Args:
        events: Events that belong to one signature cluster.
        limit: Maximum number of source files to include.

    Returns:
        Human-readable source file summary string.
    """
    counts = Counter(event.source_file for event in events)
    return "; ".join(f"{source} ({hits})" for source, hits in counts.most_common(limit))


def sample_messages(events: List[Event], limit: int = 5) -> str:
    """Collect distinct sample messages from cluster events.

    Args:
        events: Events that belong to one signature cluster.
        limit: Maximum number of distinct messages to include.

    Returns:
        Concatenated sample message string.
    """
    samples: List[str] = []
    seen = set()
    for event in events:
        message = event.message.strip()
        if not message or message in seen:
            continue
        seen.add(message)
        samples.append(message)
        if len(samples) >= limit:
            break
    return " || ".join(samples)


def levels_summary(events: List[Event]) -> str:
    """Summarize event levels found inside a cluster.

    Args:
        events: Events that belong to one signature cluster.

    Returns:
        Human-readable level histogram string.
    """
    counts = Counter((event.level or "UNKNOWN").upper() for event in events)
    return ", ".join(f"{level}:{hits}" for level, hits in counts.most_common())


def min_timestamp(events: List[Event]) -> Optional[datetime]:
    """Return the earliest timestamp present in a list of events.

    Args:
        events: Events to inspect.

    Returns:
        Earliest timestamp, or `None` when timestamps are absent.
    """
    values = [event.timestamp for event in events if event.timestamp]
    return min(values) if values else None


def max_timestamp(events: List[Event]) -> Optional[datetime]:
    """Return the latest timestamp present in a list of events.

    Args:
        events: Events to inspect.

    Returns:
        Latest timestamp, or `None` when timestamps are absent.
    """
    values = [event.timestamp for event in events if event.timestamp]
    return max(values) if values else None


def build_cluster_summaries(events: List[Event]) -> List[ClusterSummary]:
    """Build signature-based cluster summaries from canonical events.

    Args:
        events: Canonical events to group by signature hash.

    Returns:
        Sorted cluster summaries ready for reporting.
    """
    grouped: Dict[str, List[Event]] = defaultdict(list)
    for event in events:
        grouped[event.signature_hash].append(event)

    clusters: List[ClusterSummary] = []
    for signature_hash, cluster_events in grouped.items():
        incident_hits = sum(1 for event in cluster_events if event.is_incident)
        clusters.append(
            ClusterSummary(
                cluster_id=signature_hash,
                hits=len(cluster_events),
                first_seen=min_timestamp(cluster_events),
                last_seen=max_timestamp(cluster_events),
                parser_profiles="; ".join(
                    f"{profile} ({hits})"
                    for profile, hits in Counter(
                        event.parser_profile for event in cluster_events
                    ).most_common(3)
                ),
                source_files=top_source_files(cluster_events),
                sample_messages=sample_messages(cluster_events),
                example_exception=choose_first_non_null(
                    event.exception_type for event in cluster_events
                ),
                levels=levels_summary(cluster_events),
                incident_hits=incident_hits,
                representative_raw=cluster_events[0].raw_text[:1000],
                representative_normalized=cluster_events[0].normalized_message,
                representative_signature_text="",
                top_stack_frames=" | ".join(cluster_events[0].stack_frames),
            )
        )
    clusters.sort(
        key=lambda item: (
            item.incident_hits,
            item.hits,
            item.last_seen or datetime.min,
        ),
        reverse=True,
    )
    return clusters


def top_incident_clusters(clusters: List[ClusterSummary], limit: int = 10) -> List[ClusterSummary]:
    """Return the strongest incident-like clusters for reporting.

    Args:
        clusters: Cluster summaries sorted by severity and size.
        limit: Maximum number of clusters to return.

    Returns:
        Incident-like clusters when present, otherwise the top clusters overall.
    """
    incident_clusters = [cluster for cluster in clusters if cluster.incident_hits > 0]
    if incident_clusters:
        return incident_clusters[:limit]
    return clusters[:limit]


def _pick_representative_event(
    signature_hash: str,
    bucket: Dict[str, object],
    representatives: Dict[str, Event],
) -> Event | None:
    """Return the representative event chosen for one signature bucket."""
    return bucket["representative_event"] or representatives.get(signature_hash)


def _build_cluster_summary(
    signature_hash: str,
    bucket: Dict[str, object],
    representative_event: Event | None,
) -> ClusterSummary:
    """Build one public cluster summary from an internal accumulator bucket."""
    source_counts = bucket["source_counts"]
    level_counts = bucket["level_counts"]
    profile_counts = bucket["profile_counts"]
    hits = bucket["hits"]
    confidence_score = _cluster_confidence_score(bucket)
    return ClusterSummary(
        cluster_id=signature_hash,
        hits=hits,
        first_seen=bucket["first_seen"],
        last_seen=bucket["last_seen"],
        parser_profiles="; ".join(
            f"{profile} ({count})"
            for profile, count in profile_counts.most_common(3)
        ),
        source_files="; ".join(
            f"{source} ({hits})"
            for source, hits in source_counts.most_common(5)
        ),
        sample_messages=" || ".join(bucket["sample_messages"]),
        example_exception=bucket["example_exception"],
        levels=", ".join(
            f"{level}:{hits}" for level, hits in level_counts.most_common()
        ),
        incident_hits=bucket["incident_hits"],
        confidence_score=confidence_score,
        confidence_label=confidence_label(confidence_score),
        clustering_method="signature",
        representative_raw=(
            representative_event.raw_text[:2000] if representative_event else ""
        ),
        representative_normalized=(
            representative_event.normalized_message if representative_event else ""
        ),
        representative_signature_text=(
            build_signature_text(
                representative_event.normalized_message,
                representative_event.exception_type,
                representative_event.stack_frames,
            )
            if representative_event
            else ""
        ),
        top_stack_frames=(
            " | ".join(representative_event.stack_frames)
            if representative_event
            else ""
        ),
    )


class ClusterAccumulator:
    """Incrementally accumulate events into signature-based cluster statistics."""

    def __init__(self) -> None:
        self._clusters: Dict[str, Dict[str, object]] = {}
        self._representatives: Dict[str, Event] = {}

    def add(self, event: Event) -> None:
        """Add one canonical event to the accumulator.

        Args:
            event: Canonical event to aggregate.

        Returns:
            None.
        """
        bucket = self._clusters.setdefault(
            event.signature_hash,
            {
                "hits": 0,
                "first_seen": None,
                "last_seen": None,
                "source_counts": Counter(),
                "sample_messages": [],
                "sample_seen": set(),
                "example_exception": None,
                "level_counts": Counter(),
                "incident_hits": 0,
                "timestamp_count": 0,
                "component_count": 0,
                "exception_count": 0,
                "stacktrace_count": 0,
                "profile_counts": Counter(),
                "representative_event": None,
            },
        )
        bucket["hits"] += 1
        if event.timestamp:
            bucket["timestamp_count"] += 1
            first_seen = bucket["first_seen"]
            last_seen = bucket["last_seen"]
            bucket["first_seen"] = (
                event.timestamp
                if first_seen is None or event.timestamp < first_seen
                else first_seen
            )
            bucket["last_seen"] = (
                event.timestamp
                if last_seen is None or event.timestamp > last_seen
                else last_seen
            )
        bucket["source_counts"][event.source_file] += 1
        bucket["level_counts"][(event.level or "UNKNOWN").upper()] += 1
        bucket["profile_counts"][event.parser_profile] += 1
        if event.component:
            bucket["component_count"] += 1
        if event.is_incident:
            bucket["incident_hits"] += 1
        if not bucket["example_exception"] and event.exception_type:
            bucket["example_exception"] = event.exception_type
        if event.exception_type:
            bucket["exception_count"] += 1
        if event.stacktrace.strip():
            bucket["stacktrace_count"] += 1
        if event.message.strip() and event.message not in bucket["sample_seen"]:
            bucket["sample_seen"].add(event.message)
            if len(bucket["sample_messages"]) < 5:
                bucket["sample_messages"].append(event.message)

        representative = self._representatives.get(event.signature_hash)
        if representative is None:
            self._representatives[event.signature_hash] = event
            bucket["representative_event"] = event
        elif event.is_incident and not representative.is_incident:
            self._representatives[event.signature_hash] = event
            bucket["representative_event"] = event
        elif len(event.stacktrace) > len(representative.stacktrace):
            self._representatives[event.signature_hash] = event
            bucket["representative_event"] = event

    def representatives(self) -> List[Event]:
        """Return representative events for each collected signature hash.

        Returns:
            Representative events selected during accumulation.
        """
        return list(self._representatives.values())

    def build_summaries(self) -> List[ClusterSummary]:
        """Convert accumulated cluster state into sorted summaries.

        Returns:
            Cluster summaries ready for downstream reporting.
        """
        clusters: List[ClusterSummary] = []
        for signature_hash, bucket in self._clusters.items():
            representative_event = _pick_representative_event(
                signature_hash=signature_hash,
                bucket=bucket,
                representatives=self._representatives,
            )
            clusters.append(
                _build_cluster_summary(
                    signature_hash=signature_hash,
                    bucket=bucket,
                    representative_event=representative_event,
                )
            )
        clusters.sort(
            key=lambda item: (
                item.incident_hits,
                item.hits,
                item.last_seen or datetime.min,
            ),
            reverse=True,
        )
        return clusters


def _cluster_confidence_score(bucket: Dict[str, object]) -> float:
    """Compute a heuristic confidence score for one cluster bucket.

    Args:
        bucket: Internal cluster accumulator bucket.

    Returns:
        Rounded confidence score in the `[0, 1]` range.
    """
    hits = int(bucket["hits"])
    if hits <= 0:
        return 0.0
    incident_ratio = int(bucket["incident_hits"]) / hits
    exception_ratio = int(bucket["exception_count"]) / hits
    stacktrace_ratio = int(bucket["stacktrace_count"]) / hits
    timestamp_ratio = int(bucket["timestamp_count"]) / hits
    component_ratio = int(bucket["component_count"]) / hits
    hit_score = min(1.0, hits / 20.0)
    score = (
        0.3 * incident_ratio
        + 0.25 * exception_ratio
        + 0.15 * stacktrace_ratio
        + 0.15 * timestamp_ratio
        + 0.05 * component_ratio
        + 0.1 * hit_score
    )
    return round(score, 3)
