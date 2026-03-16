from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from .models import ClusterSummary, Event


def choose_first_non_null(values: Iterable[Optional[str]]) -> Optional[str]:
    for value in values:
        if value:
            return value
    return None


def top_source_files(events: List[Event], limit: int = 5) -> str:
    counts = Counter(event.source_file for event in events)
    return "; ".join(f"{source} ({hits})" for source, hits in counts.most_common(limit))


def sample_messages(events: List[Event], limit: int = 5) -> str:
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
    counts = Counter((event.level or "UNKNOWN").upper() for event in events)
    return ", ".join(f"{level}:{hits}" for level, hits in counts.most_common())


def min_timestamp(events: List[Event]) -> Optional[datetime]:
    values = [event.timestamp for event in events if event.timestamp]
    return min(values) if values else None


def max_timestamp(events: List[Event]) -> Optional[datetime]:
    values = [event.timestamp for event in events if event.timestamp]
    return max(values) if values else None


def build_cluster_summaries(events: List[Event]) -> List[ClusterSummary]:
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
                source_files=top_source_files(cluster_events),
                sample_messages=sample_messages(cluster_events),
                example_exception=choose_first_non_null(
                    event.exception_type for event in cluster_events
                ),
                levels=levels_summary(cluster_events),
                incident_hits=incident_hits,
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
    incident_clusters = [cluster for cluster in clusters if cluster.incident_hits > 0]
    if incident_clusters:
        return incident_clusters[:limit]
    return clusters[:limit]


class ClusterAccumulator:
    def __init__(self) -> None:
        self._clusters: Dict[str, Dict[str, object]] = {}
        self._representatives: Dict[str, Event] = {}

    def add(self, event: Event) -> None:
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
            },
        )
        bucket["hits"] += 1
        if event.timestamp:
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
        if event.is_incident:
            bucket["incident_hits"] += 1
        if not bucket["example_exception"] and event.exception_type:
            bucket["example_exception"] = event.exception_type
        if event.message.strip() and event.message not in bucket["sample_seen"]:
            bucket["sample_seen"].add(event.message)
            if len(bucket["sample_messages"]) < 5:
                bucket["sample_messages"].append(event.message)

        representative = self._representatives.get(event.signature_hash)
        if representative is None:
            self._representatives[event.signature_hash] = event
        elif event.is_incident and not representative.is_incident:
            self._representatives[event.signature_hash] = event
        elif len(event.stacktrace) > len(representative.stacktrace):
            self._representatives[event.signature_hash] = event

    def representatives(self) -> List[Event]:
        return list(self._representatives.values())

    def build_summaries(self) -> List[ClusterSummary]:
        clusters: List[ClusterSummary] = []
        for signature_hash, bucket in self._clusters.items():
            source_counts = bucket["source_counts"]
            level_counts = bucket["level_counts"]
            clusters.append(
                ClusterSummary(
                    cluster_id=signature_hash,
                    hits=bucket["hits"],
                    first_seen=bucket["first_seen"],
                    last_seen=bucket["last_seen"],
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
