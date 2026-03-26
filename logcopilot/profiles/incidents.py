from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from ..clustering import ClusterAccumulator, top_incident_clusters
from ..models import AnalysisSummary, Event
from ..quality import AnalysisQualityAccumulator
from ..reporting import (
    write_analysis_summary_json,
    write_clusters_csv,
    write_llm_ready_clusters_json,
    write_semantic_clusters_csv,
    write_top_clusters_md,
)
from ..semantic import cluster_signatures_semantically


def build_quality_summary(events: List[Event], source_name: str, cluster_count: int) -> AnalysisSummary:
    quality = AnalysisQualityAccumulator(source_name=source_name)
    for event in events:
        quality.add(event)
    return quality.build_summary(cluster_count=cluster_count)


def run_incidents_profile(
    events: List[Event],
    output_dir: Path,
    source_name: str,
    semantic: str = "on",
    semantic_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    semantic_min_cluster_size: int = 3,
    semantic_min_samples: Optional[int] = None,
) -> dict:
    accumulator = ClusterAccumulator()
    for event in events:
        accumulator.add(event)

    clusters = accumulator.build_summaries()
    top_clusters = top_incident_clusters(clusters, limit=10)
    analysis_summary = build_quality_summary(events, source_name=source_name, cluster_count=len(clusters))
    semantic_clusters, semantic_note = cluster_signatures_semantically(
        events=accumulator.representatives(),
        enabled=semantic,
        model_name=semantic_model,
        min_cluster_size=semantic_min_cluster_size,
        min_samples=semantic_min_samples,
    )

    clusters_path = output_dir / "clusters.csv"
    semantic_path = output_dir / "semantic_clusters.csv"
    top_incidents_path = output_dir / "top_incidents.md"
    llm_path = output_dir / "llm_ready_clusters.json"
    analysis_path = output_dir / "analysis_summary.json"

    write_clusters_csv(clusters_path, clusters)
    write_semantic_clusters_csv(semantic_path, semantic_clusters)
    write_llm_ready_clusters_json(llm_path, top_clusters)
    write_top_clusters_md(
        top_incidents_path,
        top_clusters,
        event_count=len(events),
        cluster_count=len(clusters),
        analysis_summary=analysis_summary,
        semantic_note=semantic_note,
    )
    write_analysis_summary_json(analysis_path, analysis_summary)

    return {
        "clusters": clusters,
        "semantic_clusters": semantic_clusters,
        "analysis_summary": analysis_summary,
        "semantic_note": semantic_note,
        "artifact_paths": {
            "clusters_csv": str(clusters_path),
            "semantic_clusters_csv": str(semantic_path),
            "top_incidents_md": str(top_incidents_path),
            "llm_ready_clusters_json": str(llm_path),
            "analysis_summary_json": str(analysis_path),
        },
        "summary": {
            "cluster_count": len(clusters),
            "semantic_cluster_count": len(semantic_clusters),
            "incident_event_count": analysis_summary.incident_event_count,
            "semantic_note": semantic_note,
            "analysis_summary": {
                "source_name": analysis_summary.source_name,
                "event_count": analysis_summary.event_count,
                "cluster_count": analysis_summary.cluster_count,
                "incident_event_count": analysis_summary.incident_event_count,
                "timestamp_coverage": analysis_summary.timestamp_coverage,
                "level_coverage": analysis_summary.level_coverage,
                "component_coverage": analysis_summary.component_coverage,
                "exception_coverage": analysis_summary.exception_coverage,
                "stacktrace_coverage": analysis_summary.stacktrace_coverage,
                "request_id_coverage": analysis_summary.request_id_coverage,
                "trace_id_coverage": analysis_summary.trace_id_coverage,
                "fallback_profile_rate": analysis_summary.fallback_profile_rate,
                "parser_quality_score": analysis_summary.parser_quality_score,
                "parser_quality_label": analysis_summary.parser_quality_label,
                "parser_profiles": analysis_summary.parser_profiles,
            },
        },
    }
