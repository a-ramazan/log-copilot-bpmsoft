from __future__ import annotations

"""Incident profile: signature clustering, semantic grouping and report writers."""

from dataclasses import asdict
import logging
from pathlib import Path
from typing import Callable, List, Optional

from ..analysis import (
    AnalysisQualityAccumulator,
    ClusterAccumulator,
    cluster_signatures_semantically,
    top_incident_clusters,
)
from ..domain import AnalysisSummary, Event
from ..output import (
    write_analysis_summary_json,
    write_clusters_csv,
    write_llm_ready_clusters_json,
    write_semantic_clusters_csv,
    write_top_clusters_md,
)

logger = logging.getLogger(__name__)


def build_quality_summary(events: List[Event], source_name: str, cluster_count: int) -> AnalysisSummary:
    """Build the quality summary for the incidents profile.

    Args:
        events: Parsed canonical events for the current run.
        source_name: Source file name shown in the summary.
        cluster_count: Number of signature clusters produced for the run.

    Returns:
        Aggregated quality summary for the incidents profile.
    """
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
    semantic_cache_dir: Optional[Path] = None,
    semantic_max_signatures: int = 2500,
    progress_callback: Callable[[str], None] | None = None,
) -> dict:
    """Run the incidents profile and persist its report artifacts.

    Args:
        events: Parsed canonical events for the run.
        output_dir: Directory where incidents artifacts should be written.
        source_name: Display name for the source log file.
        semantic: Semantic clustering mode.
        semantic_model: Sentence-transformer model name for embeddings.
        semantic_min_cluster_size: Minimum size for a semantic cluster.
        semantic_min_samples: Minimum density threshold for semantic clustering.
        semantic_cache_dir: Optional cache directory for embedding vectors.
        semantic_max_signatures: Maximum number of representative signatures to embed.
        progress_callback: Optional callback for progress messages.

    Returns:
        Profile payload with clusters, summaries, artifacts and compact metadata.
    """
    accumulator = ClusterAccumulator()
    for event in events:
        accumulator.add(event)

    clusters = accumulator.build_summaries()
    top_clusters = top_incident_clusters(clusters, limit=10)
    logger.info(
        "incidents_clusters_built: events=%d clusters=%d top_clusters=%d",
        len(events),
        len(clusters),
        len(top_clusters),
    )
    analysis_summary = build_quality_summary(events, source_name=source_name, cluster_count=len(clusters))
    semantic_clusters, semantic_note = cluster_signatures_semantically(
        events=accumulator.representatives(),
        enabled=semantic,
        model_name=semantic_model,
        min_cluster_size=semantic_min_cluster_size,
        min_samples=semantic_min_samples,
        cache_dir=semantic_cache_dir,
        max_signatures=semantic_max_signatures,
        progress_callback=progress_callback,
    )
    logger.info(
        "incidents_semantic_stage: semantic_clusters=%d note=%s",
        len(semantic_clusters),
        semantic_note,
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
    logger.info(
        "incidents_artifacts_written: clusters=%s semantic=%s top=%s llm=%s analysis=%s",
        str(clusters_path),
        str(semantic_path),
        str(top_incidents_path),
        str(llm_path),
        str(analysis_path),
    )

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
            "analysis_summary": asdict(analysis_summary),
        },
    }
