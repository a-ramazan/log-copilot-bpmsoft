import csv
from collections import defaultdict
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .models import Event, SemanticClusterSummary
from .reporting import write_semantic_clusters_csv


def build_representative_text(event: Event) -> str:
    return event.embedding_text or event.normalized_message or event.raw_text


def _require_semantic_dependencies():
    try:
        from sentence_transformers import SentenceTransformer  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Semantic clustering requires sentence-transformers. Install with: "
            "pip install -r requirements.txt"
        ) from exc
    try:
        from sklearn.cluster import DBSCAN  # noqa: F401
        from sklearn.metrics.pairwise import cosine_similarity  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Semantic clustering requires scikit-learn. Install with: "
            "pip install -r requirements.txt"
        ) from exc


def cluster_signatures_semantically(
    events: Iterable[Event],
    enabled: str,
    model_name: str,
    min_cluster_size: int,
    min_samples: Optional[int] = None,
) -> Tuple[List[SemanticClusterSummary], str]:
    if enabled == "off":
        return [], "disabled"

    _require_semantic_dependencies()

    from sentence_transformers import SentenceTransformer
    from sklearn.cluster import DBSCAN
    from sklearn.metrics.pairwise import cosine_similarity

    representatives = {}
    hits_by_signature = defaultdict(int)
    for event in events:
        hits_by_signature[event.signature_hash] += 1
        current = representatives.get(event.signature_hash)
        if current is None or (event.is_incident and not current.is_incident):
            representatives[event.signature_hash] = event

    signature_items = list(representatives.items())
    if len(signature_items) < 2:
        return [], "skipped: not enough signature clusters"

    texts = [build_representative_text(event) for _, event in signature_items]
    try:
        model = SentenceTransformer(model_name)
        embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load or run embedding model '{model_name}'. "
            "Ensure the model is downloadable or already cached."
        ) from exc

    labels = None
    method_note = ""
    effective_min_samples = min_samples if min_samples is not None else max(2, min_cluster_size // 2)
    try:
        import hdbscan

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=max(2, min_cluster_size),
            min_samples=max(1, effective_min_samples),
            metric="euclidean",
        )
        labels = clusterer.fit_predict(embeddings)
        method_note = "HDBSCAN"
    except ImportError:
        clusterer = DBSCAN(
            metric="cosine",
            eps=0.2,
            min_samples=max(2, effective_min_samples),
        )
        labels = clusterer.fit_predict(embeddings)
        method_note = "DBSCAN"

    grouped = defaultdict(list)
    for index, ((signature_hash, event), label) in enumerate(zip(signature_items, labels)):
        grouped[int(label)].append((index, signature_hash, event))

    summaries: List[SemanticClusterSummary] = []
    semantic_cluster_id = 0
    for label, members in grouped.items():
        if label == -1:
            continue
        total_hits = sum(hits_by_signature[signature_hash] for _, signature_hash, _ in members)
        representative_index, representative_signature, representative_event = max(
            members, key=lambda item: hits_by_signature[item[1]]
        )
        cluster_vectors = [embeddings[index] for index, _, _ in members]
        centroid = sum(cluster_vectors) / len(cluster_vectors)
        similarities = cosine_similarity(cluster_vectors, [centroid]).ravel()
        summaries.append(
            SemanticClusterSummary(
                semantic_cluster_id=semantic_cluster_id,
                signature_hash=representative_signature,
                hits=total_hits,
                representative_text=build_representative_text(representative_event),
                member_signature_hashes=" | ".join(signature_hash for _, signature_hash, _ in members),
                avg_cosine_similarity=float(similarities.mean()),
            )
        )
        semantic_cluster_id += 1

    summaries.sort(key=lambda item: item.hits, reverse=True)
    if not summaries:
        return [], f"{method_note}: no dense semantic groups found"
    return summaries, f"{method_note}: {len(summaries)} semantic clusters"


def load_representative_events_from_csv(events_csv_path: Path) -> List[Event]:
    events: List[Event] = []
    seen = set()
    with events_csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            signature_hash = row["signature_hash"]
            if signature_hash in seen:
                continue
            seen.add(signature_hash)
            stack_frames = [
                part.strip() for part in row.get("stack_frames", "").split("|") if part.strip()
            ]
            events.append(
                Event(
                    event_id=row["event_id"],
                    source_file=row["source_file"],
                    parser_profile=row["parser_profile"] or "unknown",
                    timestamp=None,
                    level=row["level"] or None,
                    message=row["message"],
                    stacktrace=row["stacktrace"],
                    raw_text=row["raw_text"],
                    line_count=int(row["line_count"]) if row.get("line_count") else 1,
                    normalized_message=row["normalized_message"],
                    signature_hash=signature_hash,
                    embedding_text=row["embedding_text"],
                    exception_type=row["exception_type"] or None,
                    stack_frames=stack_frames,
                    component=row["component"] or None,
                    request_id=row["request_id"] or None,
                    trace_id=row["trace_id"] or None,
                    http_status=int(row["http_status"]) if row["http_status"] else None,
                    is_incident=row["is_incident"] == "true",
                )
            )
    return events


def rerun_semantic_clustering_from_events_csv(
    events_csv_path: Path,
    output_csv_path: Path,
    model_name: str,
    min_cluster_size: int,
    min_samples: Optional[int] = None,
) -> Tuple[List[SemanticClusterSummary], str]:
    events = load_representative_events_from_csv(events_csv_path)
    clusters, note = cluster_signatures_semantically(
        events=events,
        enabled="on",
        model_name=model_name,
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
    )
    write_semantic_clusters_csv(output_csv_path, clusters)
    return clusters, note
