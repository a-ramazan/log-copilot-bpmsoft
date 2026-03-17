from collections import defaultdict
from typing import Iterable, List, Optional, Tuple

from .models import Event, SemanticClusterSummary


def build_representative_text(event: Event) -> str:
    return event.embedding_text or event.normalized_message or event.raw_text


def cluster_signatures_semantically(
    events: Iterable[Event],
    enabled: str,
    model_name: str,
    min_cluster_size: int,
) -> Tuple[List[SemanticClusterSummary], str]:
    if enabled == "off":
        return [], "disabled"

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return [], "skipped: sentence-transformers is not installed"

    try:
        import numpy as np
    except ImportError:
        return [], "skipped: numpy is not installed"

    representatives = {}
    hits_by_signature = defaultdict(int)
    for event in events:
        hits_by_signature[event.signature_hash] += 1
        if event.signature_hash not in representatives and event.is_incident:
            representatives[event.signature_hash] = event
        elif event.signature_hash not in representatives:
            representatives[event.signature_hash] = event

    signature_items = list(representatives.items())
    if len(signature_items) < 2:
        return [], "skipped: not enough signature clusters"

    texts = [build_representative_text(event) for _, event in signature_items]
    try:
        model = SentenceTransformer(model_name)
        embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
    except Exception as exc:
        return [], f"skipped: model load/encode failed ({exc})"

    labels = None
    try:
        import hdbscan

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=max(2, min_cluster_size),
            metric="euclidean",
        )
        labels = clusterer.fit_predict(embeddings)
        method_note = "HDBSCAN"
    except ImportError:
        try:
            from sklearn.cluster import DBSCAN
        except ImportError:
            return [], "skipped: hdbscan and scikit-learn are not installed"
        clusterer = DBSCAN(metric="cosine", eps=0.2, min_samples=max(2, min_cluster_size // 2))
        labels = clusterer.fit_predict(embeddings)
        method_note = "DBSCAN"
    except Exception as exc:
        return [], f"skipped: semantic clustering failed ({exc})"

    grouped = defaultdict(list)
    for (signature_hash, event), label in zip(signature_items, labels):
        grouped[int(label)].append((signature_hash, event))

    summaries: List[SemanticClusterSummary] = []
    semantic_cluster_id = 0
    for label, members in grouped.items():
        if label == -1:
            continue
        total_hits = sum(hits_by_signature[signature_hash] for signature_hash, _ in members)
        representative_signature, representative_event = max(
            members, key=lambda pair: hits_by_signature[pair[0]]
        )
        summaries.append(
            SemanticClusterSummary(
                semantic_cluster_id=semantic_cluster_id,
                signature_hash=representative_signature,
                hits=total_hits,
                representative_text=build_representative_text(representative_event),
            )
        )
        semantic_cluster_id += 1

    summaries.sort(key=lambda item: item.hits, reverse=True)
    if not summaries:
        return [], f"{method_note}: no dense semantic groups found"
    return summaries, f"{method_note}: {len(summaries)} semantic clusters"
