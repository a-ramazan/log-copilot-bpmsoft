from .tools import (
    find_incident_cluster,
    get_heatmap,
    get_run_summary,
    get_top_incidents,
    get_traffic_anomalies,
    get_traffic_summary,
    open_artifact,
)


def answer_question(*args, **kwargs):
    from .chat import answer_question as _answer_question

    return _answer_question(*args, **kwargs)

__all__ = [
    "answer_question",
    "find_incident_cluster",
    "get_heatmap",
    "get_run_summary",
    "get_top_incidents",
    "get_traffic_anomalies",
    "get_traffic_summary",
    "open_artifact",
]
