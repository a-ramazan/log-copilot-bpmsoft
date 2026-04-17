from __future__ import annotations

from .facts import (
    RunContext,
    allowed_actions_for_profile,
    build_fact_catalog,
    execute_fact_queries,
    fetch_event_field_stats,
    fetch_heatmap,
    fetch_incident_cluster,
    fetch_run_summary,
    fetch_top_incidents,
    fetch_traffic_anomalies,
    fetch_traffic_summary,
    open_artifact,
    resolve_run_context,
)
from .planner import plan_intent


# Backwards-compatible aliases for tests and older imports.
get_run_summary = fetch_run_summary
get_top_incidents = fetch_top_incidents
find_incident_cluster = fetch_incident_cluster
get_heatmap = fetch_heatmap
get_traffic_summary = fetch_traffic_summary
get_traffic_anomalies = fetch_traffic_anomalies
route_question = plan_intent
execute_plan = execute_fact_queries

__all__ = [
    "RunContext",
    "allowed_actions_for_profile",
    "build_fact_catalog",
    "execute_fact_queries",
    "execute_plan",
    "fetch_event_field_stats",
    "fetch_heatmap",
    "fetch_incident_cluster",
    "fetch_run_summary",
    "fetch_top_incidents",
    "fetch_traffic_anomalies",
    "fetch_traffic_summary",
    "find_incident_cluster",
    "get_heatmap",
    "get_run_summary",
    "get_top_incidents",
    "get_traffic_anomalies",
    "get_traffic_summary",
    "open_artifact",
    "plan_intent",
    "resolve_run_context",
    "route_question",
]
