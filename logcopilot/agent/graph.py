from __future__ import annotations

import logging
from typing import TypedDict

from .facts import build_fact_catalog, execute_fact_queries, resolve_run_context
from .planner import plan_intent
from .session import AgentSessionState
from .visuals import build_visuals_for_plan

logger = logging.getLogger(__name__)


class AgentState(TypedDict):
    question: str
    run_id: str
    db_path: str
    profile: str
    run_summary: dict
    plan: dict
    facts: dict
    fact_catalog: dict
    memory: dict
    trace: list[str]
    visuals: list[dict]


def _remember(plan: dict, facts: dict, memory_payload: dict | None, profile: str) -> dict:
    """Обновляет минимальное состояние диалога для follow-up вопросов."""
    memory = AgentSessionState.from_dict(memory_payload)
    action = plan.get("action")

    memory.turn_index += 1
    memory.last_action = action
    memory.last_profile_scope = profile

    if action == "top_incidents":
        rows = facts.get("top_incidents", [])
        cluster_ids = [
            row.get("cluster_id")
            for row in rows
            if isinstance(row, dict) and row.get("cluster_id")
        ]
        if cluster_ids:
            memory.last_ranked_cluster_ids = cluster_ids
            memory.last_top_cluster_ids = cluster_ids
            memory.focused_cluster_id = cluster_ids[0]
            memory.last_topic = "incident_ranking"

    if action in {"incident_cluster_detail", "incident_cluster_examples", "incident_cluster_causes"}:
        row = facts.get("incident_cluster")
        if isinstance(row, dict) and row.get("cluster_id"):
            memory.focused_cluster_id = row["cluster_id"]
            memory.last_topic = "incident_cluster"

    if action in {"parser_diagnostics_summary", "profile_fit_summary"}:
        memory.last_topic = action

    return memory.to_dict()


def run_agent_pipeline(
    question: str,
    run_id: str,
    db_path: str,
    session_state: dict | None = None,
    planner_model=None,
) -> AgentState:
    """Agent-flow поверх обработанных фактов: context -> plan -> fact-query -> visuals."""
    trace: list[str] = []
    logger.info("agent_flow_started: run_id=%s", run_id)

    context = resolve_run_context(run_id, db_path=db_path)
    trace.append(f"resolve_run: profile={context.profile}")
    logger.info("agent_resolve_run: run_id=%s profile=%s", run_id, context.profile)

    fact_catalog = build_fact_catalog(context)
    trace.append(f"build_fact_catalog: actions={fact_catalog.get('available_actions')}")
    logger.debug(
        "agent_fact_catalog: run_id=%s actions=%s",
        run_id,
        fact_catalog.get("available_actions"),
    )

    plan = plan_intent(
        context = context,
        question = question,
        fact_catalog=fact_catalog,
        memory_payload=session_state or {},
        planner_model=planner_model,
    )
    trace.append(f"route_question: action={plan['action']} args={plan.get('args', {})}")
    logger.info(
        "agent_route_selected: run_id=%s action=%s args=%s",
        run_id,
        plan.get("action"),
        plan.get("args", {}),
    )

    facts = execute_fact_queries(context, plan)
    trace.append(f"fetch_facts: keys={sorted(facts.keys())}")
    logger.info(
        "agent_fact_query_done: run_id=%s action=%s fact_keys=%s",
        run_id,
        plan.get("action"),
        sorted(facts.keys()),
    )

    visuals, visual_info = build_visuals_for_plan(context, plan, facts)
    facts = {**facts, "visual_info": visual_info}
    trace.append(f"build_visual: status={visual_info.get('status')} requested={visual_info.get('requested')}")
    logger.info(
        "agent_visual_stage: run_id=%s requested=%s status=%s",
        run_id,
        visual_info.get("requested"),
        visual_info.get("status"),
    )

    memory = _remember(plan, facts, session_state, profile=context.profile)
    trace.append(f"update_session: focused={memory.get('focused_cluster_id') or 'none'}")
    logger.info(
        "agent_flow_completed: run_id=%s action=%s focused_cluster=%s",
        run_id,
        plan.get("action"),
        memory.get("focused_cluster_id"),
    )

    return {
        "question": question,
        "run_id": run_id,
        "db_path": db_path,
        "profile": context.profile,
        "run_summary": context.run_summary,
        "plan": plan,
        "facts": facts,
        "fact_catalog": fact_catalog,
        "memory": memory,
        "trace": trace,
        "visuals": visuals,
    }
