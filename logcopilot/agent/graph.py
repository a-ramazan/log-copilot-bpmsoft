from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from .session import AgentSessionState
from .tools import execute_plan, resolve_run_context, route_question
from PIL import Image
import io

class AgentState(TypedDict):
    question: str
    run_id: str
    db_path: str
    profile: str
    run_summary: dict
    plan: dict
    facts: dict
    memory: dict
    trace: list[str]


def build_agent_graph():
    def bootstrap_node(state: AgentState) -> AgentState:
        context = resolve_run_context(state["run_id"], db_path=state["db_path"])
        trace = list(state.get("trace", []))
        trace.append(f"bootstrap: profile={context.profile}")
        return {
            **state,
            "profile": context.profile,
            "run_summary": context.run_summary,
            "trace": trace,
        }

    def plan_node(state: AgentState) -> AgentState:
        plan = route_question(
            profile=state["profile"],
            question=state["question"],
            memory_payload=state.get("memory", {}),
        )
        trace = list(state.get("trace", []))
        trace.append(f"plan: {plan['action']} {plan.get('args', {})}")
        return {
            **state,
            "plan": plan,
            "trace": trace,
        }

    def fetch_node(state: AgentState) -> AgentState:
        context = resolve_run_context(state["run_id"], db_path=state["db_path"])
        facts = execute_plan(context, state["plan"])
        trace = list(state.get("trace", []))
        trace.append(f"fetch: action={state['plan'].get('action')}")
        return {
            **state,
            "facts": facts,
            "trace": trace,
        }

    def remember_node(state: AgentState) -> AgentState:
        memory = AgentSessionState.from_dict(state.get("memory", {}))
        action = state.get("plan", {}).get("action")
        facts = state.get("facts", {})

        memory.turn_index += 1
        memory.last_action = action

        if action == "top_incidents":
            rows = facts.get("top_incidents", [])
            cluster_ids = [
                row.get("cluster_id")
                for row in rows
                if isinstance(row, dict) and row.get("cluster_id")
            ]
            if cluster_ids:
                memory.last_top_cluster_ids = cluster_ids
                memory.focused_cluster_id = cluster_ids[0]

        if action == "incident_cluster":
            row = facts.get("incident_cluster")
            if isinstance(row, dict) and row.get("cluster_id"):
                memory.focused_cluster_id = row["cluster_id"]

        trace = list(state.get("trace", []))
        trace.append(f"remember: focused={memory.focused_cluster_id or 'none'}")
        return {
            **state,
            "memory": memory.to_dict(),
            "trace": trace,
        }

    graph = StateGraph(AgentState)
    graph.add_node("bootstrap", bootstrap_node)
    graph.add_node("plan", plan_node)
    graph.add_node("fetch", fetch_node)
    graph.add_node("remember", remember_node)
    graph.add_edge(START, "bootstrap")
    graph.add_edge("bootstrap", "plan")
    graph.add_edge("plan", "fetch")
    graph.add_edge("fetch", "remember")
    graph.add_edge("remember", END)
    app = graph.compile()

    png_bytes = app.get_graph().draw_mermaid_png()

    png_bytes = app.get_graph().draw_mermaid_png()
    img = Image.open(io.BytesIO(png_bytes))
    img.show()  # Откроет стандартный просмотрщик картинок вашей ОС

    return app