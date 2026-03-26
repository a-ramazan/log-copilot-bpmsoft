from __future__ import annotations


def build_agent(*args, **kwargs):
    try:
        from langchain_core.tools import tool
        from langgraph.graph import END, StateGraph
    except ImportError as exc:
        raise RuntimeError(
            "Agent orchestration requires langchain/langgraph. "
            "Install optional agent dependencies before using logcopilot.agent."
        ) from exc

    @tool
    def ping() -> str:
        """Sanity-check tool for the future LangGraph workflow."""
        return "logcopilot-agent-ready"

    graph = StateGraph(dict)
    graph.add_node("ping", lambda state: {"message": ping.invoke({})})
    graph.set_entry_point("ping")
    graph.add_edge("ping", END)
    return graph.compile()
