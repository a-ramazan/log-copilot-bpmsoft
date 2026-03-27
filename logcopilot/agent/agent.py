from __future__ import annotations

import json
from typing import Any

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage, ToolMessage

from .tools import (
    get_run_summary,
    get_top_incidents,
    find_incident_cluster,
    get_heatmap,
    get_traffic_summary,
    get_traffic_anomalies,
)

SYSTEM_PROMPT = """
Ты LogCopilot.
Отвечай кратко, по-русски и по делу.

Ты анализируешь только уже обработанные логи конкретного run_id.
Используй tools, когда реально нужны данные.
Не выдумывай факты, числа или метрики.
Если данных недостаточно, так и скажи.

ВАЖНЫЕ ПРАВИЛА:
1. Не вызывай один и тот же tool повторно с теми же аргументами, если уже получил результат.
2. После получения результата tool постарайся сразу дать финальный ответ.
3. Для простых вопросов старайся делать не более 1-2 tool calls.
4. Если пользователь просит "что интересного", кратко суммаризируй то, что вернул tool.
5. Не зацикливайся на повторных вызовах tools.
"""

MAX_TOOL_CALLS = 4


def _sanitize_text(text: str) -> str:
    return text.encode("utf-8", "replace").decode("utf-8")


def _safe_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str)


def _compact_run_summary(data: dict | None) -> dict | str:
    if not data:
        return "Run summary не найден."

    return {
        "run_id": data.get("run_id"),
        "input_path": data.get("input_path"),
        "events_total": data.get("events_total"),
        "clusters_total": data.get("clusters_total"),
        "incidents_total": data.get("incidents_total"),
        "artifacts": data.get("artifacts", [])[:5],
    }


def build_chat_model(
    model: str = "qwen/qwen3.5-9b",
    base_url: str = "http://127.0.0.1:1234/v1",
    api_key: str = "lm-studio",
    temperature: float = 0.0,
):
    return ChatOpenAI(
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )


def build_tool_registry(run_id: str, db_path: str) -> dict[str, Any]:
    def run_summary() -> dict | str:
        data = get_run_summary(run_id, db_path=db_path)
        return _compact_run_summary(data)

    def top_incidents(limit: int = 10) -> list[dict] | str:
        return get_top_incidents(run_id, limit=limit, db_path=db_path)

    def incident_cluster(cluster_id: str) -> dict | str:
        row = find_incident_cluster(run_id, cluster_id, db_path=db_path)
        return row or f"Кластер {cluster_id} не найден."

    def heatmap(limit: int = 20) -> list[dict] | str:
        return get_heatmap(run_id, limit=limit, db_path=db_path)

    def traffic_summary(status: int | None = None, limit: int = 20) -> list[dict] | str:
        return get_traffic_summary(run_id, status=status, limit=limit, db_path=db_path)

    def traffic_anomalies(limit: int = 20) -> list[dict] | str:
        return get_traffic_anomalies(run_id, limit=limit, db_path=db_path)

    return {
        "run_summary": run_summary,
        "top_incidents": top_incidents,
        "incident_cluster": incident_cluster,
        "heatmap": heatmap,
        "traffic_summary": traffic_summary,
        "traffic_anomalies": traffic_anomalies,
    }


def build_tools_schema() -> list[dict]:
    return [
        {
            "type": "function",
            "function": {
                "name": "run_summary",
                "description": "Get high-level summary of the current processed log run.",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "top_incidents",
                "description": "Get top incident clusters for the current run.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 10},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "incident_cluster",
                "description": "Get detailed information about one incident cluster by cluster_id.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "cluster_id": {"type": "string"},
                    },
                    "required": ["cluster_id"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "heatmap",
                "description": "Get top hotspots from the current run heatmap.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 20},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "traffic_summary",
                "description": "Get endpoint traffic summary, optionally filtered by HTTP status.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "status": {"type": ["integer", "null"], "default": None},
                        "limit": {"type": "integer", "default": 20},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "traffic_anomalies",
                "description": "Get suspicious traffic anomalies for the current run.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 20},
                    },
                },
            },
        },
    ]


def _parse_tool_args(raw_args: Any) -> dict:
    if raw_args is None:
        return {}

    if isinstance(raw_args, dict):
        return raw_args

    if isinstance(raw_args, str):
        raw_args = raw_args.strip()
        if not raw_args:
            return {}
        try:
            return json.loads(raw_args)
        except json.JSONDecodeError:
            return {}

    return {}


def _make_call_signature(tool_name: str, args: dict) -> str:
    return f"{tool_name}:{json.dumps(args, sort_keys=True, ensure_ascii=False)}"


def ask_agent(
    question: str,
    run_id: str,
    db_path: str = "out/logcopilot.sqlite",
    model: str = "qwen/qwen3.5-9b",
    base_url: str = "http://127.0.0.1:1234/v1",
    api_key: str = "lm-studio",
    temperature: float = 0.0,
    debug: bool = False,
) -> str:
    llm = build_chat_model(
        model=model,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )

    tools_schema = build_tools_schema()
    tool_registry = build_tool_registry(run_id, db_path)

    messages: list[Any] = [
        SystemMessage(content=_sanitize_text(SYSTEM_PROMPT)),
        HumanMessage(content=_sanitize_text(question)),
    ]

    seen_calls: set[str] = set()

    for step in range(MAX_TOOL_CALLS + 1):
        response = llm.invoke(messages, tools=tools_schema)
        messages.append(response)

        tool_calls = getattr(response, "tool_calls", None) or []

        if debug:
            print(f"[debug] step={step}")
            print(f"[debug] content={repr(getattr(response, 'content', None))}")
            print(f"[debug] parsed_tool_calls={tool_calls}")

        if not tool_calls:
            additional_kwargs = getattr(response, "additional_kwargs", {}) or {}
            raw_tool_calls = additional_kwargs.get("tool_calls", [])

            normalized_calls = []
            for call in raw_tool_calls:
                fn = call.get("function", {})
                normalized_calls.append(
                    {
                        "id": call.get("id", ""),
                        "name": fn.get("name", ""),
                        "args": fn.get("arguments", "{}"),
                    }
                )
            tool_calls = normalized_calls

        if step >= MAX_TOOL_CALLS:
            messages.append(
                HumanMessage(
                    content=(
                        "Хватит вызывать tools. "
                        "Сформируй финальный краткий ответ на основе уже полученных результатов."
                    )
                )
            )
            final_response = llm.invoke(messages, tools=[])
            content = final_response.content
            return content.strip() if isinstance(content, str) else str(content)

        for call in tool_calls:
            tool_name = call["name"]
            tool_args = _parse_tool_args(call.get("args", {}))
            tool_call_id = call["id"]

            signature = _make_call_signature(tool_name, tool_args)

            if signature in seen_calls:
                messages.append(
                    ToolMessage(
                        tool_call_id=tool_call_id,
                        content=_safe_json_dumps(
                            {
                                "error": (
                                    f"Tool {tool_name} с теми же аргументами уже вызывался. "
                                    "Не повторяй вызов, дай финальный ответ по имеющимся данным."
                                )
                            }
                        ),
                    )
                )
                continue

            seen_calls.add(signature)

            tool_fn = tool_registry.get(tool_name)
            if tool_fn is None:
                tool_result = {"error": f"Неизвестный tool: {tool_name}"}
            else:
                try:
                    tool_result = tool_fn(**tool_args)
                except TypeError as exc:
                    tool_result = {"error": f"Неверные аргументы для {tool_name}: {exc}"}
                except Exception as exc:
                    tool_result = {"error": f"Ошибка tool {tool_name}: {exc}"}

            messages.append(
                ToolMessage(
                    tool_call_id=tool_call_id,
                    content=_safe_json_dumps(tool_result),
                )
            )

    return "Не удалось получить финальный ответ."