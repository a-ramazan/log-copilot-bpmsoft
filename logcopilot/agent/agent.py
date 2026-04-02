from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Iterator

from langchain_core.messages import HumanMessage, SystemMessage
from torchvision import message

from .config import DEFAULT_DB_PATH, AgentModelConfig, build_chat_model, resolve_model_config
from .graph import build_agent_graph
from .prompts import build_answer_system_prompt


@dataclass
class AgentExecutionResult:
    answer: str
    profile: str
    plan: dict
    facts: dict
    memory: dict
    trace: list[str]


def _clean_text(value: str) -> str:
    if not isinstance(value, str):
        return str(value)
    return value.encode("utf-8", errors="replace").decode("utf-8", errors="replace")


def _sanitize_data(value):
    if isinstance(value, dict):
        return {_clean_text(str(key)): _sanitize_data(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_data(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_data(item) for item in value)
    if isinstance(value, str):
        return _clean_text(value)
    return value


def _safe_json(data) -> str:
    return json.dumps(_sanitize_data(data), ensure_ascii=False, indent=2, default=str)


def _build_fallback_answer(plan: dict, facts: dict) -> str:
    action = plan.get("action")

    if facts.get("error"):
        return f"Ошибка: {facts['error']}"

    if action == "capabilities":
        lines = [facts.get("message", "Я готов работать с этим run_id.")]
        for item in facts.get("examples", []):
            lines.append(f"- {item}")
        return "\n".join(lines)

    if action == "profile_scope":
        return facts.get("message", "Этот сценарий для текущего run_id недоступен.")

    if action == "top_incidents":
        rows = facts.get("top_incidents", [])
        if not rows:
            return "Инциденты не найдены."
        lines = ["Топ инциденты:"]
        for index, row in enumerate(rows, start=1):
            payload = row.get("payload_json", {}) or {}
            lines.append(
                f"{index}. {row['cluster_id']} | "
                f"hits={row['incident_hits']} | "
                f"exception={payload.get('exception_type', 'n/a')}"
            )
        return "\n".join(lines)

    if action == "incident_cluster":
        row = facts.get("incident_cluster")
        if not row:
            return "Кластер не найден."
        payload = row.get("payload_json", {}) or {}
        sample = _clean_text(row.get("representative_text") or "").replace("\n", " ").strip()
        lines = [
            f"Кластер: {row['cluster_id']}",
            f"Повторений: {row['incident_hits']}",
            f"Ошибка: {payload.get('exception_type', 'n/a')}",
        ]
        if sample:
            lines.append(f"Пример: {sample[:240]}")
        return "\n".join(lines)

    if action == "heatmap":
        rows = facts.get("heatmap", [])
        if not rows:
            return "Heatmap данные не найдены."
        lines = ["Топ heatmap points:"]
        for index, row in enumerate(rows, start=1):
            lines.append(
                f"{index}. {row['bucket_start']} | {row['component']} | "
                f"{row['operation']} | hits={row['hits']}"
            )
        return "\n".join(lines)

    if action in {"traffic_summary", "traffic_500"}:
        rows = facts.get("traffic_summary", [])
        if not rows:
            return "Traffic данные не найдены."
        lines = ["Traffic summary:"]
        for index, row in enumerate(rows, start=1):
            lines.append(
                f"{index}. {row['method']} {row['path']} | "
                f"status={row['http_status']} | hits={row['hits']}"
            )
        return "\n".join(lines)

    if action == "traffic_anomalies":
        rows = facts.get("traffic_anomalies", [])
        if not rows:
            return "Аномалии не найдены."
        lines = ["Аномалии трафика:"]
        for index, row in enumerate(rows, start=1):
            lines.append(f"{index}. [{row['severity']}] {row['title']}")
        return "\n".join(lines)

    return _safe_json(facts)




def prepare_agent_context(
    question: str,
    run_id: str,
    db_path: str = DEFAULT_DB_PATH,
    session_state: dict | None = None,
) -> AgentExecutionResult:
    graph = build_agent_graph()
    state = graph.invoke(
        {
            "question": _clean_text(question),
            "run_id": run_id,
            "db_path": db_path,
            "profile": "",
            "run_summary": {},
            "plan": {},
            "facts": {},
            "memory": session_state or {},
            "trace": [],
        }
    )
    return AgentExecutionResult(
        answer="",
        profile=state["profile"],
        plan=state["plan"],
        facts=_sanitize_data(state["facts"]),
        memory=state.get("memory", {}),
        trace=state["trace"],
    )

def _build_answer_messages(question: str, profile: str, plan: dict, facts: dict) -> list:
    return [
        SystemMessage(content=build_answer_system_prompt(profile)),
        HumanMessage(
            content=(
                f"Вопрос пользователя:\n{_clean_text(question)}\n\n"
                f"Выбранное действие:\n{_safe_json(plan)}\n\n"
                f"Факты из БД:\n{_safe_json(facts)}\n\n"
                "Ответь по-человечески. Если вопрос про причину или исправление, "
                "дай краткое объяснение и практические шаги проверки."
            )
        ),
    ]

def stream_agent(
    question: str,
    run_id: str,
    provider: str = "local",
    session_state: dict | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    folder_id: str | None = None,
) -> tuple[AgentExecutionResult, Iterator[str]]:

    result = prepare_agent_context(
        question=question,
        run_id=run_id,
        db_path=DEFAULT_DB_PATH,
        session_state=session_state,
    )
    model_config = resolve_model_config(
        provider=provider,
        model=model,
        base_url=base_url,
        api_key=api_key,
        folder_id=folder_id,
    )
    llm = build_chat_model(model_config)

    def _iterator() -> Iterator[str]:
        try:
            message = _build_answer_messages(
                question=question,
                profile=result.profile,
                plan=result.plan,
                facts=result.facts
            )
            for chunk in llm.stream(message):
                content = getattr(chunk, "content", "")
                if content:
                    yield str(content)
            result.trace.append("answer: llm_stream")
        except Exception as exc:
            result.answer = _build_fallback_answer(result.plan, result.facts)
            result.trace.append(f"answer: fallback_stream -> {exc}")
            yield result.answer

    return result, _iterator()
