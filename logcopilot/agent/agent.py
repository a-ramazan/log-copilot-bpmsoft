from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Iterator

from langchain_core.messages import HumanMessage, SystemMessage

from .config import DEFAULT_DB_PATH, build_chat_model, resolve_model_config
from .graph import run_agent_pipeline
from .prompts import build_answer_system_prompt


MAX_PROMPT_ROWS = 5


@dataclass
class AgentExecutionResult:
    answer: str
    profile: str
    plan: dict
    facts: dict
    memory: dict
    trace: list[str]
    visuals: list[dict] = field(default_factory=list)


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


def _extract_text_content(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(_extract_text_content(item) for item in content)
    if isinstance(content, dict):
        for key in ("text", "content", "output_text"):
            value = content.get(key)
            if value:
                return _extract_text_content(value)
        return ""
    return str(content)


def _compact_rows(rows: list[dict], columns: list[str], limit: int = MAX_PROMPT_ROWS) -> list[dict]:
    compact = []
    for row in rows[:limit]:
        compact.append({column: row.get(column) for column in columns})
    return compact


def _compact_incident_rows(rows: list[dict], limit: int = MAX_PROMPT_ROWS) -> list[dict]:
    compact: list[dict] = []
    for row in rows[:limit]:
        payload = row.get("payload_json") or {}
        compact.append(
            {
                "cluster_id": row.get("cluster_id"),
                "incident_hits": row.get("incident_hits"),
                "hits": row.get("hits"),
                "confidence_label": row.get("confidence_label"),
                "representative_text": row.get("representative_text"),
                "exception_type": payload.get("exception_type"),
                "levels": payload.get("levels"),
                "sample_messages": list(payload.get("sample_messages", []))[:3],
                "source_files": payload.get("source_files"),
            }
        )
    return compact


def _profile_data_quality(profile: str, data_quality: dict) -> dict:
    field_coverage = data_quality.get("field_coverage", {}) or {}
    parser_quality = data_quality.get("parser_quality")
    incident_signal_quality = data_quality.get("incident_signal_quality")
    parser_diagnostics = data_quality.get("parser_diagnostics")
    profile_fit = data_quality.get("profile_fit")
    visual_readiness = data_quality.get("visual_readiness", {}) or {}

    if profile == "incidents":
        relevant_coverage = {
            "timestamp": field_coverage.get("timestamp"),
            "component": field_coverage.get("component"),
        }
        relevant_visuals = {"incidents": visual_readiness.get("incidents")}
    elif profile == "heatmap":
        relevant_coverage = {
            "timestamp": field_coverage.get("timestamp"),
            "component": field_coverage.get("component"),
            "latency_ms": field_coverage.get("latency_ms"),
        }
        relevant_visuals = {"heatmap": visual_readiness.get("heatmap")}
    else:
        relevant_coverage = {
            "method": field_coverage.get("method"),
            "path": field_coverage.get("path"),
            "http_status": field_coverage.get("http_status"),
            "latency_ms": field_coverage.get("latency_ms"),
            "client_ip": field_coverage.get("client_ip"),
        }
        relevant_visuals = {"traffic": visual_readiness.get("traffic")}

    compact = {
        "event_count": data_quality.get("event_count"),
        "field_coverage": {key: value for key, value in relevant_coverage.items() if value is not None},
        "visual_readiness": {key: value for key, value in relevant_visuals.items() if value is not None},
    }
    if parser_quality:
        compact["parser_quality"] = parser_quality
    if incident_signal_quality and incident_signal_quality.get("score") is not None:
        compact["incident_signal_quality"] = incident_signal_quality
    if parser_diagnostics:
        compact["parser_diagnostics"] = parser_diagnostics
    if profile_fit:
        compact["profile_fit"] = profile_fit
    return compact


def build_prompt_payload(plan: dict, facts: dict, visuals: list[dict]) -> dict:
    action = plan.get("action")
    profile = facts.get("profile") or facts.get("run_summary", {}).get("profile") or "unknown"
    payload = {
        "run_summary": facts.get("run_summary", {}),
        "data_quality": _profile_data_quality(profile, facts.get("data_quality", {})),
        "visual_info": facts.get("visual_info", {}),
    }

    if visuals:
        payload["visuals"] = [
            {
                "kind": item.get("kind"),
                "title": item.get("title"),
                "caption": item.get("caption"),
                "path": item.get("path"),
            }
            for item in visuals[:1]
        ]

    if action == "capabilities":
        payload["message"] = facts.get("message")
        payload["examples"] = list(facts.get("examples", []))[:4]
        return payload

    if action == "profile_scope":
        payload["message"] = facts.get("message")
        return payload

    if action == "parser_diagnostics_summary":
        payload["parser_diagnostics_summary"] = facts.get("parser_diagnostics_summary", {})
        return payload

    if action == "profile_fit_summary":
        payload["profile_fit_summary"] = facts.get("profile_fit_summary", {})
        return payload

    if action == "top_incidents":
        payload["top_incidents"] = _compact_incident_rows(facts.get("top_incidents", []))
        return payload

    if action in {"incident_cluster_detail", "incident_cluster_examples", "incident_cluster_causes"}:
        row = facts.get("incident_cluster") or {}
        cluster = {
            "cluster_id": row.get("cluster_id"),
            "incident_hits": row.get("incident_hits"),
            "hits": row.get("hits"),
            "confidence_label": row.get("confidence_label"),
            "representative_text": row.get("representative_text"),
        }
        payload_json = row.get("payload_json") or {}
        cluster["exception_type"] = payload_json.get("exception_type")
        cluster["top_stack_frames"] = list(payload_json.get("top_stack_frames", []))[:3]
        cluster["sample_messages"] = list(payload_json.get("sample_messages", []))[:4]
        cluster["levels"] = payload_json.get("levels")
        cluster["source_files"] = payload_json.get("source_files")
        payload["incident_cluster"] = cluster
        payload["parser_diagnostics_summary"] = facts.get("parser_diagnostics_summary", {})
        payload["profile_fit_summary"] = facts.get("profile_fit_summary", {})
        return payload

    if action == "heatmap":
        payload["heatmap"] = _compact_rows(
            facts.get("heatmap", []),
            ["bucket_start", "component", "operation", "hits", "qps", "p95_latency_ms"],
        )
        return payload

    if action in {"traffic_summary", "traffic_500"}:
        payload["traffic_summary"] = _compact_rows(
            facts.get("traffic_summary", []),
            ["method", "path", "http_status", "hits", "unique_ips", "p95_latency_ms", "p99_latency_ms"],
        )
        return payload

    if action == "traffic_anomalies":
        payload["traffic_anomalies"] = _compact_rows(
            facts.get("traffic_anomalies", []),
            ["anomaly_type", "severity", "title", "details"],
        )
        return payload

    overview = {}
    if facts.get("top_incidents"):
        overview["top_incidents"] = _compact_incident_rows(facts.get("top_incidents", []), limit=3)
    if facts.get("heatmap"):
        overview["heatmap"] = _compact_rows(
            facts.get("heatmap", []),
            ["bucket_start", "component", "operation", "hits"],
            limit=3,
        )
    if facts.get("traffic_summary"):
        overview["traffic_summary"] = _compact_rows(
            facts.get("traffic_summary", []),
            ["method", "path", "http_status", "hits", "p95_latency_ms"],
            limit=3,
        )
    if facts.get("traffic_anomalies"):
        overview["traffic_anomalies"] = _compact_rows(
            facts.get("traffic_anomalies", []),
            ["severity", "title", "details"],
            limit=3,
        )
    payload.update(overview)
    return payload


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

    if action == "parser_diagnostics_summary":
        diagnostics = facts.get("parser_diagnostics_summary", {}) or {}
        parse_quality = diagnostics.get("parse_quality", {})
        incident_signal = diagnostics.get("incident_signal_quality", {})
        lines = [
            "Диагностика разбора:",
            f"- dominant parser: {diagnostics.get('dominant_parser', 'unknown')}",
            f"- parse quality: {parse_quality.get('label', 'n/a')} ({parse_quality.get('score', 'n/a')})",
            f"- incident signal: {incident_signal.get('label', 'n/a')} ({incident_signal.get('score', 'n/a')})",
            f"- fallback ratio: {diagnostics.get('fallback_ratio', 'n/a')}",
        ]
        warnings = diagnostics.get("warnings_sample", [])
        if warnings:
            lines.append(f"- warnings: {warnings[0]}")
        return "\n".join(lines)

    if action == "profile_fit_summary":
        fit = facts.get("profile_fit_summary", {}) or {}
        return (
            f"Профиль '{fit.get('selected_profile', 'unknown')}' подходит на уровне {fit.get('fit_label', 'n/a')}. "
            f"Рекомендуемый профиль: {fit.get('recommended_profile', 'unknown')}. "
            f"Причина: {fit.get('reason', 'n/a')}."
        )

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

    if action in {"incident_cluster_detail", "incident_cluster_examples", "incident_cluster_causes"}:
        row = facts.get("incident_cluster")
        if not row:
            return "Кластер не найден."
        payload = row.get("payload_json", {}) or {}
        sample = _clean_text(row.get("representative_text") or "").replace("\n", " ").strip()
        if action == "incident_cluster_examples":
            messages = payload.get("sample_messages", []) or []
            lines = [f"Примеры из кластера {row['cluster_id']}:"]
            for item in messages[:4]:
                lines.append(f"- {item}")
            if len(lines) == 1 and sample:
                lines.append(f"- {sample[:240]}")
            return "\n".join(lines)

        lines = [f"Кластер {row['cluster_id']}"]
        if sample:
            lines.append(f"Суть: {sample[:240]}")
        lines.append(f"Повторений: {row['incident_hits']}")
        if payload.get("exception_type"):
            lines.append(f"Ошибка: {payload.get('exception_type')}")
        if action == "incident_cluster_causes":
            lines.append("Гипотеза: причина определяется по representative message и sample messages выше; проверь соответствующий компонент и соседние события этого времени.")
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
        visual_info = facts.get("visual_info", {})
        if visual_info.get("status") == "unavailable":
            lines.append(f"График не построен: {visual_info.get('reason')}.")
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

    run_summary = facts.get("run_summary", {}) or {}
    data_quality = facts.get("data_quality", {}) or {}
    parser_quality = (data_quality.get("parser_quality") or {}).get("label")
    profile_fit = facts.get("profile_fit_summary") or data_quality.get("profile_fit") or {}
    profile = facts.get("profile") or run_summary.get("profile") or "unknown"
    lines = [
        f"Профиль: {profile}. Событий обработано: {run_summary.get('event_count', 'n/a')}.",
    ]
    if parser_quality:
        lines.append(f"Качество структурного разбора: {parser_quality}.")
    if profile_fit:
        lines.append(
            f"По структуре лог ближе к сценарию '{profile_fit.get('recommended_profile', profile)}'."
        )
    if profile == "incidents" and facts.get("top_incidents"):
        top = facts["top_incidents"][0]
        lines.append(
            f"Главный инцидент сейчас: кластер {top.get('cluster_id')} с {top.get('incident_hits', top.get('hits', 0))} повторениями."
        )
    elif profile == "heatmap" and facts.get("heatmap"):
        top = facts["heatmap"][0]
        lines.append(
            f"Самая горячая точка: {top.get('bucket_start')} / {top.get('component')} / {top.get('operation')}."
        )
    elif profile == "traffic" and facts.get("traffic_summary"):
        top = facts["traffic_summary"][0]
        lines.append(
            f"Самый заметный endpoint: {top.get('method')} {top.get('path')} со статусом {top.get('http_status')}."
        )
    return " ".join(lines)


def prepare_agent_context(
    question: str,
    run_id: str,
    db_path: str = DEFAULT_DB_PATH,
    session_state: dict | None = None,
    planner_model=None,
) -> AgentExecutionResult:
    state = run_agent_pipeline(
        question=_clean_text(question),
        run_id = run_id,
        db_path = db_path,
        session_state = session_state,
        planner_model = planner_model,
    )
    return AgentExecutionResult(
        answer = "",
        profile = state["profile"],
        plan = state["plan"],
        facts =_sanitize_data(state["facts"]),
        memory = state.get("memory", {}),
        trace=state["trace"],
        visuals=_sanitize_data(state.get("visuals", [])),
    )


def _build_answer_messages(question: str, profile: str, plan: dict, facts: dict, visuals: list[dict]) -> list:
    prompt_payload = build_prompt_payload(plan=plan, facts=facts, visuals=visuals)
    return [
        SystemMessage(content=build_answer_system_prompt(profile)),
        HumanMessage(
            content=(
                f"Вопрос пользователя:\n{_clean_text(question)}\n\n"
                f"Выбранное действие:\n{_safe_json(plan)}\n\n"
                f"Компактные факты из БД:\n{_safe_json(prompt_payload)}\n\n"
                "Ответь по-человечески. Начинай с сути наблюдения или инцидента. "
                "Ограничения качества и missing fields упоминай только после сути. "
                "Если вопрос про причину или исправление, дай краткое объяснение и практические шаги проверки. "
                "Не повторяй просто low/medium/high без расшифровки."
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
    db_path: str = DEFAULT_DB_PATH,
) -> tuple[AgentExecutionResult, Iterator[str]]:
    model_config = resolve_model_config(
        provider = provider,
        model =model,
        base_url=base_url,
        api_key=api_key,
        folder_id=folder_id,
    )
    llm = build_chat_model(model_config)
    result = prepare_agent_context(
        question=question,
        run_id=run_id,
        db_path=db_path,
        session_state=session_state,
        planner_model=llm,
    )

    def _iterator() -> Iterator[str]:
        buffer: list[str] = []
        message_iter = _build_answer_messages(
            question=question,
            profile=result.profile,
            plan=result.plan,
            facts=result.facts,
            visuals=result.visuals,
        )
        try:
            for chunk in llm.stream(message_iter):
                text = _extract_text_content(getattr(chunk, "content", chunk))
                if text:
                    buffer.append(text)
                    yield text
            result.answer = "".join(buffer)
            result.trace.append("answer: llm_stream")
            return
        except Exception as stream_exc:
            if buffer:
                result.answer = "".join(buffer)
                result.trace.append(f"answer: partial_stream -> {stream_exc}")
                return
            try:
                response = llm.invoke(message_iter)
                text = _extract_text_content(getattr(response, "content", response))
                result.answer = text or _build_fallback_answer(result.plan, result.facts)
                result.trace.append(f"answer: llm_invoke_fallback -> {stream_exc}")
                yield result.answer
                return
            except Exception as invoke_exc:
                result.answer = _build_fallback_answer(result.plan, result.facts)
                result.trace.append(f"answer: fallback_stream -> {stream_exc}; invoke_fallback -> {invoke_exc}")
                yield result.answer

    return result, _iterator()


def ask_agent(
    question: str,
    run_id: str,
    provider: str = "local",
    session_state: dict | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
    folder_id: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> AgentExecutionResult:
    result, iterator = stream_agent(
        question=question,
        run_id=run_id,
        provider=provider,
        session_state=session_state,
        model=model,
        base_url=base_url,
        api_key=api_key,
        folder_id=folder_id,
        db_path=db_path,
    )
    if not result.answer:
        result.answer = "".join(iterator)
    else:
        for _ in iterator:
            pass
    return result
