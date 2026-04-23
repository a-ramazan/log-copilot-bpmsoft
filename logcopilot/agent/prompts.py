from __future__ import annotations

"""Short profile prompts for the structured pipeline agent."""

import json
from typing import Any, Dict, List

from ..domain import AgentInputContext


OUTPUT_CONTRACT = {
    "profile": "incidents|heatmap|traffic",
    "overall_status": "ok|warning|critical|limited",
    "confidence": 0.0,
    "short_summary": "одно предложение",
    "technical_summary": "техническое summary по фактам",
    "business_summary": "бизнесовое summary по фактам",
    "key_findings": ["строки"],
    "recommended_actions": ["строки"],
    "limitations": ["строки"],
    "cards": [
        {
            "card_type": "incident|heatmap|traffic",
            "title": "строка",
            "severity": "low|medium|high|critical",
            "confidence": 0.0,
            "summary": "строка",
            "evidence": ["строки из предоставленных фактов"],
            "recommended_actions": ["строки"],
            "limitations": ["строки"],
        }
    ],
}


PROFILE_RULES = {
    "incidents": [
        "Создавай incident cards на основе compact_llm_ready_cluster_facts.",
        "Если incident_hits равен нулю, но кластеры существуют, возвращай low-severity observation/noise cards, а не пустой список.",
        "Каждая incident card должна включать cluster_id, hits, incident_hits, first_seen, last_seen и exception_type, когда они доступны.",
    ],
    "heatmap": [
        "Создавай heatmap cards на основе hotspots.",
        "Если hotspots существуют, cards не должны быть пустыми.",
        "Каждая heatmap card должна включать bucket_start, component, operation, hits, qps и p95_latency_ms, когда они доступны.",
    ],
    "traffic": [
        "Сначала создавай traffic cards из suspicious_patterns, затем из traffic_findings.",
        "Если существуют endpoint или anomaly facts, cards не должны быть пустыми.",
        "Каждая traffic card должна включать pattern_type, method, path, http_status, hits, unique_ips и p95_latency_ms, когда они доступны.",
    ],
}


def _json(payload: Dict[str, Any]) -> str:
    """Serialize prompt payload as stable JSON."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _system_prompt(profile: str) -> str:
    """Return common model instructions."""
    rules = " ".join(PROFILE_RULES[profile])
    return (
        "Ты one-shot structured interpreter внутри LogCopilot. "
        "Используй только переданные compact facts. Не придумывай поля, счетчики, причины, таймлайны или root cause. "
        "Не проси tools, raw logs, CSV, parquet или SQLite. "
        "Верни только валидный JSON, без markdown и без текста вне JSON. "
        "JSON-объект обязан включать top-level keys: profile, overall_status, confidence, short_summary, "
        "technical_summary, business_summary, key_findings, recommended_actions, limitations, cards. "
        "Не пропускай обязательные ключи. Не возвращай пустые summaries, findings, actions, card evidence или card summaries, "
        "если в фактах есть интерпретируемые строки. "
        "Каждая card обязана включать card_type, title, severity, confidence, summary, evidence, recommended_actions и limitations. "
        "Пиши весь человекочитаемый текст по-русски: title, short_summary, technical_summary, business_summary, key_findings, "
        "recommended_actions, limitations, summary и evidence. "
        "Но значения enum-полей оставляй строго как в контракте на английском: profile, overall_status, card_type, severity. "
        "Сохраняй ответ коротким: summaries по одному предложению, не больше двух evidence items и двух actions на card. "
        "Если не уверен, используй короткий grounded text, скопированный или выведенный из supplied facts. "
        f"Выбранный профиль: {profile}. {rules}"
    )


def _messages(input_context: AgentInputContext, task: str) -> List[Dict[str, str]]:
    """Build profile messages."""
    payload = {
        "task": task,
        "quality_requirements": {
            "top_level_fields_are_required": True,
            "empty_text_is_invalid_when_facts_exist": True,
            "card_text_fields_are_required": ["title", "summary", "evidence", "recommended_actions"],
            "use_only_compact_input_context": True,
        },
        "output_contract": OUTPUT_CONTRACT,
        "compact_input_context": input_context.as_dict(),
    }
    return [
        {"role": "system", "content": _system_prompt(input_context.profile)},
        {"role": "user", "content": _json(payload)},
    ]


def build_incidents_prompt(input_context: AgentInputContext) -> List[Dict[str, str]]:
    """Build the incidents structured-output prompt."""
    return _messages(input_context, "Интерпретируй incident clusters и operational observations.")


def build_heatmap_prompt(input_context: AgentInputContext) -> List[Dict[str, str]]:
    """Build the heatmap structured-output prompt."""
    return _messages(input_context, "Интерпретируй heatmap hotspots и концентрацию нагрузки.")


def build_traffic_prompt(input_context: AgentInputContext) -> List[Dict[str, str]]:
    """Build the traffic structured-output prompt."""
    return _messages(input_context, "Интерпретируй traffic anomalies, errors, latency и load.")


def build_agent_messages(input_context: AgentInputContext) -> List[Dict[str, str]]:
    """Dispatch to the selected profile prompt."""
    if input_context.profile == "incidents":
        return build_incidents_prompt(input_context)
    if input_context.profile == "heatmap":
        return build_heatmap_prompt(input_context)
    if input_context.profile == "traffic":
        return build_traffic_prompt(input_context)
    raise ValueError(f"Unsupported agent prompt profile: {input_context.profile}")


__all__ = [
    "OUTPUT_CONTRACT",
    "build_agent_messages",
    "build_heatmap_prompt",
    "build_incidents_prompt",
    "build_traffic_prompt",
]
