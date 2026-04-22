from __future__ import annotations

"""Short profile prompts for the structured pipeline agent."""

import json
from typing import Any, Dict, List

from ..domain import AgentInputContext


OUTPUT_CONTRACT = {
    "profile": "incidents|heatmap|traffic",
    "overall_status": "ok|warning|critical|limited",
    "confidence": 0.0,
    "short_summary": "one sentence",
    "technical_summary": "grounded technical summary",
    "business_summary": "grounded impact summary",
    "key_findings": ["strings"],
    "recommended_actions": ["strings"],
    "limitations": ["strings"],
    "cards": [
        {
            "card_type": "incident|heatmap|traffic",
            "title": "string",
            "severity": "low|medium|high|critical",
            "confidence": 0.0,
            "summary": "string",
            "evidence": ["strings from supplied facts"],
            "recommended_actions": ["strings"],
            "limitations": ["strings"],
        }
    ],
}


PROFILE_RULES = {
    "incidents": [
        "Create incident cards from compact_llm_ready_cluster_facts.",
        "If incident_hits is zero but clusters exist, return low-severity observation/noise cards instead of an empty list.",
        "Each incident card should include cluster_id, hits, incident_hits, first_seen, last_seen and exception_type when available.",
    ],
    "heatmap": [
        "Create heatmap cards from hotspots.",
        "If hotspots exist, cards must not be empty.",
        "Each heatmap card should include bucket_start, component, operation, hits, qps and p95_latency_ms when available.",
    ],
    "traffic": [
        "Create traffic cards from suspicious_patterns first, then traffic_findings.",
        "If endpoint or anomaly facts exist, cards must not be empty.",
        "Each traffic card should include pattern_type, method, path, http_status, hits, unique_ips and p95_latency_ms when available.",
    ],
}


def _json(payload: Dict[str, Any]) -> str:
    """Serialize prompt payload as stable JSON."""
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _system_prompt(profile: str) -> str:
    """Return common model instructions."""
    rules = " ".join(PROFILE_RULES[profile])
    return (
        "You are LogCopilot's one-shot structured interpreter. "
        "Use only the supplied compact facts. Do not invent fields, counts, causes, timelines or root causes. "
        "Do not ask for tools, raw logs, CSV, parquet or SQLite access. "
        "Return only valid JSON, with no markdown and no prose outside JSON. "
        "The JSON object must include these top-level keys: profile, overall_status, confidence, short_summary, "
        "technical_summary, business_summary, key_findings, recommended_actions, limitations, cards. "
        "Do not omit required keys. Do not return empty summaries, findings, actions, card evidence or card summaries "
        "when supplied facts contain interpretable rows. "
        "Each card must include card_type, title, severity, confidence, summary, evidence, recommended_actions and limitations. "
        "Keep the response concise: one-sentence summaries, at most two evidence items and two actions per card. "
        "Use short grounded text copied or derived from supplied facts when uncertain. "
        f"Selected profile: {profile}. {rules}"
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
    return _messages(input_context, "Interpret incident clusters and operational observations.")


def build_heatmap_prompt(input_context: AgentInputContext) -> List[Dict[str, str]]:
    """Build the heatmap structured-output prompt."""
    return _messages(input_context, "Interpret heatmap hotspots and load concentration.")


def build_traffic_prompt(input_context: AgentInputContext) -> List[Dict[str, str]]:
    """Build the traffic structured-output prompt."""
    return _messages(input_context, "Interpret traffic anomalies, errors, latency and load.")


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
