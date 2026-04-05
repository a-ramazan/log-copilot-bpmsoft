from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from ..storage import StorageRepository
from .session import AgentSessionState, resolve_cluster_index_from_question


SHA1_RE = re.compile(r"\b[a-f0-9]{40}\b", re.IGNORECASE)


@dataclass
class RunContext:
    run_id: str
    db_path: str
    profile: str
    run_summary: dict


def _repo(db_path: str) -> StorageRepository:
    return StorageRepository(Path(db_path))


def fetch_run_summary(run_id: str, db_path: str) -> dict | None:
    return _repo(db_path).get_run_summary(run_id)


def fetch_top_incidents(run_id: str, db_path: str, limit: int = 5) -> list[dict]:
    return _repo(db_path).get_top_incidents(run_id, limit=limit)


def fetch_incident_cluster(run_id: str, db_path: str, cluster_id: str) -> dict | None:
    return _repo(db_path).find_incident_cluster(run_id, cluster_id)


def fetch_heatmap(run_id: str, db_path: str, limit: int = 5) -> list[dict]:
    return _repo(db_path).get_heatmap(run_id, limit=limit)


def fetch_traffic_summary(
    run_id: str,
    db_path: str,
    limit: int = 5,
    status: int | None = None,
) -> list[dict]:
    return _repo(db_path).get_traffic_summary(run_id, status=status, limit=limit)


def fetch_traffic_anomalies(run_id: str, db_path: str, limit: int = 5) -> list[dict]:
    return _repo(db_path).get_traffic_anomalies(run_id, limit=limit)


# Обратная совместимость для старых тестов/импортов.
get_run_summary = fetch_run_summary
get_top_incidents = fetch_top_incidents
find_incident_cluster = fetch_incident_cluster
get_heatmap = fetch_heatmap
get_traffic_summary = fetch_traffic_summary
get_traffic_anomalies = fetch_traffic_anomalies


def resolve_run_context(run_id: str, db_path: str) -> RunContext:
    summary = fetch_run_summary(run_id, db_path=db_path)
    if summary is None:
        raise ValueError(f"Run {run_id} не найден.")
    profile = summary.get("profile")
    if not profile:
        raise ValueError(f"У run {run_id} отсутствует profile.")
    return RunContext(run_id=run_id, db_path=db_path, profile=profile, run_summary=summary)


def allowed_actions_for_profile(profile: str) -> list[str]:
    if profile == "incidents":
        return ["capabilities", "overview", "top_incidents", "incident_cluster", "profile_scope"]
    if profile == "heatmap":
        return ["capabilities", "overview", "heatmap", "profile_scope"]
    if profile == "traffic":
        return ["capabilities", "overview", "traffic_summary", "traffic_500", "traffic_anomalies", "profile_scope"]
    return ["capabilities", "overview", "profile_scope"]


def _pick_cluster_from_memory(question: str, memory: AgentSessionState) -> str | None:
    index = resolve_cluster_index_from_question(question)
    if index is not None and index < len(memory.last_top_cluster_ids):
        return memory.last_top_cluster_ids[index]
    if memory.focused_cluster_id:
        return memory.focused_cluster_id
    if memory.last_top_cluster_ids:
        return memory.last_top_cluster_ids[0]
    return None


def route_question(profile: str, question: str, memory_payload: dict | None = None) -> dict:
    question_lower = (question or "").lower()
    memory = AgentSessionState.from_dict(memory_payload)

    if any(token in question_lower for token in ("привет", "кто ты", "что ты умеешь", "что умеешь", "помощь")):
        return {"action": "capabilities", "args": {}}

    explicit_cluster = SHA1_RE.search(question_lower)
    if explicit_cluster and profile == "incidents":
        return {"action": "incident_cluster", "args": {"cluster_id": explicit_cluster.group(0)}}

    if profile == "incidents":
        if any(token in question_lower for token in ("трафик", "endpoint", "latency", "500", "аномал")):
            return {"action": "profile_scope", "args": {"requested_scope": "traffic"}}
        if any(token in question_lower for token in ("нагруз", "heatmap", "hotspot", "пик")):
            return {"action": "profile_scope", "args": {"requested_scope": "heatmap"}}
        if any(token in question_lower for token in ("топ", "кластер", "инцид")):
            limit = 3 if "3" in question_lower else 5
            return {"action": "top_incidents", "args": {"limit": limit}}
        if any(
            token in question_lower
            for token in ("в чем", "в чём", "почему", "как исправ", "исправить", "как решить", "решить", "починить", "пофикс", "ошиб")
        ):
            cluster_id = _pick_cluster_from_memory(question, memory)
            if cluster_id:
                return {"action": "incident_cluster", "args": {"cluster_id": cluster_id}}
            return {"action": "top_incidents", "args": {"limit": 3}}
        return {"action": "overview", "args": {}}

    if profile == "heatmap":
        if any(token in question_lower for token in ("инцид", "ошиб", "кластер")):
            return {"action": "profile_scope", "args": {"requested_scope": "incidents"}}
        if any(token in question_lower for token in ("трафик", "endpoint", "500", "аномал")):
            return {"action": "profile_scope", "args": {"requested_scope": "traffic"}}
        if any(token in question_lower for token in ("нагруз", "heatmap", "hotspot", "пик", "компонент")):
            return {"action": "heatmap", "args": {"limit": 5}}
        return {"action": "overview", "args": {}}

    if profile == "traffic":
        if any(token in question_lower for token in ("инцид", "ошиб", "кластер")):
            return {"action": "profile_scope", "args": {"requested_scope": "incidents"}}
        if any(token in question_lower for token in ("нагруз", "heatmap", "hotspot", "пик")):
            return {"action": "profile_scope", "args": {"requested_scope": "heatmap"}}
        if "500" in question_lower:
            return {"action": "traffic_500", "args": {"limit": 5}}
        if any(token in question_lower for token in ("аномал", "подозр", "скан")):
            return {"action": "traffic_anomalies", "args": {"limit": 5}}
        if any(token in question_lower for token in ("трафик", "endpoint", "latency", "запрос", "статус")):
            return {"action": "traffic_summary", "args": {"limit": 5}}
        return {"action": "overview", "args": {}}

    return {"action": "overview", "args": {}}


def execute_plan(context: RunContext, plan: dict) -> dict:
    action = plan.get("action", "overview")
    args = plan.get("args", {}) or {}

    if action == "capabilities":
        if context.profile == "incidents":
            examples = [
                "покажи топ-3 инцидента",
                "опиши 1-й инцидент",
                "в чем проблема",
                "как исправить",
            ]
        elif context.profile == "heatmap":
            examples = [
                "покажи пики нагрузки",
                "какие компоненты самые горячие",
                "сделай краткий heatmap summary",
            ]
        else:
            examples = [
                "какие endpoint дают 500",
                "покажи аномалии трафика",
                "сделай summary по трафику",
            ]
        return {
            "profile": context.profile,
            "message": f"Я работаю только с данными run_id профиля '{context.profile}'.",
            "examples": examples,
        }

    if action == "profile_scope":
        requested_scope = args.get("requested_scope", "unknown")
        return {
            "profile": context.profile,
            "message": (
                f"Текущий run_id относится к профилю '{context.profile}', "
                f"поэтому сценарий '{requested_scope}' здесь недоступен."
            ),
        }

    if action == "overview":
        payload = {
            "profile": context.profile,
            "run_summary": context.run_summary.get("summary_json", context.run_summary),
        }
        if context.profile == "incidents":
            payload["top_incidents"] = fetch_top_incidents(context.run_id, context.db_path, limit=3)
        elif context.profile == "heatmap":
            payload["heatmap"] = fetch_heatmap(context.run_id, context.db_path, limit=3)
        elif context.profile == "traffic":
            payload["traffic_summary"] = fetch_traffic_summary(context.run_id, context.db_path, limit=3)
            payload["traffic_anomalies"] = fetch_traffic_anomalies(context.run_id, context.db_path, limit=3)
        return payload

    if action == "top_incidents":
        return {"profile": context.profile, "top_incidents": fetch_top_incidents(context.run_id, context.db_path, **args)}

    if action == "incident_cluster":
        cluster_id = args.get("cluster_id")
        if not cluster_id:
            return {"error": "Не передан cluster_id."}
        row = fetch_incident_cluster(context.run_id, context.db_path, cluster_id)
        if row is None:
            return {"error": f"Кластер {cluster_id} не найден."}
        return {"profile": context.profile, "incident_cluster": row}

    if action == "heatmap":
        return {"profile": context.profile, "heatmap": fetch_heatmap(context.run_id, context.db_path, **args)}

    if action == "traffic_summary":
        return {"profile": context.profile, "traffic_summary": fetch_traffic_summary(context.run_id, context.db_path, **args)}

    if action == "traffic_500":
        return {
            "profile": context.profile,
            "traffic_summary": fetch_traffic_summary(context.run_id, context.db_path, status=500, **args),
        }

    if action == "traffic_anomalies":
        return {"profile": context.profile, "traffic_anomalies": fetch_traffic_anomalies(context.run_id, context.db_path, **args)}

    return {"error": f"Неизвестное действие: {action}"}
