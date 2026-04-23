from __future__ import annotations

"""Single profile-aware pipeline agent stage."""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib import error as urlerror
from urllib import request

from ..domain import (
    AgentInputContext,
    AgentResult,
    HeatmapCard,
    IncidentCard,
    PipelineContext,
    TrafficCard,
)
from .config import (
    AgentModelConfig,
    MAX_AGENT_LIST_ITEMS,
    MAX_AGENT_TEXT_CHARS,
    MAX_HEATMAP_CARDS,
    MAX_INCIDENT_CARDS,
    MAX_TRAFFIC_CARDS,
    provider_is_configured,
    resolve_agent_model_config,
)
from .facts import build_agent_input_context
from .prompts import build_agent_messages

logger = logging.getLogger(__name__)

_OVERALL_STATUSES = {"ok", "warning", "critical", "limited"}
_SEVERITIES = {"low", "medium", "high", "critical"}
_STATUS_RANK = {"limited": 0, "ok": 0, "warning": 1, "critical": 2}
_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2, "critical": 3}
_LLM_MAX_TOKENS = 1600
_CARD_FACT_FIELDS = {
    "cluster_id",
    "hits",
    "incident_hits",
    "first_seen",
    "last_seen",
    "exception_type",
    "bucket_start",
    "component",
    "operation",
    "qps",
    "p95_latency_ms",
    "pattern_type",
    "method",
    "path",
    "http_status",
    "unique_ips",
}


def _ensure_agent_logging() -> None:
    """Show agent diagnostics in CLI runs when the app did not configure logging."""
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")


def _clip_text(value: Any, limit: int = MAX_AGENT_TEXT_CHARS) -> str:
    """Normalize model output strings to bounded text."""
    if value is None:
        return ""
    text = str(value).replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: max(limit - 3, 0)].rstrip() + "..."


def _as_dict(value: Any) -> Dict[str, Any]:
    """Return a mapping value or an empty mapping."""
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any, limit: int = MAX_AGENT_LIST_ITEMS) -> List[Any]:
    """Return a bounded list value."""
    if not isinstance(value, list):
        return []
    return value[:limit]


def _string_list(value: Any, limit: int = MAX_AGENT_LIST_ITEMS) -> List[str]:
    """Normalize a model-produced list of text items."""
    return [_clip_text(item, 360) for item in _as_list(value, limit) if _clip_text(item)]


def _float_value(value: Any, default: float = 0.0) -> float:
    """Coerce a value to float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_value(value: Any, default: int = 0) -> int:
    """Coerce a value to int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_int(value: Any) -> Optional[int]:
    """Coerce a value to optional int."""
    if value is None or value == "":
        return None
    return _int_value(value)


def _optional_float(value: Any) -> Optional[float]:
    """Coerce a value to optional float."""
    if value is None or value == "":
        return None
    return _float_value(value)


def _confidence(value: Any, default: float = 0.5) -> float:
    """Clamp confidence to the public 0..1 contract."""
    return max(0.0, min(1.0, _float_value(value, default)))


def _severity(value: Any, default: str = "medium") -> str:
    """Normalize a severity string."""
    severity = str(value or default).lower()
    return severity if severity in _SEVERITIES else default


def _severity_floor(value: Any, deterministic: str) -> str:
    """Do not let the model understate deterministic card severity."""
    severity = _severity(value or deterministic, deterministic)
    if _SEVERITY_RANK.get(deterministic, 0) > _SEVERITY_RANK.get(severity, 0):
        return deterministic
    return severity


def _fact_limitations(input_context: AgentInputContext) -> List[str]:
    """Build deterministic limitations from parser/profile quality facts."""
    limitations = []
    parse_quality = _as_dict(input_context.parser_diagnostics.get("parse_quality"))
    if parse_quality.get("label") == "low":
        limitations.append("Parser quality is low, so interpretation confidence is limited.")
    incident_signal = _as_dict(input_context.parser_diagnostics.get("incident_signal_quality"))
    if input_context.profile == "incidents" and incident_signal.get("label") == "low":
        limitations.append("Incident signal quality is low; clusters may be weak or noisy.")
    if input_context.profile_fit.get("fit_label") == "low":
        recommended = input_context.profile_fit.get("recommended_profile")
        if recommended:
            limitations.append(f"Selected profile has low fit; recommended profile is {recommended}.")
        else:
            limitations.append("Selected profile has low fit for this run.")
    return limitations


def _provider_limitation(config: AgentModelConfig) -> List[str]:
    """Return a limitation explaining deterministic fallback when no LLM call is used."""
    if config.provider == "none":
        return ["LLM provider is disabled; result is based on deterministic aggregate rules."]
    if not provider_is_configured(config):
        return [f"LLM provider {config.provider} is missing YC_AI_API_KEY or YC_FOLDER_ID; deterministic fallback was used."]
    return []


def _status_from_severities(severities: Iterable[str], has_cards: bool) -> str:
    """Derive run-level status from card severities."""
    severity_set = set(severities)
    if "critical" in severity_set:
        return "critical"
    if "high" in severity_set or "medium" in severity_set:
        return "warning"
    return "ok" if has_cards else "limited"


def _status_floor(status: str, cards: List[Any], profile: str) -> str:
    """Do not let the model understate deterministic card severity."""
    derived = _status_from_severities([getattr(card, "severity", "low") for card in cards], bool(cards))
    if _STATUS_RANK.get(derived, 0) > _STATUS_RANK.get(status, 0):
        logger.warning(
            "agent_status_adjusted: profile=%s llm_status=%s deterministic_status=%s",
            profile,
            status,
            derived,
        )
        return derived
    return status


def _result_confidence(input_context: AgentInputContext, card_confidences: List[float]) -> float:
    """Combine card confidence with parse quality into one bounded score."""
    parse_quality = _as_dict(input_context.parser_diagnostics.get("parse_quality"))
    parse_score = _confidence(parse_quality.get("score"), 0.6)
    if not card_confidences:
        return round(parse_score * 0.8, 3)
    return round((sum(card_confidences) / len(card_confidences) + parse_score) / 2.0, 3)


def _incident_severity(fact: Dict[str, Any]) -> str:
    """Derive incident severity from deterministic cluster facts."""
    incident_hits = _int_value(fact.get("incident_hits"))
    hits = _int_value(fact.get("hits"))
    label = str(fact.get("confidence_label") or "").lower()
    if incident_hits >= 20 or (label == "high" and hits >= 20):
        return "critical"
    if incident_hits >= 5 or label == "high":
        return "high"
    if incident_hits > 0 or hits > 0:
        return "medium"
    return "low"


def _incident_card_from_fact(fact: Dict[str, Any]) -> IncidentCard:
    """Build one deterministic incident card."""
    cluster_id = _clip_text(fact.get("cluster_id", ""), 120)
    incident_hits = _int_value(fact.get("incident_hits"))
    hits = _int_value(fact.get("hits"))
    exception = fact.get("exception_type")
    title_suffix = exception or cluster_id or "incident cluster"
    evidence = []
    if fact.get("representative_text"):
        evidence.append(_clip_text(fact["representative_text"], 360))
    if fact.get("levels"):
        evidence.append(f"Levels: {_clip_text(fact['levels'], 220)}")
    evidence.extend(_string_list(fact.get("sample_messages"), limit=2))
    return IncidentCard(
        title=f"Incident cluster: {title_suffix}",
        severity=_incident_severity(fact),
        confidence=_confidence(fact.get("confidence_score"), 0.6),
        cluster_id=cluster_id,
        hits=hits,
        incident_hits=incident_hits,
        first_seen=_clip_text(fact.get("first_seen", ""), 80),
        last_seen=_clip_text(fact.get("last_seen", ""), 80),
        exception_type=_clip_text(exception, 160) or None,
        summary=f"{incident_hits} incident-like events out of {hits} hits in cluster {cluster_id}.",
        evidence=evidence,
        recommended_actions=[
            "Inspect representative stack frames and source files for the cluster.",
            "Check recent deploys or configuration changes affecting the failing component.",
        ],
        limitations=[],
    )


def _incident_observation_card(input_context: AgentInputContext) -> Optional[IncidentCard]:
    """Build a useful low-severity card when incidents profile has events but no strong clusters."""
    facts = _as_dict(input_context.facts)
    summary = _as_dict(facts.get("summary"))
    analysis = _as_dict(facts.get("analysis_summary"))
    event_count = _int_value(
        input_context.run_summary.get("event_count")
        or analysis.get("event_count")
        or summary.get("event_count")
    )
    cluster_count = _int_value(summary.get("cluster_count") or analysis.get("cluster_count"))
    if event_count <= 0 and cluster_count <= 0:
        return None
    evidence = [
        f"events={event_count}",
        f"clusters={cluster_count}",
    ]
    if analysis.get("parse_quality_label"):
        evidence.append(f"parse_quality={analysis.get('parse_quality_label')}")
    if analysis.get("incident_signal_label"):
        evidence.append(f"incident_signal={analysis.get('incident_signal_label')}")
    return IncidentCard(
        title="No strong incident cluster detected",
        severity="low",
        confidence=0.55,
        cluster_id="incidents-observation",
        hits=event_count,
        incident_hits=0,
        summary="The incidents profile found processed log activity but no strong incident cluster in the compact facts.",
        evidence=evidence,
        recommended_actions=[
            "Treat this as an operational observation unless external symptoms exist.",
            "Check parser/profile fit before escalating low-signal logs as incidents.",
        ],
        limitations=_fact_limitations(input_context),
    )


def _build_incidents_result(input_context: AgentInputContext, config: AgentModelConfig) -> AgentResult:
    """Build deterministic structured interpretation for incidents."""
    facts = _as_dict(input_context.facts)
    cluster_facts = _as_list(facts.get("compact_llm_ready_cluster_facts"), MAX_INCIDENT_CARDS)
    cards = [_incident_card_from_fact(_as_dict(item)) for item in cluster_facts]
    if not cards:
        observation_card = _incident_observation_card(input_context)
        if observation_card is not None:
            cards = [observation_card]
    limitations = _fact_limitations(input_context) + _provider_limitation(config)
    if cards:
        top = cards[0]
        short = (
            f"Top incident signal is {top.cluster_id} with {top.incident_hits} incident-like events."
            if top.incident_hits
            else "No strong incident cluster was detected; returned a low-severity operational observation."
        )
        technical = (
            f"Detected {len(cards)} notable incident clusters from "
            f"{input_context.run_summary.get('event_count', 0)} parsed events."
        )
        business = "Potential service reliability impact depends on the affected components and recurrence window."
        findings = [
            f"{card.cluster_id}: {card.incident_hits}/{card.hits} incident-like hits ({card.severity})."
            for card in cards
        ]
    else:
        short = "No notable incident clusters were found in the compact aggregate facts."
        technical = "The incidents profile did not produce cluster facts for interpretation."
        business = "No direct incident impact is visible from the processed aggregates."
        findings = ["No incident cards were produced."]
    severities = [card.severity for card in cards]
    confidence = _result_confidence(input_context, [card.confidence for card in cards])
    return AgentResult(
        enabled=True,
        status="completed",
        profile="incidents",
        overall_status=_status_from_severities(severities, bool(cards)),
        confidence=confidence,
        short_summary=short,
        technical_summary=technical,
        business_summary=business,
        key_findings=findings[:MAX_AGENT_LIST_ITEMS],
        recommended_actions=[
            "Prioritize clusters with high incident hit counts and recent first/last seen windows.",
            "Use source files, levels and stack frames from the cards to route ownership.",
        ],
        limitations=limitations,
        cards=cards,
        provider=config.provider,
        model=config.model,
    )


def _heatmap_severity(fact: Dict[str, Any]) -> str:
    """Derive heatmap severity from load and latency."""
    hits = _int_value(fact.get("hits"))
    qps = _float_value(fact.get("qps"))
    p95 = _float_value(fact.get("p95_latency_ms"), 0.0)
    if p95 >= 2000 or qps >= 20 or hits >= 200:
        return "critical"
    if p95 >= 1000 or qps >= 5 or hits >= 50:
        return "high"
    if hits > 0:
        return "medium"
    return "low"


def _heatmap_card_from_fact(fact: Dict[str, Any]) -> HeatmapCard:
    """Build one deterministic heatmap card."""
    operation = _clip_text(fact.get("operation", "unknown"), 220)
    bucket = _clip_text(fact.get("bucket_start", ""), 80)
    hits = _int_value(fact.get("hits"))
    p95 = _optional_float(fact.get("p95_latency_ms"))
    evidence = [
        f"hits={hits}",
        f"qps={_float_value(fact.get('qps')):.3f}",
    ]
    if p95 is not None:
        evidence.append(f"p95_latency_ms={p95}")
    return HeatmapCard(
        title=f"Hotspot: {operation}",
        severity=_heatmap_severity(fact),
        confidence=0.75 if hits else 0.4,
        bucket_start=bucket,
        component=_clip_text(fact.get("component", "unknown"), 160),
        operation=operation,
        hits=hits,
        qps=_float_value(fact.get("qps")),
        p95_latency_ms=p95,
        summary=f"{operation} produced {hits} hits in bucket {bucket}.",
        evidence=evidence,
        recommended_actions=[
            "Check whether the hotspot aligns with expected batch or peak traffic.",
            "Investigate latency contributors when p95 is elevated.",
        ],
        limitations=[],
    )


def _build_heatmap_result(input_context: AgentInputContext, config: AgentModelConfig) -> AgentResult:
    """Build deterministic structured interpretation for heatmap."""
    facts = _as_dict(input_context.facts)
    hotspots = _as_list(facts.get("hotspots"), MAX_HEATMAP_CARDS)
    cards = [_heatmap_card_from_fact(_as_dict(item)) for item in hotspots]
    limitations = _fact_limitations(input_context) + _provider_limitation(config)
    if cards:
        top = cards[0]
        short = f"Main hotspot is {top.operation} with {top.hits} hits in {top.bucket_start}."
        technical = f"Interpreted {len(cards)} hotspot buckets from heatmap aggregate rows."
        business = "Hotspots may indicate peak user load, batch jobs, retries, or degraded downstream dependencies."
        findings = [
            f"{card.bucket_start} {card.operation}: {card.hits} hits, p95={card.p95_latency_ms}."
            for card in cards
        ]
    else:
        short = "No heatmap hotspots were found in the compact aggregate facts."
        technical = "The heatmap profile did not produce hotspot rows for interpretation."
        business = "No direct load concentration is visible from the processed aggregates."
        findings = ["No heatmap cards were produced."]
    return AgentResult(
        enabled=True,
        status="completed",
        profile="heatmap",
        overall_status=_status_from_severities([card.severity for card in cards], bool(cards)),
        confidence=_result_confidence(input_context, [card.confidence for card in cards]),
        short_summary=short,
        technical_summary=technical,
        business_summary=business,
        key_findings=findings[:MAX_AGENT_LIST_ITEMS],
        recommended_actions=[
            "Compare hotspot buckets with expected traffic windows.",
            "Correlate slow hotspots with downstream service metrics and deploy history.",
        ],
        limitations=limitations,
        cards=cards,
        provider=config.provider,
        model=config.model,
    )


def _traffic_severity(fact: Dict[str, Any]) -> str:
    """Derive traffic severity from anomaly or endpoint facts."""
    explicit = str(fact.get("severity") or "").lower()
    if explicit in _SEVERITIES:
        return explicit
    status = _optional_int(fact.get("http_status"))
    p95 = _float_value(fact.get("p95_latency_ms"), 0.0)
    hits = _int_value(fact.get("hits"))
    if status is not None and status >= 500 and hits >= 10:
        return "critical"
    if status is not None and status >= 500:
        return "high"
    if p95 >= 1000:
        return "high"
    if hits > 0:
        return "medium"
    return "low"


def _traffic_card_from_anomaly(fact: Dict[str, Any]) -> TrafficCard:
    """Build one deterministic traffic card from an anomaly fact."""
    payload = _as_dict(fact.get("payload"))
    method = _clip_text(payload.get("method", ""), 32)
    path = _clip_text(payload.get("path", ""), 220)
    title = _clip_text(fact.get("title") or f"{method} {path}".strip() or "Traffic pattern", 220)
    hits = _int_value(payload.get("hits") or payload.get("request_count"))
    p95 = _optional_float(payload.get("p95_latency_ms"))
    return TrafficCard(
        title=title,
        severity=_traffic_severity({**payload, "severity": fact.get("severity")}),
        confidence=0.8,
        pattern_type=_clip_text(fact.get("anomaly_type", "traffic_pattern"), 80),
        method=method,
        path=path,
        http_status=_optional_int(payload.get("http_status")),
        hits=hits,
        unique_ips=_int_value(payload.get("unique_ips")),
        p95_latency_ms=p95,
        summary=_clip_text(fact.get("details") or title, 360),
        evidence=[_clip_text(fact.get("details", ""), 320)] if fact.get("details") else [],
        recommended_actions=[
            "Check endpoint error budgets, recent deploys and upstream dependencies.",
            "Review client IP distribution for retry storms or scanning behavior.",
        ],
        limitations=[],
    )


def _traffic_card_from_row(fact: Dict[str, Any]) -> TrafficCard:
    """Build one deterministic traffic card from an aggregate endpoint row."""
    method = _clip_text(fact.get("method", "UNKNOWN"), 32)
    path = _clip_text(fact.get("path", "unknown"), 220)
    status = _optional_int(fact.get("http_status"))
    hits = _int_value(fact.get("hits"))
    p95 = _optional_float(fact.get("p95_latency_ms"))
    title = f"Endpoint load: {method} {path}"
    evidence = [f"hits={hits}", f"unique_ips={_int_value(fact.get('unique_ips'))}"]
    if status is not None:
        evidence.append(f"http_status={status}")
    if p95 is not None:
        evidence.append(f"p95_latency_ms={p95}")
    return TrafficCard(
        title=title,
        severity=_traffic_severity(fact),
        confidence=0.72 if hits else 0.4,
        pattern_type="endpoint_load",
        method=method,
        path=path,
        http_status=status,
        hits=hits,
        unique_ips=_int_value(fact.get("unique_ips")),
        p95_latency_ms=p95,
        summary=f"{method} {path} had {hits} hits with status {status if status is not None else 'n/a'}.",
        evidence=evidence,
        recommended_actions=[
            "Validate whether the request volume and latency match expected traffic.",
            "Inspect 5xx or slow endpoints before lower-volume rows.",
        ],
        limitations=[],
    )


def _build_traffic_result(input_context: AgentInputContext, config: AgentModelConfig) -> AgentResult:
    """Build deterministic structured interpretation for traffic."""
    facts = _as_dict(input_context.facts)
    patterns = _as_list(facts.get("suspicious_patterns"), MAX_TRAFFIC_CARDS)
    cards: List[TrafficCard] = [_traffic_card_from_anomaly(_as_dict(item)) for item in patterns]
    if len(cards) < MAX_TRAFFIC_CARDS:
        traffic_findings = _as_dict(facts.get("traffic_findings"))
        for row in _as_list(traffic_findings.get("top_endpoints_by_hits"), MAX_TRAFFIC_CARDS):
            if len(cards) >= MAX_TRAFFIC_CARDS:
                break
            cards.append(_traffic_card_from_row(_as_dict(row)))
    limitations = _fact_limitations(input_context) + _provider_limitation(config)
    if cards:
        top = cards[0]
        short = f"Main traffic signal is {top.title} ({top.severity})."
        technical = f"Interpreted {len(cards)} traffic cards from endpoint and anomaly aggregates."
        business = "Traffic anomalies may reflect user-facing errors, elevated latency, retry storms or scanning behavior."
        findings = [
            f"{card.pattern_type}: {card.method} {card.path} hits={card.hits} severity={card.severity}."
            for card in cards
        ]
    else:
        short = "No traffic anomalies or notable endpoint rows were found in compact aggregate facts."
        technical = "The traffic profile did not produce interpretable rows for the agent stage."
        business = "No direct traffic risk is visible from the processed aggregates."
        findings = ["No traffic cards were produced."]
    return AgentResult(
        enabled=True,
        status="completed",
        profile="traffic",
        overall_status=_status_from_severities([card.severity for card in cards], bool(cards)),
        confidence=_result_confidence(input_context, [card.confidence for card in cards]),
        short_summary=short,
        technical_summary=technical,
        business_summary=business,
        key_findings=findings[:MAX_AGENT_LIST_ITEMS],
        recommended_actions=[
            "Prioritize high-severity cards, then validate latency and error spikes against service metrics.",
            "Review suspicious client patterns separately from normal high-volume endpoints.",
        ],
        limitations=limitations,
        cards=cards,
        provider=config.provider,
        model=config.model,
    )


def _build_deterministic_result(input_context: AgentInputContext, config: AgentModelConfig) -> AgentResult:
    """Build structured output without an LLM call."""
    if input_context.profile == "incidents":
        return _build_incidents_result(input_context, config)
    if input_context.profile == "heatmap":
        return _build_heatmap_result(input_context, config)
    if input_context.profile == "traffic":
        return _build_traffic_result(input_context, config)
    raise ValueError(f"Unsupported agent result profile: {input_context.profile}")


def _fallback_card_facts(input_context: AgentInputContext) -> List[Dict[str, Any]]:
    """Return deterministic card facts in the same order used for fallback cards."""
    facts = _as_dict(input_context.facts)
    if input_context.profile == "incidents":
        return [_as_dict(item) for item in _as_list(facts.get("compact_llm_ready_cluster_facts"), MAX_INCIDENT_CARDS)]
    if input_context.profile == "heatmap":
        return [_as_dict(item) for item in _as_list(facts.get("hotspots"), MAX_HEATMAP_CARDS)]
    if input_context.profile == "traffic":
        traffic_findings = _as_dict(facts.get("traffic_findings"))
        rows = [_as_dict(item) for item in _as_list(traffic_findings.get("top_endpoints_by_hits"), MAX_TRAFFIC_CARDS)]
        patterns = [_as_dict(item) for item in _as_list(facts.get("suspicious_patterns"), MAX_TRAFFIC_CARDS)]
        return (patterns + rows)[:MAX_TRAFFIC_CARDS]
    return []


def _fallback_by_card_key(input_context: AgentInputContext) -> Dict[str, Dict[str, Any]]:
    """Build profile-specific fallback facts for LLM card validation."""
    facts = _as_dict(input_context.facts)
    if input_context.profile == "incidents":
        return {
            _clip_text(_as_dict(item).get("cluster_id", ""), 120): _as_dict(item)
            for item in _as_list(facts.get("compact_llm_ready_cluster_facts"), MAX_INCIDENT_CARDS)
        }
    if input_context.profile == "heatmap":
        return {
            "|".join(
                [
                    _clip_text(_as_dict(item).get("bucket_start", ""), 80),
                    _clip_text(_as_dict(item).get("operation", ""), 160),
                ]
            ): _as_dict(item)
            for item in _as_list(facts.get("hotspots"), MAX_HEATMAP_CARDS)
        }
    if input_context.profile == "traffic":
        traffic_findings = _as_dict(facts.get("traffic_findings"))
        rows = _as_list(traffic_findings.get("top_endpoints_by_hits"), MAX_TRAFFIC_CARDS)
        return {
            "|".join(
                [
                    _clip_text(_as_dict(item).get("method", ""), 32),
                    _clip_text(_as_dict(item).get("path", ""), 160),
                    str(_as_dict(item).get("http_status")),
                ]
            ): _as_dict(item)
            for item in rows
        }
    return {}


def _has_fact_value(value: Any) -> bool:
    """Return whether a deterministic scalar fact is present."""
    if value is None or value == "" or value == []:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    return True


def _merge_card_payload(fallback: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
    """Merge LLM text with deterministic facts without letting empty model fields erase facts."""
    merged = dict(fallback)
    for key, value in payload.items():
        if value is None or value == "" or value == []:
            continue
        if key in _CARD_FACT_FIELDS and _has_fact_value(fallback.get(key)):
            if isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0:
                continue
        merged[key] = value
    return merged


def _validate_incident_card(payload: Dict[str, Any], fallback: Dict[str, Any]) -> IncidentCard:
    """Validate one LLM-produced incident card."""
    merged = _merge_card_payload(fallback, payload)
    deterministic = _incident_card_from_fact(merged)
    return IncidentCard(
        title=_clip_text(payload.get("title"), 220) or deterministic.title,
        severity=_severity_floor(payload.get("severity"), deterministic.severity),
        confidence=_confidence(payload.get("confidence"), deterministic.confidence),
        cluster_id=_clip_text(merged.get("cluster_id", ""), 120),
        hits=_int_value(merged.get("hits")),
        incident_hits=_int_value(merged.get("incident_hits")),
        first_seen=_clip_text(merged.get("first_seen", ""), 80),
        last_seen=_clip_text(merged.get("last_seen", ""), 80),
        exception_type=_clip_text(merged.get("exception_type"), 160) or None,
        summary=_clip_text(payload.get("summary"), 500) or deterministic.summary,
        evidence=_string_list(payload.get("evidence"), 5) or deterministic.evidence,
        recommended_actions=_string_list(payload.get("recommended_actions"), 5) or deterministic.recommended_actions,
        limitations=_string_list(payload.get("limitations"), 5) or deterministic.limitations,
    )


def _validate_heatmap_card(payload: Dict[str, Any], fallback: Dict[str, Any]) -> HeatmapCard:
    """Validate one LLM-produced heatmap card."""
    merged = _merge_card_payload(fallback, payload)
    deterministic = _heatmap_card_from_fact(merged)
    return HeatmapCard(
        title=_clip_text(payload.get("title"), 220) or deterministic.title,
        severity=_severity_floor(payload.get("severity"), deterministic.severity),
        confidence=_confidence(payload.get("confidence"), deterministic.confidence),
        bucket_start=_clip_text(merged.get("bucket_start", ""), 80),
        component=_clip_text(merged.get("component", ""), 160),
        operation=_clip_text(merged.get("operation", ""), 220),
        hits=_int_value(merged.get("hits")),
        qps=_float_value(merged.get("qps")),
        p95_latency_ms=_optional_float(merged.get("p95_latency_ms")),
        summary=_clip_text(payload.get("summary"), 500) or deterministic.summary,
        evidence=_string_list(payload.get("evidence"), 5) or deterministic.evidence,
        recommended_actions=_string_list(payload.get("recommended_actions"), 5) or deterministic.recommended_actions,
        limitations=_string_list(payload.get("limitations"), 5) or deterministic.limitations,
    )


def _validate_traffic_card(payload: Dict[str, Any], fallback: Dict[str, Any]) -> TrafficCard:
    """Validate one LLM-produced traffic card."""
    merged = _merge_card_payload(fallback, payload)
    deterministic = (
        _traffic_card_from_anomaly(merged)
        if merged.get("anomaly_type") or merged.get("payload")
        else _traffic_card_from_row(merged)
    )
    return TrafficCard(
        title=_clip_text(payload.get("title"), 220) or deterministic.title,
        severity=_severity_floor(payload.get("severity"), deterministic.severity),
        confidence=_confidence(payload.get("confidence"), deterministic.confidence),
        pattern_type=_clip_text(merged.get("pattern_type") or merged.get("anomaly_type") or "traffic_pattern", 80),
        method=_clip_text(merged.get("method", ""), 32),
        path=_clip_text(merged.get("path", ""), 220),
        http_status=_optional_int(merged.get("http_status")),
        hits=_int_value(merged.get("hits")),
        unique_ips=_int_value(merged.get("unique_ips")),
        p95_latency_ms=_optional_float(merged.get("p95_latency_ms")),
        summary=_clip_text(payload.get("summary"), 500) or deterministic.summary,
        evidence=_string_list(payload.get("evidence"), 5) or deterministic.evidence,
        recommended_actions=_string_list(payload.get("recommended_actions"), 5) or deterministic.recommended_actions,
        limitations=_string_list(payload.get("limitations"), 5) or deterministic.limitations,
    )


def _missing_card_text_counts(cards: List[Any]) -> Dict[str, int]:
    """Count cards with missing human-facing text sections."""
    return {
        "empty_summary": sum(1 for card in cards if not _clip_text(getattr(card, "summary", ""))),
        "empty_evidence": sum(1 for card in cards if not getattr(card, "evidence", [])),
        "empty_actions": sum(1 for card in cards if not getattr(card, "recommended_actions", [])),
    }


def _dedupe_strings(values: List[str]) -> List[str]:
    """Deduplicate short text lists while preserving order."""
    seen = set()
    result = []
    for value in values:
        text = _clip_text(value, 500)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _llm_payload_quality_gaps(payload: Dict[str, Any], cards_payload: List[Any]) -> Dict[str, Any]:
    """Return bounded diagnostics for incomplete but JSON-valid LLM payloads."""
    missing_top_fields = [
        field
        for field in ("short_summary", "technical_summary", "business_summary")
        if not _clip_text(payload.get(field))
    ]
    if not _string_list(payload.get("key_findings"), MAX_AGENT_LIST_ITEMS):
        missing_top_fields.append("key_findings")
    if not _string_list(payload.get("recommended_actions"), MAX_AGENT_LIST_ITEMS):
        missing_top_fields.append("recommended_actions")

    card_dicts = [_as_dict(item) for item in cards_payload]
    card_gaps = {
        "missing_title": sum(1 for item in card_dicts if not _clip_text(item.get("title"))),
        "missing_summary": sum(1 for item in card_dicts if not _clip_text(item.get("summary"))),
        "missing_evidence": sum(1 for item in card_dicts if not _string_list(item.get("evidence"), 5)),
        "missing_actions": sum(1 for item in card_dicts if not _string_list(item.get("recommended_actions"), 5)),
    }
    return {
        "missing_top_fields": missing_top_fields,
        "card_count": len(card_dicts),
        "card_gaps": card_gaps,
    }


def _has_llm_payload_quality_gaps(gaps: Dict[str, Any]) -> bool:
    """Return whether an LLM payload needs deterministic repair."""
    card_gaps = _as_dict(gaps.get("card_gaps"))
    return bool(gaps.get("missing_top_fields") or any(_int_value(value) for value in card_gaps.values()))


def _agent_context_shape(input_context: AgentInputContext) -> Dict[str, Any]:
    """Return small diagnostics for the compact facts payload."""
    facts = _as_dict(input_context.facts)
    shape: Dict[str, Any] = {
        "facts_keys": sorted(facts.keys()),
    }
    if input_context.profile == "incidents":
        shape["cluster_facts"] = len(_as_list(facts.get("compact_llm_ready_cluster_facts"), MAX_INCIDENT_CARDS))
        shape["cluster_candidates"] = len(_as_list(facts.get("top_cluster_candidates"), MAX_INCIDENT_CARDS))
    elif input_context.profile == "heatmap":
        shape["hotspots"] = len(_as_list(facts.get("hotspots"), MAX_HEATMAP_CARDS))
    elif input_context.profile == "traffic":
        findings = _as_dict(facts.get("traffic_findings"))
        shape["traffic_rows"] = len(_as_list(findings.get("top_endpoints_by_hits"), MAX_TRAFFIC_CARDS))
        shape["suspicious_patterns"] = len(_as_list(facts.get("suspicious_patterns"), MAX_TRAFFIC_CARDS))
    return shape


def validate_agent_result_payload(
    payload: Dict[str, Any],
    input_context: AgentInputContext,
    config: AgentModelConfig,
) -> AgentResult:
    """Validate one structured LLM payload into the public `AgentResult` contract."""
    if not isinstance(payload, dict):
        raise ValueError("Agent LLM payload must be a JSON object.")
    profile = _clip_text(payload.get("profile") or input_context.profile, 40)
    if profile != input_context.profile:
        raise ValueError(f"Agent LLM profile mismatch: {profile} != {input_context.profile}")

    fallback_by_key = _fallback_by_card_key(input_context)
    fallback_facts = _fallback_card_facts(input_context)
    cards_payload = _as_list(payload.get("cards"), MAX_AGENT_LIST_ITEMS)
    logger.info(
        "agent_llm_payload_shape: profile=%s keys=%s cards=%d findings=%d actions=%d limitations=%d",
        profile,
        sorted(payload.keys()),
        len(cards_payload),
        len(_as_list(payload.get("key_findings"), MAX_AGENT_LIST_ITEMS)),
        len(_as_list(payload.get("recommended_actions"), MAX_AGENT_LIST_ITEMS)),
        len(_as_list(payload.get("limitations"), MAX_AGENT_LIST_ITEMS)),
    )
    quality_gaps = _llm_payload_quality_gaps(payload, cards_payload)
    llm_payload_degraded = _has_llm_payload_quality_gaps(quality_gaps)
    if llm_payload_degraded:
        logger.warning(
            "agent_llm_payload_degraded: profile=%s gaps=%s",
            input_context.profile,
            quality_gaps,
        )
    cards = []
    matched_fallbacks = 0
    positional_fallbacks = 0
    missed_fallbacks = 0
    for index, item in enumerate(cards_payload):
        item = _as_dict(item)
        if input_context.profile == "incidents":
            fallback = fallback_by_key.get(_clip_text(item.get("cluster_id", ""), 120), {})
            if not fallback and index < len(fallback_facts):
                fallback = fallback_facts[index]
                positional_fallbacks += 1
            matched_fallbacks += 1 if fallback else 0
            missed_fallbacks += 0 if fallback else 1
            cards.append(_validate_incident_card(item, fallback))
        elif input_context.profile == "heatmap":
            key = "|".join([
                _clip_text(item.get("bucket_start", ""), 80),
                _clip_text(item.get("operation", ""), 160),
            ])
            fallback = fallback_by_key.get(key, {})
            if not fallback and index < len(fallback_facts):
                fallback = fallback_facts[index]
                positional_fallbacks += 1
            matched_fallbacks += 1 if fallback else 0
            missed_fallbacks += 0 if fallback else 1
            cards.append(_validate_heatmap_card(item, fallback))
        elif input_context.profile == "traffic":
            key = "|".join([
                _clip_text(item.get("method", ""), 32),
                _clip_text(item.get("path", ""), 160),
                str(item.get("http_status")),
            ])
            fallback = fallback_by_key.get(key, {})
            if not fallback and index < len(fallback_facts):
                fallback = fallback_facts[index]
                positional_fallbacks += 1
            matched_fallbacks += 1 if fallback else 0
            missed_fallbacks += 0 if fallback else 1
            cards.append(_validate_traffic_card(item, fallback))

    status = str(payload.get("overall_status") or "").lower()
    if status not in _OVERALL_STATUSES:
        status = _status_from_severities([card.severity for card in cards], bool(cards))
    limitations = _string_list(payload.get("limitations"), MAX_AGENT_LIST_ITEMS)
    limitations.extend(_fact_limitations(input_context))
    if llm_payload_degraded:
        limitations.append("LLM response was incomplete; missing text fields were filled from deterministic compact facts.")
    fallback_result = None
    if not cards:
        fallback_result = _build_deterministic_result(input_context, config)
        if fallback_result.cards:
            logger.warning(
                "agent_llm_empty_cards_fallback: profile=%s deterministic_cards=%d",
                input_context.profile,
                len(fallback_result.cards),
            )
            cards = fallback_result.cards
            status = _status_from_severities([card.severity for card in cards], True)
    if quality_gaps.get("missing_top_fields"):
        if fallback_result is None:
            fallback_result = _build_deterministic_result(input_context, config)
        logger.warning(
            "agent_llm_text_fallback: profile=%s missing_top_fields=%s short=%s technical=%s business=%s findings=%d actions=%d",
            input_context.profile,
            quality_gaps.get("missing_top_fields"),
            bool(_clip_text(payload.get("short_summary"))),
            bool(_clip_text(payload.get("technical_summary"))),
            bool(_clip_text(payload.get("business_summary"))),
            len(_string_list(payload.get("key_findings"), MAX_AGENT_LIST_ITEMS)),
            len(_string_list(payload.get("recommended_actions"), MAX_AGENT_LIST_ITEMS)),
        )
    missing_counts = _missing_card_text_counts(cards)
    if any(missing_counts.values()):
        if fallback_result is None:
            fallback_result = _build_deterministic_result(input_context, config)
        if fallback_result.cards:
            logger.warning(
                "agent_cards_deterministic_replace: profile=%s cards=%d missing=%s",
                input_context.profile,
                len(fallback_result.cards),
                missing_counts,
            )
            cards = fallback_result.cards
            status = _status_from_severities([card.severity for card in cards], True)
            missing_counts = _missing_card_text_counts(cards)
    if any(missing_counts.values()):
        logger.warning(
            "agent_card_text_gaps: profile=%s cards=%d fallbacks_applied=%d positional_fallbacks=%d missed_fallbacks=%d %s",
            input_context.profile,
            len(cards),
            matched_fallbacks,
            positional_fallbacks,
            missed_fallbacks,
            missing_counts,
        )
    else:
        logger.info(
            "agent_cards_validated: profile=%s cards=%d fallbacks_applied=%d positional_fallbacks=%d missed_fallbacks=%d",
            input_context.profile,
            len(cards),
            matched_fallbacks,
            positional_fallbacks,
            missed_fallbacks,
        )
    status = _status_floor(status, cards, input_context.profile)
    return AgentResult(
        enabled=True,
        status="completed",
        profile=input_context.profile,
        overall_status=status,
        confidence=_confidence(payload.get("confidence"), _result_confidence(input_context, [card.confidence for card in cards])),
        short_summary=_clip_text(payload.get("short_summary"), 500) or (fallback_result.short_summary if fallback_result else ""),
        technical_summary=_clip_text(payload.get("technical_summary"), 1200) or (fallback_result.technical_summary if fallback_result else ""),
        business_summary=_clip_text(payload.get("business_summary"), 1000) or (fallback_result.business_summary if fallback_result else ""),
        key_findings=_string_list(payload.get("key_findings"), MAX_AGENT_LIST_ITEMS)
        or (fallback_result.key_findings if fallback_result else []),
        recommended_actions=_string_list(payload.get("recommended_actions"), MAX_AGENT_LIST_ITEMS)
        or (fallback_result.recommended_actions if fallback_result else []),
        limitations=_dedupe_strings(limitations or (fallback_result.limitations if fallback_result else []))[:MAX_AGENT_LIST_ITEMS],
        cards=cards,
        provider=config.provider,
        model=config.model,
        mode="llm",
        used_llm=True,
        used_fallback=False,
        schema_valid=True,
        repair_applied=llm_payload_degraded,
    )


def _extract_json_object(content: str) -> Dict[str, Any]:
    """Parse a JSON object from a model response, tolerating fenced JSON."""
    text = content.strip()
    if text.startswith("```"):
        lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
        text = "\n".join(lines).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        payload = json.loads(text[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("Model response JSON must be an object.")
    return payload


def _message_content(value: Any) -> str:
    """Extract text from OpenAI-like message content shapes."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("content") or ""))
            else:
                parts.append(str(item))
        return "".join(parts)
    return str(value or "")


def _invoke_structured_llm(config: AgentModelConfig, messages: List[Dict[str, str]]) -> Dict[str, Any]:
    """Call one OpenAI-compatible chat-completions endpoint and parse JSON."""
    if not provider_is_configured(config):
        raise RuntimeError(f"Agent provider {config.provider} is not configured.")
    url = config.base_url.rstrip("/")
    if not url.endswith("/chat/completions"):
        url = f"{url}/chat/completions"
    body = json.dumps(
        {
            "model": config.model,
            "messages": messages,
            "response_format": {"type": "json_object"},
            "temperature": 0,
            "max_tokens": _LLM_MAX_TOKENS,
        },
        ensure_ascii=False,
    ).encode("utf-8")
    logger.info(
        "agent_llm_request: provider=%s model=%s url=%s timeout=%.1fs messages=%d body_bytes=%d max_tokens=%d",
        config.provider,
        config.model,
        url,
        config.timeout_seconds,
        len(messages),
        len(body),
        _LLM_MAX_TOKENS,
    )
    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    if config.provider == "yandex" and config.folder_id:
        headers["OpenAI-Project"] = config.folder_id
        headers["x-folder-id"] = config.folder_id
    http_request = request.Request(url, data=body, headers=headers, method="POST")
    try:
        with request.urlopen(http_request, timeout=config.timeout_seconds) as response:
            status = getattr(response, "status", "unknown")
            raw_body = response.read().decode("utf-8")
    except urlerror.HTTPError as error:
        raw_error_body = error.read(1200).decode("utf-8", errors="replace")
        logger.warning(
            "agent_llm_http_error: provider=%s model=%s status=%s body=%s",
            config.provider,
            config.model,
            error.code,
            _clip_text(raw_error_body, 500),
        )
        raise RuntimeError(f"LLM HTTP {error.code}: {_clip_text(raw_error_body, 240)}") from error
    except urlerror.URLError as error:
        logger.warning(
            "agent_llm_url_error: provider=%s model=%s url=%s error=%s",
            config.provider,
            config.model,
            url,
            _clip_text(error.reason, 240),
        )
        raise RuntimeError(f"LLM network error: {_clip_text(error.reason, 240)}") from error
    except TimeoutError as error:
        logger.warning(
            "agent_llm_timeout: provider=%s model=%s url=%s timeout=%.1fs",
            config.provider,
            config.model,
            url,
            config.timeout_seconds,
        )
        raise RuntimeError(f"LLM timeout after {config.timeout_seconds:.1f}s") from error

    logger.info(
        "agent_llm_response: provider=%s model=%s status=%s body_chars=%d",
        config.provider,
        config.model,
        status,
        len(raw_body),
    )
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as error:
        raise ValueError(f"Agent LLM response is not JSON: {_clip_text(raw_body, 240)}") from error
    choices = payload.get("choices") or []
    if not choices:
        raise ValueError("Agent LLM response did not contain choices.")
    message = _as_dict(_as_dict(choices[0]).get("message"))
    content = _message_content(message.get("content"))
    if not content:
        raise ValueError("Agent LLM response content is empty.")
    logger.info(
        "agent_llm_content: provider=%s model=%s chars=%d preview=%s",
        config.provider,
        config.model,
        len(content),
        _clip_text(content, 500),
    )
    return _extract_json_object(content)


def run_agent_stage(context: PipelineContext) -> PipelineContext:
    """Run one profile-aware structured agent stage over compact deterministic facts."""
    _ensure_agent_logging()
    input_context = context.agent_input_context
    if input_context is None:
        raise RuntimeError("Agent input context must be built before the agent stage.")

    started = time.perf_counter()
    model_config = resolve_agent_model_config(context.config.agent_provider)
    logger.info(
        "agent_stage_start: run_id=%s profile=%s provider=%s configured=%s has_api_key=%s has_folder_id=%s model=%s url=%s timeout=%.1fs context_shape=%s",
        context.run_id,
        input_context.profile,
        model_config.provider,
        provider_is_configured(model_config),
        bool(model_config.api_key),
        bool(model_config.folder_id),
        model_config.model,
        model_config.base_url,
        model_config.timeout_seconds,
        _agent_context_shape(input_context),
    )
    llm_error: Optional[str] = None
    result: AgentResult
    if model_config.provider != "none" and provider_is_configured(model_config):
        try:
            messages = build_agent_messages(input_context)
            prompt_chars = sum(len(message.get("content", "")) for message in messages)
            facts_chars = len(json.dumps(input_context.facts, ensure_ascii=False))
            logger.info(
                "agent_prompt_built: run_id=%s profile=%s messages=%d prompt_chars=%d facts_chars=%d",
                context.run_id,
                input_context.profile,
                len(messages),
                prompt_chars,
                facts_chars,
            )
            llm_payload = _invoke_structured_llm(model_config, messages)
            result = validate_agent_result_payload(llm_payload, input_context, model_config)
        except Exception as error:
            llm_error = f"{type(error).__name__}: {error}"
            logger.warning("agent_llm_fallback: run_id=%s error=%s", context.run_id, llm_error)
            result = _build_deterministic_result(input_context, model_config)
            result.error = llm_error
            result.mode = "fallback"
            result.used_fallback = True
            result.schema_valid = False
            result.limitations = (result.limitations + [f"Structured LLM call failed: {llm_error}"])[:MAX_AGENT_LIST_ITEMS]
    else:
        if model_config.provider != "none":
            logger.info(
                "agent_llm_skipped: provider=%s model=%s url=%s timeout=%.1fs reason=not_configured",
                model_config.provider,
                model_config.model,
                model_config.base_url,
                model_config.timeout_seconds,
            )
        result = _build_deterministic_result(input_context, model_config)

    result.duration_seconds = time.perf_counter() - started
    result_card_gaps = _missing_card_text_counts(result.cards)
    if (
        not result.short_summary
        or not result.technical_summary
        or not result.business_summary
        or not result.key_findings
        or not result.recommended_actions
        or any(result_card_gaps.values())
    ):
        logger.warning(
            "agent_result_quality_gaps: run_id=%s profile=%s short=%s technical=%s business=%s findings=%d actions=%d cards=%d card_gaps=%s provider=%s error=%s",
            context.run_id,
            input_context.profile,
            bool(result.short_summary),
            bool(result.technical_summary),
            bool(result.business_summary),
            len(result.key_findings),
            len(result.recommended_actions),
            len(result.cards),
            result_card_gaps,
            result.provider,
            result.error,
        )
    else:
        logger.info(
            "agent_result_ready: run_id=%s profile=%s status=%s confidence=%.3f cards=%d findings=%d actions=%d",
            context.run_id,
            input_context.profile,
            result.overall_status,
            result.confidence,
            len(result.cards),
            len(result.key_findings),
            len(result.recommended_actions),
        )
    result.artifact_paths = {}
    context.agent_result = result
    context.timings["agent_stage"] = result.duration_seconds
    logger.info(
        "agent_stage_finished: run_id=%s profile=%s provider=%s cards=%d duration=%.3fs",
        context.run_id,
        input_context.profile,
        model_config.provider,
        len(result.cards),
        result.duration_seconds,
    )
    return context


def build_agent_summary(result: AgentResult) -> Dict[str, Any]:
    """Build the compact agent summary persisted in `run_summary.json`."""
    return {
        "overall_status": result.overall_status,
        "confidence": round(float(result.confidence), 3),
        "card_count": len(result.cards),
        "short_summary": result.short_summary,
        "provider": result.provider,
        "mode": result.mode,
        "used_llm": result.used_llm,
        "used_fallback": result.used_fallback,
        "schema_valid": result.schema_valid,
        "repair_applied": result.repair_applied,
    }


def _update_agent_run_outputs(context: PipelineContext) -> PipelineContext:
    """Update in-memory run summary after the structured agent result is built."""
    if context.run_summary is None:
        raise RuntimeError("Run summary must exist before agent pipeline output updates.")
    agent_result = context.agent_result
    if agent_result is None:
        return context

    context.run_summary["trace_summary"]["timings_seconds"] = {
        name: round(value, 3) for name, value in context.timings.items()
    }
    context.run_summary["agent_summary"] = build_agent_summary(agent_result)
    context.run_summary.pop("agent_result", None)
    return context


def run_agent_pipeline(context: PipelineContext) -> PipelineContext:
    """Run the complete mandatory interpretation stage over deterministic profile outputs."""
    _ensure_agent_logging()
    context = build_agent_input_context(context)
    context = run_agent_stage(context)
    return _update_agent_run_outputs(context)


__all__ = [
    "build_agent_summary",
    "run_agent_pipeline",
    "run_agent_stage",
    "validate_agent_result_payload",
]
