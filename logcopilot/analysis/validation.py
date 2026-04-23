from __future__ import annotations

"""Pipeline execution quality validation."""

from typing import Any, Dict, List

from ..domain import ExecutionQuality, PipelineContext


def _dict(value: Any) -> Dict[str, Any]:
    """Return a mapping value or an empty mapping."""
    return dict(value) if isinstance(value, dict) else {}


def _float(value: Any, default: float = 0.0) -> float:
    """Coerce a value to float with a safe fallback."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _label(value: Any) -> str:
    """Normalize a label-like value."""
    return str(value or "").lower()


def _missing_card_sections(cards: List[Any]) -> Dict[str, int]:
    """Count cards missing product-facing sections."""
    return {
        "empty_summary": sum(1 for card in cards if not getattr(card, "summary", "")),
        "empty_evidence": sum(1 for card in cards if not getattr(card, "evidence", [])),
        "empty_actions": sum(1 for card in cards if not getattr(card, "recommended_actions", [])),
    }


def _add_reason(
    reasons: List[str],
    recommendations: List[str],
    reason: str,
    recommendation: str,
) -> None:
    """Append one reason and recommendation pair."""
    reasons.append(reason)
    if recommendation not in recommendations:
        recommendations.append(recommendation)


def run_quality_validation(context: PipelineContext) -> PipelineContext:
    """Validate whether the pipeline output is trustworthy and useful."""
    run_summary = _dict(context.run_summary)
    parser_diagnostics = _dict(run_summary.get("parser_diagnostics"))
    parse_quality = _dict(parser_diagnostics.get("parse_quality"))
    incident_signal = _dict(parser_diagnostics.get("incident_signal_quality"))
    profile_fit = _dict(run_summary.get("profile_fit"))
    agent_result = context.agent_result
    cards = list(agent_result.cards) if agent_result is not None else []

    parse_score = _float(parse_quality.get("score"))
    fallback_ratio = _float(parser_diagnostics.get("fallback_ratio"))
    parser_confidence = _float(parser_diagnostics.get("mean_parser_confidence"))
    profile_fit_label = _label(profile_fit.get("fit_label"))
    incident_signal_label = _label(incident_signal.get("label"))
    missing_card_sections = _missing_card_sections(cards)

    signals = {
        "event_count": run_summary.get("event_count", len(context.events)),
        "parse_quality": parse_quality,
        "parser_confidence": parser_confidence,
        "fallback_ratio": fallback_ratio,
        "profile_fit": profile_fit,
        "incident_signal_quality": incident_signal,
        "agent": {
            "present": agent_result is not None,
            "status": getattr(agent_result, "status", "missing"),
            "mode": getattr(agent_result, "mode", "missing"),
            "provider": getattr(agent_result, "provider", "missing"),
            "used_llm": getattr(agent_result, "used_llm", False),
            "used_fallback": getattr(agent_result, "used_fallback", False),
            "schema_valid": getattr(agent_result, "schema_valid", False),
            "repair_applied": getattr(agent_result, "repair_applied", False),
            "error": getattr(agent_result, "error", None),
        },
        "cards": {
            "count": len(cards),
            "missing_sections": missing_card_sections,
        },
    }

    score = 1.0
    status = "ok"
    reasons: List[str] = []
    recommendations: List[str] = []

    if signals["event_count"] <= 0:
        status = "failed"
        score = 0.0
        _add_reason(
            reasons,
            recommendations,
            "No events were parsed from the input.",
            "Check parser selection, input encoding and whether the file contains supported log records.",
        )

    if agent_result is None or getattr(agent_result, "status", "") != "completed":
        status = "failed"
        score = min(score, 0.2)
        _add_reason(
            reasons,
            recommendations,
            "Agent interpretation stage did not produce a completed result.",
            "Keep deterministic interpretation enabled and inspect agent stage errors.",
        )

    if status != "failed":
        if parse_score < 0.55 or _label(parse_quality.get("label")) == "low":
            status = "weak"
            score -= 0.25
            _add_reason(
                reasons,
                recommendations,
                "Parse quality is low.",
                "Improve parser coverage or inspect fallback parser usage for this log format.",
            )
        elif parse_score < 0.75:
            status = "degraded"
            score -= 0.1
            _add_reason(
                reasons,
                recommendations,
                "Parse quality is moderate.",
                "Review parser diagnostics before relying on fine-grained fields.",
            )

        if fallback_ratio >= 0.7:
            status = "weak"
            score -= 0.25
            _add_reason(
                reasons,
                recommendations,
                "Most events came from fallback parsing.",
                "Add or improve a specific parser for this log source.",
            )
        elif fallback_ratio >= 0.3 and status == "ok":
            status = "degraded"
            score -= 0.1
            _add_reason(
                reasons,
                recommendations,
                "Fallback parsing was used for a meaningful share of events.",
                "Review fallback samples before integrating this output automatically.",
            )

        if profile_fit_label == "low":
            status = "weak"
            score -= 0.2
            _add_reason(
                reasons,
                recommendations,
                "Selected profile has low fit for the parsed event structure.",
                "Use the recommended profile from profile_fit or adjust profile selection rules.",
            )
        elif profile_fit_label == "medium" and status == "ok":
            status = "degraded"
            score -= 0.1
            _add_reason(
                reasons,
                recommendations,
                "Selected profile fit is only medium.",
                "Treat findings as directional until profile selection is confirmed.",
            )

        if context.config.profile == "incidents" and incident_signal_label == "low":
            status = "weak"
            score -= 0.15
            _add_reason(
                reasons,
                recommendations,
                "Incident signal quality is low for the incidents profile.",
                "Do not escalate low-signal incident findings without external symptoms.",
            )

        if getattr(agent_result, "used_fallback", False):
            if status == "ok":
                status = "degraded"
            score -= 0.1
            _add_reason(
                reasons,
                recommendations,
                "Agent interpretation used deterministic fallback after an LLM failure.",
                "Inspect agent error and retry with a configured provider if natural-language enrichment is required.",
            )

        if getattr(agent_result, "repair_applied", False) or not getattr(agent_result, "schema_valid", True):
            if status == "ok":
                status = "degraded"
            score -= 0.1
            _add_reason(
                reasons,
                recommendations,
                "Agent output required schema repair or validation fallback.",
                "Check agent output schema compliance before using LLM-enriched text downstream.",
            )

        if not cards:
            status = "weak"
            score -= 0.25
            _add_reason(
                reasons,
                recommendations,
                "No finding cards were produced.",
                "Inspect profile aggregates and agent facts; external systems should not consume empty findings blindly.",
            )
        elif any(missing_card_sections.values()):
            status = "weak" if status != "failed" else status
            score -= 0.15
            _add_reason(
                reasons,
                recommendations,
                "Some finding cards are missing summary, evidence or actions.",
                "Repair card generation before presenting findings as final operational output.",
            )

    score = max(0.0, min(1.0, score))
    if not reasons and status == "ok":
        reasons.append("Pipeline produced parsed events, profile analytics and complete finding cards.")
        recommendations.append("Output is suitable for downstream integration.")

    context.execution_quality = ExecutionQuality(
        status=status,
        score=score,
        signals=signals,
        reasons=reasons,
        recommendations=recommendations,
    )
    return context


__all__ = ["run_quality_validation"]
