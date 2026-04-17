from __future__ import annotations

from dataclasses import dataclass, field
import re


ORDINAL_TO_INDEX = {
    "1": 0,
    "1й": 0,
    "1-й": 0,
    "перв": 0,
    "2": 1,
    "2й": 1,
    "2-й": 1,
    "втор": 1,
    "3": 2,
    "3й": 2,
    "3-й": 2,
    "трет": 2,
}


@dataclass
class AgentSessionState:
    focused_cluster_id: str | None = None
    last_ranked_cluster_ids: list[str] = field(default_factory=list)
    last_top_cluster_ids: list[str] = field(default_factory=list)
    last_topic: str | None = None
    last_profile_scope: str | None = None
    last_action: str | None = None
    turn_index: int = 0

    def to_dict(self) -> dict:
        return {
            "focused_cluster_id": self.focused_cluster_id,
            "last_ranked_cluster_ids": list(self.last_ranked_cluster_ids),
            "last_top_cluster_ids": list(self.last_ranked_cluster_ids or self.last_top_cluster_ids),
            "last_topic": self.last_topic,
            "last_profile_scope": self.last_profile_scope,
            "last_action": self.last_action,
            "turn_index": self.turn_index,
        }

    @classmethod
    def from_dict(cls, payload: dict | None) -> "AgentSessionState":
        if not payload:
            return cls()
        return cls(
            focused_cluster_id=payload.get("focused_cluster_id"),
            last_ranked_cluster_ids=list(
                payload.get("last_ranked_cluster_ids", payload.get("last_top_cluster_ids", []))
            ),
            last_top_cluster_ids=list(payload.get("last_top_cluster_ids", payload.get("last_ranked_cluster_ids", []))),
            last_topic=payload.get("last_topic"),
            last_profile_scope=payload.get("last_profile_scope"),
            last_action=payload.get("last_action"),
            turn_index=int(payload.get("turn_index", 0)),
        )


def resolve_cluster_index_from_question(question: str) -> int | None:
    question_lower = (question or "").lower()
    for token, index in ORDINAL_TO_INDEX.items():
        if token in question_lower:
            return index
    return None
