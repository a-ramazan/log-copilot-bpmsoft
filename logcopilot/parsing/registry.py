from __future__ import annotations

from dataclasses import dataclass

from .base import BaseParser
from .models import ParserSelection


@dataclass
class RegisteredParser:
    parser: BaseParser
    is_fallback: bool = False


class ParserRegistry:
    """Registry for parser implementations and selection rules."""

    def __init__(self, fallback_threshold: float = 0.45) -> None:
        self.fallback_threshold = fallback_threshold
        self._parsers: list[RegisteredParser] = []

    def register(self, parser: BaseParser, *, is_fallback: bool = False) -> None:
        self._parsers.append(RegisteredParser(parser=parser, is_fallback=is_fallback))

    def get_fallback(self) -> BaseParser:
        for entry in self._parsers:
            if entry.is_fallback:
                return entry.parser
        raise LookupError("ParserRegistry requires a fallback parser")

    def select(self, sample: str) -> tuple[BaseParser, ParserSelection]:
        fallback = self.get_fallback()
        scored: list[tuple[float, RegisteredParser]] = []
        for entry in self._parsers:
            if entry.is_fallback:
                continue
            confidence = entry.parser.can_parse(sample)
            scored.append((confidence, entry))
        if not scored:
            return fallback, ParserSelection(parser_name=fallback.name, confidence=0.0, used_fallback=True)
        best_score, best_entry = max(scored, key=lambda item: item[0])
        if best_score < self.fallback_threshold:
            return fallback, ParserSelection(parser_name=fallback.name, confidence=best_score, used_fallback=True)
        return best_entry.parser, ParserSelection(parser_name=best_entry.parser.name, confidence=best_score, used_fallback=False)

