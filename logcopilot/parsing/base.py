from __future__ import annotations

from abc import ABC, abstractmethod

from .models import ParseResult


class BaseParser(ABC):
    """Base contract for parser implementations."""

    name: str

    @abstractmethod
    def can_parse(self, sample: str) -> float:
        """Return detector confidence in range [0.0, 1.0]."""

    @abstractmethod
    def parse(self, text: str, source: str | None = None) -> ParseResult:
        """Parse full text payload into canonical events."""

