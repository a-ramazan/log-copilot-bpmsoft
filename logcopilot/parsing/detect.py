from __future__ import annotations

from .models import ParserSelection
from .registry import ParserRegistry
from .utils import read_detection_sample


def detect_parser(text: str, registry: ParserRegistry) -> ParserSelection:
    """Run parser detection on a sample extracted from text."""

    sample = read_detection_sample(text)
    _, selection = registry.select(sample)
    return selection

