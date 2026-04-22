from __future__ import annotations

"""Text normalization: regex masking of PII, IDs and timestamps."""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
import re
from typing import Dict, Iterable, List, Optional, Tuple

UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
IPV6_RE = re.compile(r"(?<![\w:])(?:[0-9a-fA-F]{1,4}:){2,}[0-9a-fA-F]{1,4}(?![\w:])")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b")
HEX_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
LONG_HEX_RE = re.compile(r"\b[a-f0-9]{16,}\b", re.IGNORECASE)
LONG_ID_RE = re.compile(r"\b\d{4,}\b")
DATETIME_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[,.]\d+)?\b"
)
DATE_ONLY_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
TIME_ONLY_RE = re.compile(r"\b\d{2}:\d{2}:\d{2}(?:[,.]\d+)?\b")
REQUEST_ID_RE = re.compile(r"(\brequestid\b\s*[:=]?\s*)(\S+)", re.IGNORECASE)
TRACE_ID_RE = re.compile(
    r"(\b(?:traceid|correlationid|activityid|connectionid)\b\s*[:=]?\s*)(\S+)",
    re.IGNORECASE,
)
TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_-]{24,}\b")
WHITESPACE_RE = re.compile(r"\s+")

MASK_SPECS: List[Tuple[str, re.Pattern[str], str]] = [
    ("UUID", UUID_RE, "<UUID>"),
    ("DATETIME", DATETIME_RE, "<DATETIME>"),
    ("DATE", DATE_ONLY_RE, "<DATE>"),
    ("TIME", TIME_ONLY_RE, "<TIME>"),
    ("IP", IPV4_RE, "<IP>"),
    ("IP", IPV6_RE, "<IP>"),
    ("EMAIL", EMAIL_RE, "<EMAIL>"),
    ("JWT", JWT_RE, "<JWT>"),
    ("HEX", HEX_RE, "<HEX>"),
    ("HEX", LONG_HEX_RE, "<HEX>"),
    ("REQ_ID", REQUEST_ID_RE, r"\1<REQ_ID>"),
    ("TRACE_ID", TRACE_ID_RE, r"\1<TRACE_ID>"),
    ("NUM", LONG_ID_RE, "<NUM>"),
    ("TOKEN", TOKENISH_RE, "<TOKEN>"),
]
MASK_TOKEN_NAMES = ("UUID", "IP", "EMAIL", "JWT", "HEX", "REQ_ID", "TRACE_ID", "NUM", "TOKEN")


@dataclass
class NormalizationStats:
    """Accumulator with counts and examples of applied normalization masks."""

    mask_counts: Counter[str] = field(default_factory=Counter)
    raw_patterns: Dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))
    total_events: int = 0

    def observe_mask(self, mask_name: str, raw_value: str) -> None:
        """Record one applied mask and its raw value preview.

        Args:
            mask_name: Logical mask identifier.
            raw_value: Raw matched text before replacement.

        Returns:
            None.
        """
        self.mask_counts[mask_name] += 1
        preview = WHITESPACE_RE.sub(" ", raw_value.strip())[:120]
        if preview:
            self.raw_patterns[mask_name][preview] += 1

    def snapshot(self, top_n: int = 10) -> dict:
        """Build a serializable snapshot of collected normalization statistics.

        Args:
            top_n: Maximum number of sample raw patterns to keep per mask.

        Returns:
            Snapshot dictionary for reporting and diagnostics.
        """
        return {
            "mask_counts": dict(self.mask_counts),
            "top_replaced_patterns": {
                name: patterns.most_common(top_n)
                for name, patterns in self.raw_patterns.items()
            },
        }


def _apply_mask(
    text: str,
    mask_name: str,
    pattern: re.Pattern[str],
    replacement: str,
    stats: Optional[NormalizationStats],
) -> str:
    """Apply one regex mask and optionally record mask statistics.

    Args:
        text: Source text to normalize.
        mask_name: Logical mask identifier.
        pattern: Regex pattern to replace.
        replacement: Replacement template.
        stats: Optional stats accumulator.

    Returns:
        Text after applying the mask replacement.
    """
    if stats is None:
        return pattern.sub(replacement, text)

    def replacer(match: re.Match[str]) -> str:
        stats.observe_mask(mask_name, match.group(0))
        return match.expand(replacement)

    return pattern.sub(replacer, text)


def normalize_text(text: str, stats: Optional[NormalizationStats] = None) -> str:
    """Normalize free-form text by masking identifiers and collapsing whitespace.

    Args:
        text: Source text to normalize.
        stats: Optional stats accumulator for observed masks.

    Returns:
        Lowercased normalized text with masked volatile fragments.
    """
    normalized = text or ""
    if stats is not None:
        stats.total_events += 1
    for mask_name, pattern, replacement in MASK_SPECS:
        normalized = _apply_mask(normalized, mask_name, pattern, replacement, stats)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip().lower()
    return normalized


def count_mask_tokens(texts: Iterable[str]) -> Counter[str]:
    """Count how many mask tokens appear across normalized texts.

    Args:
        texts: Normalized texts to inspect.

    Returns:
        Counter keyed by logical mask token name.
    """
    counts: Counter[str] = Counter()
    for text in texts:
        upper = (text or "").upper()
        for token_name in MASK_TOKEN_NAMES:
            token = f"<{token_name}>"
            counts[token_name] += upper.count(token)
    return counts
