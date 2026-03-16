import re

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


def normalize_text(text: str) -> str:
    normalized = text or ""
    normalized = UUID_RE.sub("<UUID>", normalized)
    normalized = DATETIME_RE.sub("<DATETIME>", normalized)
    normalized = DATE_ONLY_RE.sub("<DATE>", normalized)
    normalized = TIME_ONLY_RE.sub("<TIME>", normalized)
    normalized = IPV4_RE.sub("<IP>", normalized)
    normalized = IPV6_RE.sub("<IP>", normalized)
    normalized = EMAIL_RE.sub("<EMAIL>", normalized)
    normalized = JWT_RE.sub("<JWT>", normalized)
    normalized = HEX_RE.sub("<HEX>", normalized)
    normalized = LONG_HEX_RE.sub("<HEX>", normalized)
    normalized = REQUEST_ID_RE.sub(r"\1<REQ_ID>", normalized)
    normalized = TRACE_ID_RE.sub(r"\1<TRACE_ID>", normalized)
    normalized = LONG_ID_RE.sub("<NUM>", normalized)
    normalized = TOKENISH_RE.sub("<TOKEN>", normalized)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip().lower()
    return normalized
