"""Helpers for recovering victim identity from source metadata."""

from __future__ import annotations

import re
from typing import Optional

from src.edu_cti.pipeline.phase2.utils.deduplication import clean_institution_name

_GENERIC_EDU_ENTITY_RE = (
    r"(?:university|college|school|academy|institute|polytechnic|district|"
    r"school district|community college|technical college|research university|research institute)"
)
_GENERIC_IDENTITY_RE = re.compile(
    r"^(?:(?:the\s+website\s+of\s+)?(?:a|an|the)\s+)?"
    r"(?:public\s+|private\s+|state\s+|local\s+|regional\s+)?"
    rf"(?:{_GENERIC_EDU_ENTITY_RE})(?:\s+{_GENERIC_EDU_ENTITY_RE})*"
    r"(?:\s+in\b.*)?$",
    re.IGNORECASE,
)
_VAGUE_PLURAL_RE = re.compile(r"^(?:several|multiple|various|few|many|some)\b", re.IGNORECASE)
_WEBSITE_OF_RE = re.compile(r"\bwebsites?\s+of\b", re.IGNORECASE)
_EDU_KEYWORD_RE = re.compile(
    r"\b(university|college|school|academy|institute|polytechnic|district|campus)\b",
    re.IGNORECASE,
)
_NON_ASCII_RE = re.compile(r"[^\x00-\x7F]")
_UNKNOWN_VALUES = {"", "unknown", "unnamed", "undisclosed", "n/a", "none", "null"}


def _looks_generic_identity(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if _GENERIC_IDENTITY_RE.match(text):
        return True
    if _VAGUE_PLURAL_RE.match(text):
        return True
    if _WEBSITE_OF_RE.search(text):
        return True
    if re.search(r"\b(?:few|several|multiple|various|many|some)\s+(?:colleges?|schools?|universities?|districts?)\b", text, re.IGNORECASE):
        return True
    words = text.split()
    if len(words) >= 10:
        return True
    if text.endswith("?"):
        return True
    if len(words) >= 6 and any(punct in text for punct in (":", ";")):
        return True
    return False


def _strip_location_suffix(value: str) -> str:
    if " - " not in value:
        return value.strip()
    head, tail = value.split(" - ", 1)
    tail = tail.strip()
    if "," in tail or "/" in tail:
        return head.strip()
    return value.strip()


def _prefer_english_alias(value: str) -> str:
    if " / " not in value:
        return value
    left, right = value.split(" / ", 1)
    if _EDU_KEYWORD_RE.search(left) and _NON_ASCII_RE.search(right):
        return left.strip()
    return value


def _normalize_source_identity_candidate(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip().strip("\"'“”")
    if not text:
        return None
    text = _strip_location_suffix(text)
    text = _prefer_english_alias(text)
    cleaned = clean_institution_name(text).strip()
    if not cleaned or cleaned.lower() in _UNKNOWN_VALUES:
        return None
    if len(cleaned) < 4:
        return None
    return cleaned


def recover_source_identity(
    *,
    raw_institution_name: Optional[str] = None,
    raw_victim_name: Optional[str] = None,
    raw_subtitle: Optional[str] = None,
    raw_title: Optional[str] = None,
) -> Optional[str]:
    """Recover the best victim label from source metadata."""

    candidates = []
    for raw, origin in (
        (raw_institution_name, "institution"),
        (raw_victim_name, "victim"),
        (raw_subtitle, "subtitle"),
        (raw_title, "title"),
    ):
        normalized = _normalize_source_identity_candidate(raw)
        if not normalized:
            continue
        # Titles are useful when they collapse cleanly to a short educational
        # victim name, but broad incident headlines should not become labels.
        if origin == "title":
            if not _EDU_KEYWORD_RE.search(normalized):
                continue
            if _looks_generic_identity(normalized):
                continue
            if len(normalized.split()) > 5:
                continue
        score = 0
        if _EDU_KEYWORD_RE.search(normalized):
            score += 40
        if not _looks_generic_identity(normalized):
            score += 30
        if normalized.isascii():
            score += 10
        score += min(len(normalized.split()), 8)
        candidates.append((score, len(normalized), normalized))

    if not candidates:
        return None
    return max(candidates)[2]
