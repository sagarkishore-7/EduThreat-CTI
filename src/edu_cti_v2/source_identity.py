"""Helpers for recovering and matching victim identity from source metadata."""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

from src.edu_cti.core.countries import get_country_code, normalize_country
from src.edu_cti.pipeline.phase2.utils.deduplication import (
    clean_institution_name,
    institution_names_match,
)

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
_BROAD_COLLECTIVE_IDENTITY_RE = re.compile(
    r"^(?:aussie|australian|new\s+york|maryland|georgia|california|texas|florida|"
    r"u\.?s\.?|american|canadian|british|uk|dutch|european|global|worldwide|"
    r"international|state|local|regional)\s+"
    r"(?:public\s+)?(?:schools?|school\s+systems?|school\s+districts?|colleges?|"
    r"universities|unis|campuses)\b",
    re.IGNORECASE,
)
_WEBSITE_OF_RE = re.compile(r"\bwebsites?\s+of\b", re.IGNORECASE)
_EDU_KEYWORD_RE = re.compile(
    r"\b(university|college|school|academy|institute|polytechnic|district|campus)\b",
    re.IGNORECASE,
)
_NON_ASCII_RE = re.compile(r"[^\x00-\x7F]")
_UNKNOWN_VALUES = {"", "unknown", "unnamed", "undisclosed", "n/a", "none", "null"}
_PARENTHETICAL_ALIAS_RE = re.compile(r"\s+\(([A-Za-z0-9&.\- /]{2,24})\)\s*$")
_TRAILING_CAMPUS_RE = re.compile(r"\b(?:[a-z0-9-]+\s+)?campus$", re.IGNORECASE)
_INCIDENT_HEADLINE_RE = re.compile(
    r"\b(?:breach|breached|cyber|cyberattack|hack|hacked|ransomware|attack|"
    r"outage|outages|disrupt|disrupts|disrupted|stolen|leak|leaked|phishing|"
    r"malware|data|impact|impacted|affected|exposed|confirms|confirmed)\b",
    re.IGNORECASE,
)
_IDENTITY_TOKEN_STOP_WORDS = {
    "the",
    "of",
    "at",
    "for",
    "and",
    "de",
    "del",
    "des",
    "der",
    "den",
    "da",
    "do",
    "dos",
    "das",
    "du",
    "di",
    "degli",
    "della",
    "la",
    "le",
    "los",
    "las",
    "el",
    "y",
    "und",
    "et",
    "v",
}
_GENERIC_IDENTITY_TOKENS = {
    "academy",
    "board",
    "centre",
    "center",
    "college",
    "colleges",
    "community",
    "department",
    "district",
    "education",
    "institute",
    "institution",
    "joint",
    "office",
    "public",
    "school",
    "schools",
    "system",
    "systems",
    "township",
    "unified",
    "university",
}
_VENDOR_ANCHOR_RE = re.compile(
    r"\b(?:canvas|classrooms|instructure|powerschool|software|vendor)\b",
    re.IGNORECASE,
)
_IDENTITY_TERM_REPLACEMENTS: tuple[tuple[str, str], ...] = (
    ("university of applied sciences", "hochschule"),
    ("universite", "university"),
    ("universitat", "university"),
    ("universita", "university"),
    ("universitaet", "university"),
    ("universidad", "university"),
    ("universidade", "university"),
    ("universiteit", "university"),
    ("univerza", "university"),
    ("universitario", "university"),
    ("universitaria", "university"),
    ("hochschule", "university"),
)
_IDENTITY_TOKEN_REPLACEMENTS: dict[str, tuple[str, ...]] = {
    # Common source shorthand seen in school and university reporting. Keep this
    # list deliberately small so normal initials are not over-expanded.
    "nc": ("north", "carolina"),
}


def looks_geographic_only_identity(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if _EDU_KEYWORD_RE.search(text):
        return False
    normalized_country = normalize_country(text)
    if not normalized_country:
        return False
    return bool(get_country_code(normalized_country))


def looks_broad_collective_identity(value: Optional[str]) -> bool:
    """Return True for broad regional victim labels, not named institutions."""

    text = str(value or "").strip()
    if not text:
        return False
    return bool(_BROAD_COLLECTIVE_IDENTITY_RE.match(text))


def _looks_generic_identity(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if looks_geographic_only_identity(text):
        return True
    if looks_broad_collective_identity(text):
        return True
    if _GENERIC_IDENTITY_RE.match(text):
        return True
    if _VAGUE_PLURAL_RE.match(text):
        return True
    if _WEBSITE_OF_RE.search(text):
        return True
    if re.search(
        r"\b(?:few|several|multiple|various|many|some)\s+(?:colleges?|schools?|universities?|districts?)\b",
        text,
        re.IGNORECASE,
    ):
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
    if looks_geographic_only_identity(cleaned):
        return None
    if len(cleaned) < 4:
        return None
    return cleaned


def _compact_metadata_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).strip()


def _looks_like_rss_title_copy(raw_value: Optional[str], raw_title: Optional[str]) -> bool:
    """Detect RSS descriptions that are just the headline plus publisher.

    Google/Bing RSS often stores descriptions as "Headline Publisher" while
    titles use "Headline - Publisher". If we normalize only after
    clean_institution_name(), the remaining fragment can look like a victim
    anchor. Catch the raw pattern before institution cleaning.
    """

    value = _compact_metadata_text(raw_value).strip("\"'“”")
    title = _compact_metadata_text(raw_title).strip("\"'“”")
    if not value or not title:
        return False
    if value.lower() == title.lower():
        return True
    title_without_separator = re.sub(r"\s+-\s+", " ", title).strip()
    if value.lower() == title_without_separator.lower():
        return True
    if " - " in title:
        headline, publisher = title.rsplit(" - ", 1)
        combined = f"{headline.strip()} {publisher.strip()}".strip()
        if value.lower() == combined.lower():
            return True
        if value.lower().startswith(headline.strip().lower()) and value.lower().endswith(
            publisher.strip().lower()
        ):
            return True
    return False


def _looks_like_related_or_excerpt_subtitle(raw_value: Optional[str]) -> bool:
    value = _compact_metadata_text(raw_value)
    if not value:
        return False
    lowered = value.lower()
    return value.startswith(("...", "…")) or "related:" in lowered


def _looks_like_descriptive_subtitle(raw_value: Optional[str], normalized: str) -> bool:
    value = _compact_metadata_text(raw_value)
    if not value:
        return False

    # Check both the displayed text and the romanised/mapped match form so
    # legitimate non-English labels such as "Universität ..." are preserved.
    match_form = _normalize_identity_for_match(normalized) or ""
    evidence = f"{value} {normalized} {match_form}"
    if _EDU_KEYWORD_RE.search(evidence) or _VENDOR_ANCHOR_RE.search(evidence):
        return False

    if re.match(r"^(?:through|via|using|after|about|following)\b", value, re.IGNORECASE):
        return True
    return len(value.split()) >= 5 and value.endswith(".")


def _looks_like_location_label(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if _EDU_KEYWORD_RE.search(text) or _VENDOR_ANCHOR_RE.search(text):
        return False
    if "/" in text and not re.search(
        r"\b(?:college|school|university|classrooms|systems?)\b", text, re.IGNORECASE
    ):
        return True
    parts = [part.strip() for part in re.split(r"[,/]", text) if part.strip()]
    normalized_country = normalize_country(parts[-1]) if parts else None
    if len(parts) >= 2 and normalized_country and get_country_code(normalized_country):
        return True
    return False


def _normalized_token_set(value: Optional[str]) -> set[str]:
    normalized = _normalize_identity_for_match(value)
    if not normalized:
        return set()
    return {
        token
        for token in normalized.split()
        if token and token not in _IDENTITY_TOKEN_STOP_WORDS and len(token) > 1
    }


def _looks_like_title_publisher(candidate: str, raw_title: Optional[str]) -> bool:
    title = str(raw_title or "")
    if " - " not in title:
        return False
    suffix = title.rsplit(" - ", 1)[1].strip()
    if not suffix:
        return False
    candidate_norm = _normalize_identity_for_match(candidate)
    suffix_norm = _normalize_identity_for_match(suffix)
    if not candidate_norm or not suffix_norm:
        return False
    if candidate_norm == suffix_norm:
        return True
    return institution_names_match(candidate_norm, suffix_norm, threshold=92)


def _looks_like_repeated_headline(candidate: str, raw_title: Optional[str]) -> bool:
    title_tokens = _normalized_token_set(raw_title)
    candidate_tokens = _normalized_token_set(candidate)
    if not title_tokens or not candidate_tokens:
        return False
    overlap = candidate_tokens & title_tokens
    if len(candidate_tokens) <= 2:
        return overlap == candidate_tokens and _looks_like_title_publisher(candidate, raw_title)
    return len(overlap) / len(candidate_tokens) >= 0.75 and _INCIDENT_HEADLINE_RE.search(candidate)


def _looks_like_incident_headline_fragment(candidate: str) -> bool:
    words = candidate.split()
    if len(words) >= 5 and _INCIDENT_HEADLINE_RE.search(candidate):
        return True
    return False


def _ascii_fold(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value)
    return "".join(char for char in folded if not unicodedata.combining(char))


def _normalize_identity_for_match(value: Optional[str]) -> Optional[str]:
    cleaned = clean_institution_name(value).strip()
    if not cleaned:
        return None
    normalized = _ascii_fold(cleaned).lower()
    normalized = _PARENTHETICAL_ALIAS_RE.sub("", normalized).strip()
    normalized = normalized.replace("/", " ")
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^\w\s-]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = _TRAILING_CAMPUS_RE.sub("", normalized).strip()
    for source, target in _IDENTITY_TERM_REPLACEMENTS:
        normalized = re.sub(rf"\b{re.escape(source)}\b", target, normalized)
    tokens: list[str] = []
    for token in normalized.split():
        replacement = _IDENTITY_TOKEN_REPLACEMENTS.get(token)
        if replacement:
            tokens.extend(replacement)
        else:
            tokens.append(token)
    normalized = " ".join(tokens)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _identity_match_tokens(value: Optional[str]) -> set[str]:
    normalized = _normalize_identity_for_match(value)
    if not normalized:
        return set()
    return {
        token
        for token in normalized.split()
        if token and token not in _IDENTITY_TOKEN_STOP_WORDS and len(token) > 1
    }


def _identity_match_variants(value: Optional[str]) -> set[str]:
    normalized = _normalize_identity_for_match(value)
    if not normalized:
        return set()
    variants = {normalized}
    stripped = _PARENTHETICAL_ALIAS_RE.sub("", normalized).strip()
    if stripped:
        variants.add(stripped)
    if normalized.endswith(" campus"):
        variants.add(normalized[: -len(" campus")].strip())
    campus_trimmed = _TRAILING_CAMPUS_RE.sub("", normalized).strip()
    if campus_trimmed:
        variants.add(campus_trimmed)
    return {variant for variant in variants if variant}


def _identity_acronym(value: Optional[str]) -> Optional[str]:
    normalized = _normalize_identity_for_match(value)
    if not normalized:
        return None
    tokens = [
        token
        for token in normalized.split()
        if token and token not in _IDENTITY_TOKEN_STOP_WORDS and len(token) > 1
    ]
    if len(tokens) < 3:
        return None
    acronym = "".join(token[0] for token in tokens)
    if len(acronym) < 3 or len(acronym) > 10:
        return None
    return acronym


def identity_matches_source_anchor(
    extracted_identity: Optional[str],
    source_identity: Optional[str],
    *,
    extracted_aliases: Optional[list[str]] = None,
    source_aliases: Optional[list[str]] = None,
    threshold: int = 80,
) -> bool:
    """Return True when translated, romanised, or aliased forms refer to the same victim."""

    left_candidates = [extracted_identity, *(extracted_aliases or [])]
    right_candidates = [source_identity, *(source_aliases or [])]

    for left in left_candidates:
        for right in right_candidates:
            if institution_names_match(str(left or ""), str(right or ""), threshold=threshold):
                return True

            left_variants = _identity_match_variants(left)
            right_variants = _identity_match_variants(right)
            if not left_variants or not right_variants:
                continue
            if left_variants & right_variants:
                return True

            for left_variant in left_variants:
                for right_variant in right_variants:
                    if left_variant == right_variant:
                        return True
                    left_tokens = _identity_match_tokens(left_variant)
                    right_tokens = _identity_match_tokens(right_variant)
                    if not left_tokens or not right_tokens:
                        continue
                    if left_tokens == right_tokens:
                        return True
                    smaller, larger = sorted((left_tokens, right_tokens), key=len)
                    if len(smaller) >= 2 and smaller.issubset(larger):
                        return True
                    left_distinct = left_tokens - _GENERIC_IDENTITY_TOKENS
                    right_distinct = right_tokens - _GENERIC_IDENTITY_TOKENS
                    if left_distinct and right_distinct:
                        if left_distinct == right_distinct:
                            return True
                        distinct_smaller, distinct_larger = sorted(
                            (left_distinct, right_distinct),
                            key=len,
                        )
                        if (
                            len(distinct_smaller) >= 2
                            and distinct_smaller.issubset(distinct_larger)
                            and len(distinct_smaller) / len(distinct_larger) >= 0.66
                        ):
                            return True
                        if (
                            len(distinct_smaller) == 1
                            and distinct_smaller.issubset(distinct_larger)
                            and len(next(iter(distinct_smaller))) >= 6
                        ):
                            return True
                    left_acronym = _identity_acronym(left)
                    right_acronym = _identity_acronym(right)
                    if left_acronym and left_acronym in right_tokens:
                        return True
                    if right_acronym and right_acronym in left_tokens:
                        return True
                    if left_acronym and right_acronym and left_acronym == right_acronym:
                        return True
                    if institution_names_match(left_variant, right_variant, threshold=threshold):
                        return True

    return False


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
        if origin == "subtitle" and _looks_like_rss_title_copy(raw, raw_title):
            continue
        normalized = _normalize_source_identity_candidate(raw)
        if not normalized:
            continue
        if _looks_like_location_label(normalized):
            continue
        if looks_broad_collective_identity(normalized):
            continue
        if _looks_like_title_publisher(normalized, raw_title):
            continue
        if _looks_like_incident_headline_fragment(normalized):
            continue
        if origin == "subtitle":
            # Google/Bing RSS descriptions often duplicate the headline with the
            # publisher suffix stripped or transformed. Those strings are useful
            # search evidence, but they are not victim anchors.
            if _looks_like_related_or_excerpt_subtitle(raw):
                continue
            if _looks_like_descriptive_subtitle(raw, normalized):
                continue
            if _looks_like_title_publisher(normalized, raw_title):
                continue
            if _looks_like_repeated_headline(normalized, raw_title):
                continue
            if _looks_like_incident_headline_fragment(normalized):
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
