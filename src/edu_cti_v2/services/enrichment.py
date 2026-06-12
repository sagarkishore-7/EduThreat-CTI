"""v2 source enrichment service backed by the existing Phase 2 extractor."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
import re
from typing import Any, Dict, Literal, Optional, Tuple
from uuid import uuid4

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage import ArticleContent
from src.edu_cti.pipeline.phase2.utils.deduplication import (
    clean_institution_name,
    institution_names_match,
)
from src.edu_cti.pipeline.phase2.utils.post_processing import (
    infer_institution_type,
    is_headline_format,
)
from src.edu_cti_v2.models import PipelineTask, SourceEnrichment, SourceIncident
from src.edu_cti_v2.repositories import (
    ArticleRepository,
    PipelineTaskRepository,
    SourceIncidentRepository,
    SourceEnrichmentRepository,
)
from src.edu_cti_v2.source_identity import (
    _looks_generic_identity,
    identity_matches_source_anchor,
    looks_broad_collective_identity,
    looks_geographic_only_identity,
    recover_source_identity,
)
from src.edu_cti_v2.services.intake import V2IntakeService

_COLLECTIVE_IDENTITY_RE = re.compile(
    # Collective/plural victim labels ("5 universities", "school districts", "districts").
    # The negative lookahead exempts a SPECIFIC named district written type-first, e.g.
    # "School District of Elmbrook" / "District of Columbia" — these are one named victim,
    # not a collective, and must not be flagged as an invalid primary identity.
    r"^(?:\d+\s+)?(?:universities|colleges|schools|school districts?|districts|campuses|providers|students)\b"
    r"(?!\s+of\s+(?-i:[A-Z]))",
    re.IGNORECASE,
)
_GENERIC_EDU_ENTITY_RE = (
    r"(?:university|college|school|academy|institute|polytechnic|district|"
    r"school district|community college|technical college|research university|"
    r"research institute|health center)"
)
_GENERIC_SINGLE_IDENTITY_RE = re.compile(
    r"^(?:(?:the\s+website\s+of\s+)?(?:a|an|the)\s+)?"
    r"(?:public\s+|private\s+|state\s+|local\s+|regional\s+)?"
    rf"(?:{_GENERIC_EDU_ENTITY_RE})(?:\s+{_GENERIC_EDU_ENTITY_RE})*"
    r"(?:\s+in\b.*)?$",
    re.IGNORECASE,
)
_COMMENTARY_IDENTITY_RE = re.compile(
    r"^(?:the\s+cyber\s+threat\s+to|who\s+are|what\s+are|old-school|cyber\s+threat\s+to)\b",
    re.IGNORECASE,
)
_GENERIC_INDUSTRY_RE = re.compile(r"\bindustry\b", re.IGNORECASE)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INVALID_SECONDARY_VICTIM_NAMES = {
    "",
    "unknown",
    "unknown school",
    "unknown institution",
    "unknown university",
    "unnamed",
    "unnamed school",
    "undisclosed",
    "undisclosed institution",
    "n/a",
    "none",
    "redacted",
    "unidentified",
}
_ARTICLE_BOILERPLATE_RE = re.compile(
    r"\b(?:Related(?:\s+Reading)?\s*:|More\s+from[A-Z]|Written\s+By[A-Z]|"
    r"Promoted\s+Content\b|Partner\s+Content\b)",
    re.IGNORECASE,
)
_ARTICLE_TRIM_MIN_CHARS = 500
_ARTICLE_EVIDENCE_WINDOW_CHARS = 3500
_INCIDENT_LANGUAGE_RE = re.compile(
    r"\b(?:cyber(?:security|attack)?|ransomware|breach|hack(?:ed|ers?)?|attack|leak(?:ed)?|stolen|"
    r"compromis(?:e|ed|es|ing)|unauthori[sz]ed|phish(?:ing)?|deface(?:d|ment)?|"
    r"extort(?:ion|ed)?)\b",
    re.IGNORECASE,
)
_ONLINE_COURSE_SCOPE_RE = re.compile(
    r"\b(?:andrew\s+tate|the\s+real\s+world|hustler'?s\s+university)\b",
    re.IGNORECASE,
)
_EDUCATION_SCOPE_RE = re.compile(
    r"\b(?:university|universit(?:y|ies)|universit[a\u00e4]t|universidad|universidade|"
    r"universit\u00e0|universiteit|colleges?|schools?|academy|academies|polytechnic|hochschule|"
    r"institute\s+of\s+technology|technical\s+institute|polytechnic\s+institute|"
    r"school\s+districts?|community\s+college|technical\s+college|"
    # Education context terms \u2014 keep borderline higher-ed / K-12 cases in scope.
    r"higher\s+education|k-?12|campus|faculty|student\s+(?:data|records|information)|"
    r"education\s+(?:sector|department|provider|technology)|edtech|"
    r"learning\s+management|student\s+information\s+system|"
    # Known education-technology vendors \u2014 vendor breaches that impact schools
    # must stay in scope so their named institutions get fanned out as victims.
    r"instructure|canvas\s+lms|powerschool|blackboard|moodle|schoology|ellucian|"
    r"anthology|illuminate\s+education|finalsite|brightspace|d2l)\b",
    re.IGNORECASE,
)
_HEALTHCARE_SCOPE_RE = re.compile(
    r"\b(?:hospital|hospitals|clinic|clinical|medicine|medical|health|nhs|"
    r"klinikum|universit[a\u00e4]tsklinikum|chu)\b",
    re.IGNORECASE,
)
_ACADEMIC_MEDICAL_CENTER_RE = re.compile(
    r"\b(?:"
    r"academic\s+medical\s+cent(?:er|re)|"
    r"medical\s+school|school\s+of\s+medicine|"
    r"health\s+sciences?\s+cent(?:er|re)|"
    r"university\s+(?:hospital|hospitals|clinic|medical\s+cent(?:er|re)|health)|"
    r"(?:hospital|clinic|medical\s+cent(?:er|re))\s+(?:of|at)\s+(?:the\s+)?university|"
    r"universit[a\u00e4]tsklinikum|universit(?:y|ies)\s+health"
    r")\b",
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
    "la",
    "le",
    "los",
    "las",
    "el",
    "y",
    "und",
    "et",
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
    "health",
    "institute",
    "institution",
    "public",
    "school",
    "schools",
    "system",
    "systems",
    "township",
    "unified",
    "university",
}
_MONTH_NAME_BY_NUMBER = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}
_HISTORICAL_DATE_CONTEXT_RE = re.compile(
    r"\b(?:last|previous|prior)\s+year\b|\ba\s+year\s+(?:earlier|before|prior)\b",
    re.IGNORECASE,
)


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _coerce_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, list):
        value = " ".join(str(item).strip() for item in value if str(item).strip())
    text = str(value).strip()
    return text or None


def _compact_text(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _trim_article_boilerplate_tail(content: Optional[str]) -> str:
    """Trim related-link and author-card tails before LLM extraction/evidence checks."""
    text = _compact_text(content)
    if not text:
        return ""
    for match in _ARTICLE_BOILERPLATE_RE.finditer(text):
        if match.start() < _ARTICLE_TRIM_MIN_CHARS:
            continue
        prefix = text[: match.start()].strip()
        if _INCIDENT_LANGUAGE_RE.search(prefix):
            return prefix
    return text


def _identity_search_terms(
    identity: Optional[str], aliases: Optional[list[str]] = None
) -> list[str]:
    terms: list[str] = []
    for value in [identity, *(aliases or [])]:
        cleaned = clean_institution_name(value).strip()
        if cleaned and cleaned.lower() not in {term.lower() for term in terms}:
            terms.append(cleaned)
    cleaned_identity = clean_institution_name(identity).strip()
    tokens = [
        token
        for token in re.findall(r"[A-Za-z0-9]+", cleaned_identity)
        if token.lower() not in _IDENTITY_TOKEN_STOP_WORDS and len(token) > 1
    ]
    if len(tokens) >= 3:
        acronym = "".join(token[0] for token in tokens).upper()
        if 3 <= len(acronym) <= 10 and acronym.lower() not in {term.lower() for term in terms}:
            terms.append(acronym)
    return terms


def _identity_term_position(text: str, terms: list[str]) -> int:
    haystack = _compact_text(text).lower()
    positions: list[int] = []
    for term in terms:
        needle = _compact_text(term).lower()
        if not needle or (len(needle) < 3 and not term.isupper()):
            continue
        position = haystack.find(needle)
        if position >= 0:
            positions.append(position)
    return min(positions) if positions else -1


def _distinct_identity_tokens(value: Optional[str]) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", _normalized_identity_text(value))
        if token not in _IDENTITY_TOKEN_STOP_WORDS
        and token not in _GENERIC_IDENTITY_TOKENS
        and len(token) > 1
    }


def _identity_token_supports_article(identity: Optional[str], text: str) -> bool:
    tokens = _distinct_identity_tokens(identity)
    if not tokens:
        return False
    # For names like "Texas University", a single geographic token plus a
    # generic education suffix is too weak: any article mentioning Texas would
    # otherwise support a hallucinated institution. Exact phrase/alias matching
    # is still handled before this token-overlap fallback.
    if len(tokens) == 1 and _EDUCATION_SCOPE_RE.search(str(identity or "")):
        return False
    article_tokens = set(re.findall(r"[a-z0-9]+", _compact_text(text).lower()))
    overlap = tokens & article_tokens
    if len(tokens) <= 2:
        return len(overlap) == len(tokens)
    return len(overlap) >= 2 and len(overlap) / len(tokens) >= 0.66


def _parse_iso_day(value: Any) -> Optional[date]:
    text = str(value or "").strip()
    if not _ISO_DATE_RE.match(text):
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


_THIRD_PARTY_ATTACK_SIGNALS = {
    "third_party_compromise",
    "supply_chain_compromise",
    "third_party_vendor",
    "software_update_compromise",
    "trusted_relationship",
}


def _apply_extraction_quality_fixes(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Deterministic post-LLM repairs applied to both the raw JSON and the typed
    projection so they reach the canonical record:

    1. institution_type — infer from the institution name when the LLM left it
       null/unknown (e.g. "University of Oxford" -> "university").
    2. third_party_vendor_impact — set true when the attack is classified as a
       third-party/supply-chain compromise, even if the LLM forgot the boolean.
    """
    if not isinstance(payload, dict):
        return payload

    # 1. institution_type inference.
    name = payload.get("institution_name") or payload.get("institution_name_en")
    inst_type = payload.get("institution_type")
    if name and inst_type in (None, "", "unknown"):
        inferred = infer_institution_type(str(name), inst_type if isinstance(inst_type, str) else None)
        if inferred and inferred not in (None, "unknown"):
            payload["institution_type"] = inferred

    # 2. third_party_vendor_impact reconciliation.
    attack_vals = []
    for key in ("attack_category", "attack_vector"):
        v = payload.get(key)
        if isinstance(v, str):
            attack_vals.append(v.lower())
        elif isinstance(v, list):
            attack_vals.extend(str(x).lower() for x in v)
    is_third_party = any(sig in attack_vals for sig in _THIRD_PARTY_ATTACK_SIGNALS)
    if is_third_party:
        si = payload.get("system_impact")
        if isinstance(si, dict) and not si.get("third_party_vendor_impact"):
            si["third_party_vendor_impact"] = True
    return payload


def _source_or_document_publish_date(
    source_incident: SourceIncident,
    document: ArticleDocument,
    raw_json_data: Dict[str, Any],
) -> Optional[date]:
    for candidate in (
        raw_json_data.get("source_published_date"),
        raw_json_data.get("publication_date"),
        document.publish_date,
        source_incident.source_published_at.date()
        if source_incident.source_published_at
        else None,
    ):
        if isinstance(candidate, date) and not isinstance(candidate, datetime):
            return candidate
        parsed = _parse_iso_day(candidate)
        if parsed:
            return parsed
    return None


def _date_phrase_contexts(article_content: Optional[str], parsed: date) -> list[str]:
    text = _compact_text(article_content)
    if not text:
        return []
    month = _MONTH_NAME_BY_NUMBER[parsed.month]
    pattern = re.compile(
        rf"\b{re.escape(month)}\.?\s+0?{parsed.day}\b",
        re.IGNORECASE,
    )
    contexts: list[str] = []
    for match in pattern.finditer(text):
        start = max(0, match.start() - 80)
        end = min(len(text), match.end() + 80)
        contexts.append(text[start:end])
    return contexts


def _repair_yearless_date_if_needed(
    value: Any,
    *,
    publish_date: Optional[date],
    article_content: Optional[str],
) -> Any:
    parsed = _parse_iso_day(value)
    if parsed is None or publish_date is None:
        return value
    if publish_date.year - parsed.year != 1:
        return value
    contexts = _date_phrase_contexts(article_content, parsed)
    if not contexts:
        return value
    extracted_year = str(parsed.year)
    for context in contexts:
        if extracted_year in context or _HISTORICAL_DATE_CONTEXT_RE.search(context):
            return value
    return f"{publish_date.year:04d}-{parsed.month:02d}-{parsed.day:02d}"


def _repair_yearless_dates_in_payload(
    payload: Optional[Dict[str, Any]],
    *,
    publish_date: Optional[date],
    article_content: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return payload
    updated = dict(payload)
    changed = False
    for key in ("incident_date", "discovery_date", "public_disclosure_date", "notification_date"):
        if key not in updated:
            continue
        repaired = _repair_yearless_date_if_needed(
            updated.get(key),
            publish_date=publish_date,
            article_content=article_content,
        )
        if repaired != updated.get(key):
            updated[key] = repaired
            changed = True
    timeline = updated.get("timeline")
    if isinstance(timeline, list):
        repaired_timeline: list[Any] = []
        for item in timeline:
            if isinstance(item, dict) and item.get("date"):
                repaired_item = dict(item)
                repaired = _repair_yearless_date_if_needed(
                    repaired_item.get("date"),
                    publish_date=publish_date,
                    article_content=article_content,
                )
                if repaired != repaired_item.get("date"):
                    repaired_item["date"] = repaired
                    changed = True
                repaired_timeline.append(repaired_item)
            else:
                repaired_timeline.append(item)
        if changed:
            updated["timeline"] = repaired_timeline
    if changed:
        notes = str(updated.get("extraction_notes") or "").strip()
        repair_note = (
            "Adjusted yearless article dates to the source publication year "
            "when the article named only month/day and did not mention the extracted prior year."
        )
        updated["extraction_notes"] = f"{notes} {repair_note}".strip() if notes else repair_note
    return updated


def _article_main_text_supports_identity(
    *,
    extracted_identity: Optional[str],
    extracted_aliases: Optional[list[str]],
    article_title: Optional[str],
    article_content: Optional[str],
) -> bool:
    terms = _identity_search_terms(extracted_identity, extracted_aliases)
    main_text = _trim_article_boilerplate_tail(article_content)
    title_text = _compact_text(article_title)
    evidence_text = f"{title_text} {main_text[:_ARTICLE_EVIDENCE_WINDOW_CHARS]}"
    if not _INCIDENT_LANGUAGE_RE.search(evidence_text):
        return False
    if _identity_term_position(title_text, terms) >= 0:
        return True
    if _identity_term_position(main_text[:_ARTICLE_EVIDENCE_WINDOW_CHARS], terms) >= 0:
        return True
    return _identity_token_supports_article(extracted_identity, evidence_text)


def _article_text_names_identity(
    *,
    extracted_identity: Optional[str],
    extracted_aliases: Optional[list[str]],
    article_title: Optional[str],
    article_content: Optional[str],
) -> bool:
    """Return True when source title/body names the extracted identity.

    This is intentionally weaker than `_article_main_text_supports_identity`:
    it does not require incident-language cues, so translated and short local
    stories can still pass when the victim name or alias is present.
    """
    terms = _identity_search_terms(extracted_identity, extracted_aliases)
    main_text = _trim_article_boilerplate_tail(article_content)
    title_text = _compact_text(article_title)
    evidence_text = f"{title_text} {main_text[:_ARTICLE_EVIDENCE_WINDOW_CHARS]}"
    if _identity_term_position(title_text, terms) >= 0:
        return True
    if _identity_term_position(main_text[:_ARTICLE_EVIDENCE_WINDOW_CHARS], terms) >= 0:
        return True
    return _identity_token_supports_article(extracted_identity, evidence_text)


def _identity_appears_only_in_boilerplate(
    *,
    extracted_identity: Optional[str],
    extracted_aliases: Optional[list[str]],
    article_title: Optional[str],
    article_content: Optional[str],
) -> bool:
    terms = _identity_search_terms(extracted_identity, extracted_aliases)
    full_text = _compact_text(article_content)
    if not full_text:
        return False
    main_text = _trim_article_boilerplate_tail(full_text)
    if main_text == full_text:
        return False
    if _identity_term_position(_compact_text(article_title), terms) >= 0:
        return False
    return (
        _identity_term_position(full_text, terms) >= 0
        and _identity_term_position(main_text, terms) < 0
    )


def _source_has_strong_structured_identity(source_incident) -> bool:
    for candidate in (source_incident.raw_institution_name, source_incident.raw_victim_name):
        cleaned = _clean_identity(candidate)
        if not cleaned:
            continue
        if not _looks_invalid_primary_identity(
            candidate,
            title=source_incident.raw_title,
            cleaned_value=cleaned,
        ):
            return True
    return False


def _has_other_edu_incidents(raw_json_data: Dict[str, Any]) -> bool:
    incidents = raw_json_data.get("other_edu_incidents")
    return isinstance(incidents, list) and any(isinstance(item, dict) for item in incidents)


def _source_requires_article_identity_support(source_incident) -> bool:
    source_name = str(getattr(source_incident, "source_name", "") or "").lower()
    source_group = str(getattr(source_incident, "source_group", "") or "").lower()
    if source_name in {"googlenews_rss", "bing_news_rss", "yahoo_news_rss"}:
        return True
    return source_group in {"rss", "news"}


def _structured_curated_source_should_review_non_edu_article(source_incident) -> bool:
    source_group = str(getattr(source_incident, "source_group", "") or "").lower()
    if source_group != "curated":
        return False
    if not _source_has_strong_structured_identity(source_incident):
        return False
    evidence = " ".join(
        _compact_text(value)
        for value in (
            getattr(source_incident, "raw_institution_name", None),
            getattr(source_incident, "raw_victim_name", None),
            getattr(source_incident, "raw_title", None),
        )
        if value
    )
    title_text = _compact_text(getattr(source_incident, "raw_title", None))
    if _HEALTHCARE_SCOPE_RE.search(evidence):
        title_has_healthcare_scope = bool(_HEALTHCARE_SCOPE_RE.search(title_text))
        title_has_education_scope = bool(_EDUCATION_SCOPE_RE.search(title_text))
        if title_has_healthcare_scope or not title_has_education_scope:
            return False
    has_education_scope = bool(_EDUCATION_SCOPE_RE.search(evidence))
    has_attack_context = bool(
        _coerce_attack_hint(getattr(source_incident, "raw_attack_hint", None))
        or _INCIDENT_LANGUAGE_RE.search(title_text)
    )
    return has_education_scope and has_attack_context


def _academic_medical_context_text(
    source_incident,
    raw_json_data: Dict[str, Any],
    typed_enrichment: Optional[Dict[str, Any]],
    *,
    article_title: Optional[str],
    article_content: Optional[str],
) -> str:
    typed = typed_enrichment if isinstance(typed_enrichment, dict) else {}
    values = [
        getattr(source_incident, "raw_institution_name", None),
        getattr(source_incident, "raw_victim_name", None),
        getattr(source_incident, "raw_institution_type", None),
        getattr(source_incident, "raw_title", None),
        getattr(source_incident, "raw_subtitle", None),
        raw_json_data.get("institution_name"),
        raw_json_data.get("institution_name_en"),
        raw_json_data.get("institution_type"),
        raw_json_data.get("education_relevance_reasoning"),
        typed.get("institution_name"),
        typed.get("institution_type"),
        article_title,
        (article_content or "")[:2500],
    ]
    return " ".join(_compact_text(value) for value in values if value)


def _is_university_branded_healthcare_context(
    source_incident,
    raw_json_data: Dict[str, Any],
    typed_enrichment: Optional[Dict[str, Any]],
    *,
    article_title: Optional[str],
    article_content: Optional[str],
) -> bool:
    text = _academic_medical_context_text(
        source_incident,
        raw_json_data,
        typed_enrichment,
        article_title=article_title,
        article_content=article_content,
    )
    if not text:
        return False
    if _ACADEMIC_MEDICAL_CENTER_RE.search(text):
        return True
    return bool(_HEALTHCARE_SCOPE_RE.search(text) and _EDUCATION_SCOPE_RE.search(text))


def _mark_academic_medical_center_in_scope(
    source_incident,
    raw_json_data: Dict[str, Any],
    typed_enrichment: Optional[Dict[str, Any]],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Promote university-branded hospitals to education-adjacent incidents."""
    updated_raw = dict(raw_json_data)
    updated_raw["is_edu_cyber_incident"] = True
    updated_raw["_education_adjacent_scope"] = "academic_medical_center"
    reason = (
        "University-branded hospital, medical center, or health sciences center "
        "is treated as education-adjacent scope."
    )
    existing_reasoning = str(updated_raw.get("education_relevance_reasoning") or "").strip()
    if reason not in existing_reasoning:
        updated_raw["education_relevance_reasoning"] = (
            f"{existing_reasoning} {reason}".strip() if existing_reasoning else reason
        )

    updated_typed = dict(typed_enrichment or {})
    source_identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    )
    institution_name = _coerce_optional_text(
        updated_typed.get("institution_name")
        or updated_raw.get("institution_name")
        or updated_raw.get("institution_name_en")
        or source_identity
    )
    if institution_name:
        updated_raw["institution_name"] = updated_raw.get("institution_name") or institution_name
        updated_typed["institution_name"] = institution_name
    updated_raw["institution_type"] = updated_raw.get("institution_type") or "university_hospital"
    updated_typed["institution_type"] = updated_typed.get("institution_type") or "university_hospital"
    updated_typed["attack_category"] = (
        updated_typed.get("attack_category")
        or updated_raw.get("attack_category")
        or _coerce_attack_hint(getattr(source_incident, "raw_attack_hint", None))
        or "cyber_incident"
    )
    updated_typed["incident_date"] = (
        updated_typed.get("incident_date")
        or updated_raw.get("incident_date")
        or getattr(source_incident, "raw_incident_date", None)
    )
    updated_typed["incident_date_precision"] = (
        updated_typed.get("incident_date_precision")
        or updated_raw.get("incident_date_precision")
        or getattr(source_incident, "raw_date_precision", None)
        or _infer_date_precision(updated_typed.get("incident_date"))
    )
    updated_typed["enriched_summary"] = (
        updated_typed.get("enriched_summary")
        or updated_raw.get("enriched_summary")
        or getattr(source_incident, "raw_title", None)
    )
    updated_typed["education_relevance_reasoning"] = updated_raw["education_relevance_reasoning"]
    updated_typed["_education_adjacent_scope"] = "academic_medical_center"
    return updated_raw, updated_typed


def _mark_structured_curated_in_scope(
    source_incident,
    raw_json_data: Dict[str, Any],
    typed_enrichment: Optional[Dict[str, Any]],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Keep a curated incident on its authoritative structured record (Path A).

    A curated breach DB (comparitech / konbriefing) that strongly names an
    education incident is authoritative about *that the incident happened* even
    when the news article discovered to support it is weak or off-topic. Rather
    than parking it in manual review (losing real coverage), trust the structured
    victim, mark it in-scope, and flag that article corroboration is weak. The
    structured victim / date come from the curated record; suspect article-derived
    fields are left for the downstream identity repair to reconcile.
    """
    updated_raw = dict(raw_json_data)
    updated_raw["is_edu_cyber_incident"] = True
    updated_raw["_structured_curated_trust"] = True
    updated_raw["_weak_article_support"] = True
    reason = (
        "Curated breach source authoritatively names this education incident; kept "
        "on the structured record despite weak supporting-article corroboration."
    )
    existing_reasoning = str(updated_raw.get("education_relevance_reasoning") or "").strip()
    if reason not in existing_reasoning:
        updated_raw["education_relevance_reasoning"] = (
            f"{existing_reasoning} {reason}".strip() if existing_reasoning else reason
        )

    updated_typed = dict(typed_enrichment or {})
    structured_identity = _structured_source_authoritative_identity(source_incident)
    institution_name = _coerce_optional_text(
        structured_identity
        or updated_typed.get("institution_name")
        or updated_raw.get("institution_name")
    )
    if institution_name:
        # The curated structured victim is authoritative — prefer it over any
        # name parsed from the unrelated article.
        updated_raw["institution_name"] = institution_name
        updated_typed["institution_name"] = institution_name
    updated_typed["incident_date"] = (
        updated_typed.get("incident_date")
        or updated_raw.get("incident_date")
        or getattr(source_incident, "raw_incident_date", None)
    )
    updated_typed["incident_date_precision"] = (
        updated_typed.get("incident_date_precision")
        or updated_raw.get("incident_date_precision")
        or getattr(source_incident, "raw_date_precision", None)
        or _infer_date_precision(updated_typed.get("incident_date"))
    )
    updated_typed["education_relevance_reasoning"] = updated_raw["education_relevance_reasoning"]
    updated_typed["_weak_article_support"] = True
    return updated_raw, updated_typed


def _coerce_attack_hint(value: Any) -> Optional[str]:
    if isinstance(value, list):
        value = next((item for item in value if str(item).strip()), None)
    return _coerce_optional_text(value)


def _infer_date_precision(value: Optional[str]) -> str:
    return "day" if value and _ISO_DATE_RE.match(value) else "unknown"


def _build_roundup_secondary_event_key(
    source_incident,
    victim_name: str,
    incident_date: Optional[str],
) -> str:
    parent_key = (source_incident.source_event_key or "").strip() or str(source_incident.id)
    parent_fingerprint = hashlib.sha256(parent_key.encode("utf-8")).hexdigest()[:16]
    normalized_victim = clean_institution_name(victim_name).strip().lower()
    return f"roundup_extract|{parent_fingerprint}|{normalized_victim}|{incident_date or ''}"


def _build_roundup_secondary_notes(
    *,
    source_url: Optional[str],
    brief_description: Optional[str],
) -> Optional[str]:
    parts: list[str] = []
    if source_url:
        parts.append(f"Extracted from roundup: {source_url}")
    if brief_description:
        parts.append(brief_description)
    return "\n".join(parts) or None


def source_incident_to_base_incident(
    source_incident,
    article_url: str,
    *,
    re_enrich_attempts: int | None = None,
    re_enrich_reason: str | None = None,
) -> BaseIncident:
    """Adapt a v2 source incident into the current Phase 2 BaseIncident shape."""
    source_identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    )
    all_urls = [
        row.url
        for row in (source_incident.urls or [])
        if row.url_kind == "article" and not row.is_wrapper
    ]
    if article_url and article_url not in all_urls:
        all_urls.insert(0, article_url)

    return BaseIncident(
        incident_id=str(source_incident.id),
        source=source_incident.source_name,
        source_event_id=source_incident.source_event_key,
        institution_name=source_identity or "",
        victim_raw_name=source_incident.raw_victim_name,
        institution_type=source_incident.raw_institution_type,
        country=source_incident.raw_country,
        region=source_incident.raw_region,
        city=source_incident.raw_city,
        incident_date=source_incident.raw_incident_date,
        date_precision=source_incident.raw_date_precision or "unknown",
        source_published_date=(
            source_incident.source_published_at.date().isoformat()
            if source_incident.source_published_at
            else None
        ),
        ingested_at=source_incident.collected_at.isoformat(),
        title=source_incident.raw_title,
        subtitle=source_incident.raw_subtitle,
        primary_url=None,
        all_urls=all_urls,
        leak_site_url=next(
            (row.url for row in (source_incident.urls or []) if row.url_kind == "leak_site"), None
        ),
        source_detail_url=next(
            (row.url for row in (source_incident.urls or []) if row.url_kind == "detail"), None
        ),
        screenshot_url=next(
            (row.url for row in (source_incident.urls or []) if row.url_kind == "screenshot"), None
        ),
        attack_type_hint=source_incident.raw_attack_hint,
        status=source_incident.raw_status or "suspected",
        source_confidence=source_incident.source_confidence or "medium",
        notes=source_incident.raw_notes,
        threat_actor=source_incident.raw_threat_actor,
        re_enrich_attempts=re_enrich_attempts,
        re_enrich_reason=re_enrich_reason,
    )


def _strip_storage_debug(raw_json_data: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in raw_json_data.items() if key != "_storage_debug"}


def _clean_identity(value: Optional[str]) -> Optional[str]:
    cleaned = clean_institution_name(value).strip()
    return cleaned or None


def _normalized_identity_text(value: Optional[str]) -> str:
    cleaned = _clean_identity(value)
    if not cleaned:
        return ""
    return re.sub(r"\s+", " ", cleaned).strip().lower()


def _source_metadata_supports_extracted_identity(
    source_incident, cleaned_extracted: Optional[str]
) -> bool:
    """Return True when source metadata itself clearly names the extracted victim.

    Some news collectors have noisy subtitle/anchor text from related links. If
    the article title or source-provided victim fields explicitly contain the
    extracted victim, we should not send the row to manual review just because a
    weaker recovered anchor drifted.
    """
    extracted_norm = _normalized_identity_text(cleaned_extracted)
    if not extracted_norm:
        return False

    for candidate in (
        source_incident.raw_institution_name,
        source_incident.raw_victim_name,
        source_incident.raw_title,
    ):
        candidate_norm = _normalized_identity_text(candidate)
        if not candidate_norm:
            continue
        if extracted_norm == candidate_norm or extracted_norm in candidate_norm:
            return True
        if identity_matches_source_anchor(cleaned_extracted, candidate, threshold=75):
            return True
    subtitle = _compact_text(source_incident.raw_subtitle)
    if subtitle and not _ARTICLE_BOILERPLATE_RE.search(subtitle):
        subtitle_norm = _normalized_identity_text(subtitle)
        if extracted_norm == subtitle_norm or extracted_norm in subtitle_norm:
            return True
        if identity_matches_source_anchor(cleaned_extracted, subtitle, threshold=75):
            return True
    return False


def _looks_invalid_primary_identity(
    value: Optional[str],
    *,
    title: Optional[str],
    cleaned_value: Optional[str] = None,
) -> bool:
    raw_text = str(value or "").strip()
    text = str(cleaned_value or value or "").strip()
    if not text:
        return True
    if raw_text and is_headline_format(raw_text, title):
        return True
    if is_headline_format(text, title):
        return True
    if _COLLECTIVE_IDENTITY_RE.match(text):
        return True
    if _GENERIC_SINGLE_IDENTITY_RE.match(text):
        return True
    if looks_geographic_only_identity(text):
        return True
    if _COMMENTARY_IDENTITY_RE.match(text):
        return True
    lowered = text.lower()
    if any(
        marker in lowered
        for marker in ("websites of", "website of", "multiple universities", "many universities")
    ):
        return True
    if (
        _GENERIC_INDUSTRY_RE.search(text)
        and "university" not in lowered
        and "college" not in lowered
        and "school" not in lowered
    ):
        return True
    words = text.split()
    if len(words) >= 10:
        return True
    if len(words) >= 6 and any(punct in text for punct in (":", ";")):
        return True
    return False


def _mark_non_specific_victim(
    raw_json_data: Dict[str, Any],
    *,
    reason: str,
) -> Dict[str, Any]:
    updated = dict(raw_json_data)
    updated["is_edu_cyber_incident"] = False
    updated["_not_education_related"] = True
    updated["_reason"] = reason
    existing_reasoning = str(updated.get("education_relevance_reasoning") or "").strip()
    if reason not in existing_reasoning:
        updated["education_relevance_reasoning"] = (
            f"{existing_reasoning} {reason}".strip() if existing_reasoning else reason
        )
    return updated


def _mark_victim_review_required(
    raw_json_data: Dict[str, Any],
    *,
    reason: str,
) -> Dict[str, Any]:
    updated = dict(raw_json_data)
    updated["_manual_review_required"] = True
    updated["_reason"] = reason
    existing_reasoning = str(updated.get("education_relevance_reasoning") or "").strip()
    if reason not in existing_reasoning:
        updated["education_relevance_reasoning"] = (
            f"{existing_reasoning} {reason}".strip() if existing_reasoning else reason
        )
    return updated


# Source groups that carry an authoritative, independently-known victim name in a
# STRUCTURED field (not parsed from an article): curated breach databases
# (comparitech / konbriefing) and leak-site APIs (ransomware.live / ransomwatch,
# whose victim is named by the ransomware operator's own claim post). For these the
# fetched article is only supporting evidence, so the incident must not be dropped
# for "no specific victim in the article". news/rss have no such structured victim —
# their identity must come from (and be supported by) the article.
_STRUCTURED_VICTIM_SOURCE_GROUPS = ("curated", "api")


def _structured_source_authoritative_identity(source_incident) -> Optional[str]:
    """Trust the structured victim name of a curated/api source as authoritative.

    comparitech / konbriefing (curated) and ransomware.live / ransomwatch (api) record
    the victim in a structured ``raw_institution_name`` known independently of any
    article — for leak-site APIs the ransomware operator itself names the victim. So
    such an incident must NOT be dropped for "no specific victim in the article" just
    because the soft identity heuristics or a weak supporting article are imperfect. We
    still refuse a structured name that is itself clearly collective / geographic /
    generic / a headline (those are genuinely non-specific).
    """
    if getattr(source_incident, "source_group", None) not in _STRUCTURED_VICTIM_SOURCE_GROUPS:
        return None
    name = _clean_identity(source_incident.raw_institution_name) or _clean_identity(
        source_incident.raw_victim_name
    )
    if not name:
        return None
    # NB: no is_headline_format() guard here — a curated/api structured victim is a
    # data field, not a parsed headline, and is_headline_format() returns True whenever
    # the name equals the title (the normal case for ransomware.live, where title ==
    # victim_name). The collective / geographic / generic guards below are what actually
    # distinguish a non-specific structured name from a real one.
    if (
        _COLLECTIVE_IDENTITY_RE.match(name)
        or looks_geographic_only_identity(name)
        or looks_broad_collective_identity(name)
        or _looks_generic_identity(name)
    ):
        return None
    return name


def _repair_or_reject_primary_identity(
    source_incident,
    *,
    raw_json_data: Dict[str, Any],
    typed_enrichment: Optional[Dict[str, Any]],
    article_title: Optional[str] = None,
    article_content: Optional[str] = None,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Literal["ok", "reject", "review"]]:
    # For a curated/api source, prefer its clean structured victim name (authoritative)
    # over the recovered identity — both so the incident is never dropped for a weak
    # article / name-format quirk, AND so the canonical carries the clean structured name
    # rather than a title-derived form (e.g. avoids a "(2022)" year suffix from the headline).
    source_identity = _structured_source_authoritative_identity(source_incident)
    if not source_identity:
        source_identity = recover_source_identity(
            raw_institution_name=source_incident.raw_institution_name,
            raw_victim_name=source_incident.raw_victim_name,
            raw_subtitle=source_incident.raw_subtitle,
            raw_title=source_incident.raw_title,
        )
        if _looks_invalid_primary_identity(source_identity, title=source_incident.raw_title):
            source_identity = None
    extracted_identity = (
        raw_json_data.get("institution_name")
        or raw_json_data.get("institution_name_en")
        or raw_json_data.get("vendor_name")
        or raw_json_data.get("vendor_name_en")
        or (typed_enrichment or {}).get("institution_name")
        or (typed_enrichment or {}).get("vendor_name")
    )
    cleaned_extracted = _clean_identity(extracted_identity)
    title = source_incident.raw_title
    evidence_title = " ".join(
        _compact_text(value) for value in (source_incident.raw_title, article_title) if value
    )
    extracted_aliases = _coerce_string_list(raw_json_data.get("institution_aliases"))
    has_other_edu_incidents = _has_other_edu_incidents(raw_json_data)

    if cleaned_extracted and _ONLINE_COURSE_SCOPE_RE.search(
        " ".join(
            _compact_text(value)
            for value in (
                cleaned_extracted,
                source_incident.raw_title,
                article_title,
            )
            if value
        )
    ):
        reason = "Online course platform is outside the education-sector institution scope."
        return _mark_non_specific_victim(raw_json_data, reason=reason), None, "reject"

    if cleaned_extracted and _identity_appears_only_in_boilerplate(
        extracted_identity=cleaned_extracted,
        extracted_aliases=extracted_aliases,
        article_title=evidence_title,
        article_content=article_content,
    ):
        if has_other_edu_incidents:
            reason = (
                "Article names multiple education victims, but the selected primary victim "
                "appears outside the main article evidence window."
            )
            return _mark_victim_review_required(raw_json_data, reason=reason), None, "review"
        reason = "Extracted victim appears only in related-story or boilerplate text, not the main article."
        return _mark_non_specific_victim(raw_json_data, reason=reason), None, "reject"

    if _looks_invalid_primary_identity(
        extracted_identity, title=title, cleaned_value=cleaned_extracted
    ):
        if source_identity:
            raw_json_data = dict(raw_json_data)
            raw_json_data["institution_name"] = source_identity
            raw_json_data["institution_name_basis"] = "source_anchor_fallback"
            if typed_enrichment is not None:
                typed_enrichment = dict(typed_enrichment)
                typed_enrichment["institution_name"] = source_identity
            return raw_json_data, typed_enrichment, "ok"
        reason = "Article does not identify a specific victim institution or vendor."
        return _mark_non_specific_victim(raw_json_data, reason=reason), None, "reject"

    source_aliases = [
        candidate
        for candidate in (
            source_incident.raw_institution_name,
            source_incident.raw_victim_name,
            source_incident.raw_subtitle,
        )
        if candidate
    ]
    if (
        not source_identity
        and cleaned_extracted
        and _source_requires_article_identity_support(source_incident)
        and not _article_text_names_identity(
            extracted_identity=cleaned_extracted,
            extracted_aliases=extracted_aliases,
            article_title=evidence_title,
            article_content=article_content,
        )
    ):
        reason = "Extracted victim is not supported by the source title or main article text."
        return _mark_victim_review_required(raw_json_data, reason=reason), None, "review"

    if (
        source_identity
        and cleaned_extracted
        and not identity_matches_source_anchor(
            cleaned_extracted,
            source_identity,
            extracted_aliases=extracted_aliases,
            source_aliases=source_aliases,
            threshold=80,
        )
    ):
        if _source_metadata_supports_extracted_identity(source_incident, cleaned_extracted):
            return raw_json_data, typed_enrichment, "ok"
        if _source_has_strong_structured_identity(source_incident):
            reason = (
                f"Extracted victim '{cleaned_extracted}' drifted from structured source target "
                f"'{source_identity}'."
            )
            return _mark_victim_review_required(raw_json_data, reason=reason), None, "review"
        if _article_main_text_supports_identity(
            extracted_identity=cleaned_extracted,
            extracted_aliases=extracted_aliases,
            article_title=evidence_title,
            article_content=article_content,
        ):
            return raw_json_data, typed_enrichment, "ok"
        reason = (
            f"Extracted victim '{cleaned_extracted}' drifted from source anchor "
            f"'{source_identity}'."
        )
        return _mark_victim_review_required(raw_json_data, reason=reason), None, "review"

    return raw_json_data, typed_enrichment, "ok"


class V2EnrichmentService:
    """Persist source-level enrichments using the existing LLM extraction stack."""

    def __init__(
        self,
        *,
        article_repository: Optional[ArticleRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        intake_service: Optional[V2IntakeService] = None,
        enricher: Optional[IncidentEnricher] = None,
        llm_client: Optional[OllamaLLMClient] = None,
    ) -> None:
        self.article_repository = article_repository or ArticleRepository()
        self.source_enrichment_repository = (
            source_enrichment_repository or SourceEnrichmentRepository()
        )
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.intake_service = intake_service or V2IntakeService(
            pipeline_task_repository=self.pipeline_task_repository,
        )
        if enricher is not None:
            self.enricher = enricher
        else:
            llm_client = llm_client or OllamaLLMClient()
            self.enricher = IncidentEnricher(llm_client=llm_client)

    def _create_secondary_source_incidents(
        self,
        session,
        *,
        source_incident,
        article_url: Optional[str],
        raw_json_data: Dict[str, Any],
    ) -> int:
        secondary_entries = raw_json_data.get("other_edu_incidents")
        if not isinstance(secondary_entries, list) or not secondary_entries:
            return 0

        # Vendor / supply-chain context from the parent extraction. When the
        # primary subject is a breached vendor (Instructure, PowerSchool, MOVEit),
        # the named institutions become per-victim incidents tagged
        # affected-via-vendor — and the vendor's AGGREGATE record count is NOT
        # propagated onto them (the stubs carry no records by construction).
        parent_vendor = _coerce_optional_text(
            raw_json_data.get("vendor_name") or raw_json_data.get("vendor_name_detail")
        )
        parent_attack_category = str(raw_json_data.get("attack_category") or "").lower()
        parent_is_vendor_breach = bool(parent_vendor) or any(
            token in parent_attack_category
            for token in ("supply_chain", "third_party", "vendor")
        )

        primary_identity = recover_source_identity(
            raw_institution_name=source_incident.raw_institution_name,
            raw_victim_name=source_incident.raw_victim_name,
            raw_subtitle=source_incident.raw_subtitle,
            raw_title=source_incident.raw_title,
        )
        created = 0
        for entry in secondary_entries:
            if not isinstance(entry, dict):
                continue

            victim_name = _coerce_optional_text(entry.get("victim_name"))
            cleaned_victim = clean_institution_name(victim_name).strip() if victim_name else ""
            if not cleaned_victim or cleaned_victim.lower() in _INVALID_SECONDARY_VICTIM_NAMES:
                continue
            if _looks_invalid_primary_identity(cleaned_victim, title=source_incident.raw_title):
                continue
            if primary_identity and identity_matches_source_anchor(
                cleaned_victim,
                primary_identity,
                source_aliases=[
                    candidate
                    for candidate in (
                        source_incident.raw_institution_name,
                        source_incident.raw_victim_name,
                        source_incident.raw_subtitle,
                    )
                    if candidate
                ],
                threshold=80,
            ):
                continue

            incident_date = _coerce_optional_text(entry.get("incident_date"))
            country = _coerce_optional_text(entry.get("country"))
            attack_hint = _coerce_attack_hint(entry.get("attack_type"))
            brief_description = _coerce_optional_text(entry.get("brief_description"))

            # Treat this fanned-out institution as affected-via-vendor when the
            # entry says so, or when the parent article is a vendor breach.
            entry_via_vendor = bool(entry.get("is_via_vendor")) or parent_is_vendor_breach
            stub_vendor = parent_vendor if entry_via_vendor else None
            if entry_via_vendor and not attack_hint:
                attack_hint = "supply_chain_compromise"

            event_key = _build_roundup_secondary_event_key(
                source_incident,
                cleaned_victim,
                incident_date,
            )

            existing = self.source_incident_repository.get_by_source_event_key(
                session,
                source_incident.source_name,
                event_key,
            )
            if existing is not None:
                self.intake_service.ensure_initial_processing_task(session, existing)
                continue

            notes = _build_roundup_secondary_notes(
                source_url=article_url,
                brief_description=brief_description,
            )
            if entry_via_vendor and stub_vendor:
                # Guard against a later re-enrichment hallucinating the vendor's
                # aggregate record count onto this single institution.
                notes = (
                    f"{notes} Affected via compromised vendor {stub_vendor}. "
                    "Vendor-level aggregate totals (records/users) belong to the "
                    "vendor/campaign, NOT to this institution — only attribute "
                    "figures explicitly stated for this institution."
                ).strip()
            raw_payload = {
                "kind": "vendor_victim_stub" if entry_via_vendor else "roundup_secondary_stub",
                "roundup_parent_source_incident_id": str(source_incident.id),
                "roundup_parent_source_name": source_incident.source_name,
                "roundup_parent_source_event_key": source_incident.source_event_key,
                "roundup_parent_article_url": article_url,
                "secondary_entry": entry,
            }
            if entry_via_vendor and stub_vendor:
                raw_payload["affected_via_vendor"] = stub_vendor

            stub = SourceIncident(
                id=uuid4(),
                source_name=source_incident.source_name,
                source_group=source_incident.source_group,
                source_event_key=event_key,
                collector_version=source_incident.collector_version,
                collected_at=source_incident.collected_at,
                source_published_at=source_incident.source_published_at,
                raw_title=cleaned_victim,
                raw_subtitle=brief_description,
                raw_victim_name=cleaned_victim,
                raw_institution_name=cleaned_victim,
                raw_institution_type=None,
                raw_country=country,
                raw_region=None,
                raw_city=None,
                raw_incident_date=incident_date,
                raw_date_precision=_infer_date_precision(incident_date),
                raw_status="suspected",
                raw_attack_hint=attack_hint,
                raw_threat_actor=None,
                raw_notes=notes,
                source_confidence=source_incident.source_confidence,
                ingest_hash=event_key,
                raw_payload=raw_payload,
                is_deleted=False,
            )
            stub.urls = []
            self.source_incident_repository.add(session, stub)
            self.intake_service.ensure_initial_processing_task(session, stub)
            created += 1

        return created

    def _select_article(
        self, session, source_incident
    ) -> Tuple[Optional[ArticleContent], Optional[object], Optional[str]]:
        document = self.article_repository.get_selected_document(session, source_incident.id)
        if document is None:
            return None, None, None

        url = None
        if document.source_incident_url_id:
            for row in source_incident.urls or []:
                if row.id == document.source_incident_url_id:
                    url = row.resolved_url or row.url
                    break
        if url is None:
            metadata = document.document_metadata or {}
            url = metadata.get("fetched_url") or metadata.get("source_url")
        if not url:
            return None, document, None

        article = ArticleContent(
            url=url,
            title=document.title or "",
            content=_trim_article_boilerplate_tail(document.content_text or ""),
            author=document.author,
            publish_date=document.publish_date.isoformat() if document.publish_date else None,
            fetch_successful=True,
            error_message=None,
            content_length=len(document.content_text or ""),
        )
        return article, document, url

    def enrich_source_incident(
        self,
        session,
        source_incident,
        *,
        re_enrich_attempts: int | None = None,
        re_enrich_reason: str | None = None,
        force_canonicalize: bool = False,
    ) -> Dict[str, object]:
        article_content, document, article_url = self._select_article(session, source_incident)
        if article_content is None or document is None or not article_url:
            enrichment = self.source_enrichment_repository.get_by_source_incident(
                session, source_incident.id
            )
            if enrichment is None:
                enrichment = SourceEnrichment(
                    source_incident_id=source_incident.id,
                    article_document_id=None,
                )
            if re_enrich_attempts is not None:
                enrichment.re_enrich_attempts = int(re_enrich_attempts)
            if re_enrich_reason is not None:
                enrichment.re_enrich_reason = re_enrich_reason
            enrichment.failed_reason = "No selected article available for enrichment"
            enrichment.is_education_related = None
            enrichment.manual_review_required = False
            enrichment.manual_review_reason = None
            self.source_enrichment_repository.add(session, enrichment)
            return {
                "enriched": False,
                "reason": "missing_article",
                "canonicalize_tasks_enqueued": 0,
                "secondary_source_incidents_created": 0,
            }

        existing_enrichment = self.source_enrichment_repository.get_by_source_incident(
            session, source_incident.id
        )
        is_explicit_reenrich = (
            force_canonicalize
            or re_enrich_attempts is not None
            or re_enrich_reason is not None
        )
        if (
            existing_enrichment is not None
            and not is_explicit_reenrich
            and existing_enrichment.article_document_id is not None
            and str(existing_enrichment.article_document_id) == str(document.id)
            and existing_enrichment.failed_reason != "No selected article available for enrichment"
        ):
            return {
                "enriched": existing_enrichment.typed_enrichment is not None,
                "is_education_related": existing_enrichment.is_education_related,
                "has_typed_enrichment": existing_enrichment.typed_enrichment is not None,
                "article_document_id": str(document.id),
                "canonicalize_tasks_enqueued": 0,
                "secondary_source_incidents_created": 0,
                "skipped_already_enriched": 1,
            }
        effective_attempts = (
            int(re_enrich_attempts)
            if re_enrich_attempts is not None
            else (
                int(existing_enrichment.re_enrich_attempts or 0)
                if existing_enrichment is not None
                else None
            )
        )
        effective_reason = (
            re_enrich_reason
            if re_enrich_reason is not None
            else existing_enrichment.re_enrich_reason if existing_enrichment is not None else None
        )

        base_incident = source_incident_to_base_incident(
            source_incident,
            article_url,
            re_enrich_attempts=effective_attempts,
            re_enrich_reason=effective_reason,
        )
        result, raw_json_data = self.enricher._enrich_article(
            base_incident,
            {article_url: article_content},
        )

        storage_debug = (
            (raw_json_data or {}).get("_storage_debug", {})
            if isinstance(raw_json_data, dict)
            else {}
        )
        llm_metadata = storage_debug.get("llm_metadata", {})
        raw_llm_responses = storage_debug.get("raw_llm_responses", {})
        typed_enrichment = (
            result.model_dump(mode="json", exclude_none=False) if result is not None else None
        )
        if isinstance(raw_json_data, dict):
            source_publish_date = _source_or_document_publish_date(
                source_incident,
                document,
                raw_json_data,
            )
            raw_json_data = _repair_yearless_dates_in_payload(
                raw_json_data,
                publish_date=source_publish_date,
                article_content=document.content_text,
            )
            typed_enrichment = _repair_yearless_dates_in_payload(
                typed_enrichment,
                publish_date=source_publish_date,
                article_content=document.content_text,
            )
            # Deterministic extraction-quality repairs (institution_type inference,
            # third-party-vendor flag reconciliation) applied to both layers.
            raw_json_data = _apply_extraction_quality_fixes(raw_json_data)
            typed_enrichment = _apply_extraction_quality_fixes(typed_enrichment)
        is_education_related = None
        if isinstance(raw_json_data, dict):
            is_education_related = raw_json_data.get("is_edu_cyber_incident")
            if is_education_related is None and raw_json_data.get("_not_education_related"):
                is_education_related = False

        if (
            isinstance(raw_json_data, dict)
            and is_education_related is False
            and _is_university_branded_healthcare_context(
                source_incident,
                raw_json_data,
                typed_enrichment,
                article_title=document.title,
                article_content=document.content_text,
            )
        ):
            raw_json_data, typed_enrichment = _mark_academic_medical_center_in_scope(
                source_incident,
                raw_json_data,
                typed_enrichment,
            )
            result = result or object()
            is_education_related = True

        if (
            isinstance(raw_json_data, dict)
            and is_education_related is False
            and _structured_curated_source_should_review_non_edu_article(source_incident)
        ):
            # Path A: a curated breach DB authoritatively names this education
            # incident. Trust the structured record over the weak/unrelated article
            # and KEEP it (flagged low article-support) instead of parking it in
            # manual review — recovering real curated coverage.
            raw_json_data, typed_enrichment = _mark_structured_curated_in_scope(
                source_incident,
                raw_json_data,
                typed_enrichment,
            )
            result = result or object()
            is_education_related = True

        if (
            result is not None
            and isinstance(raw_json_data, dict)
            and is_education_related is not False
        ):
            raw_json_data, typed_enrichment, disposition = _repair_or_reject_primary_identity(
                source_incident,
                raw_json_data=raw_json_data,
                typed_enrichment=typed_enrichment,
                article_title=document.title,
                article_content=document.content_text,
            )
            if disposition == "reject":
                result = None
                is_education_related = False
            elif disposition == "review":
                result = None
                is_education_related = None

        enrichment = existing_enrichment
        if enrichment is None:
            enrichment = SourceEnrichment(
                source_incident_id=source_incident.id,
                article_document_id=document.id,
            )

        enrichment.article_document_id = document.id
        enrichment.llm_provider = llm_metadata.get("provider", "ollama")
        enrichment.llm_model = llm_metadata.get("model") or getattr(
            self.enricher.llm_client, "model", None
        )
        enrichment.prompt_version = llm_metadata.get("prompt_version")
        enrichment.schema_version = llm_metadata.get("schema_version")
        enrichment.mapper_version = llm_metadata.get("mapper_version")
        enrichment.post_processing_version = llm_metadata.get("post_processing_version")
        enrichment.raw_response = raw_llm_responses or None
        enrichment.raw_extraction = (
            _strip_storage_debug(raw_json_data) if isinstance(raw_json_data, dict) else None
        )
        enrichment.typed_enrichment = typed_enrichment
        enrichment.enrichment_confidence = (
            raw_json_data.get("confidence_score")
            if isinstance(raw_json_data, dict)
            and isinstance(raw_json_data.get("confidence_score"), (int, float))
            else None
        )
        enrichment.is_education_related = is_education_related
        enrichment.re_enrich_attempts = int(effective_attempts or 0)
        enrichment.re_enrich_reason = effective_reason
        enrichment.manual_review_required = bool(
            isinstance(raw_json_data, dict) and raw_json_data.get("_manual_review_required")
        )
        enrichment.manual_review_reason = (
            raw_json_data.get("_reason")
            if enrichment.manual_review_required and isinstance(raw_json_data, dict)
            else None
        )
        enrichment.failed_reason = None
        if result is None:
            if isinstance(raw_json_data, dict):
                enrichment.failed_reason = (
                    raw_json_data.get("_reason") or "Enrichment returned no typed result"
                )
            else:
                enrichment.failed_reason = "Enrichment returned no typed result"

        self.source_enrichment_repository.add(session, enrichment)
        secondary_source_incidents_created = 0
        if isinstance(raw_json_data, dict):
            secondary_source_incidents_created = self._create_secondary_source_incidents(
                session,
                source_incident=source_incident,
                article_url=article_url,
                raw_json_data=raw_json_data,
            )

        canonicalize_tasks_enqueued = 0
        if force_canonicalize or (result is not None and is_education_related is not False):
            existing_canonicalize_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="canonicalize",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_canonicalize_task is None:
                self.pipeline_task_repository.enqueue(
                    session,
                    PipelineTask(
                        run_id=None,
                        task_type="canonicalize",
                        target_table="source_incidents",
                        target_id=source_incident.id,
                        status="queued",
                        priority=120,
                        payload={
                            "source_incident_id": str(source_incident.id),
                            "source_name": source_incident.source_name,
                            "trigger": "reenrich" if force_canonicalize else "enrich_source",
                        },
                        result={},
                        available_at=datetime.now(timezone.utc),
                        attempt_count=0,
                        max_attempts=5,
                    ),
                )
                canonicalize_tasks_enqueued = 1

        return {
            "enriched": result is not None,
            "is_education_related": is_education_related,
            "has_typed_enrichment": typed_enrichment is not None,
            "article_document_id": str(document.id),
            "canonicalize_tasks_enqueued": canonicalize_tasks_enqueued,
            "secondary_source_incidents_created": secondary_source_incidents_created,
        }
