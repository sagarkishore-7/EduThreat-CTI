"""Canonical incident creation and update for the v2 runtime."""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.edu_cti.core.countries import get_country_code, normalize_country
from src.edu_cti.pipeline.phase2.utils.deduplication import (
    _SURVIVOR_SOURCE_RANK,
    clean_institution_name,
    choose_best_institution_name,
    dates_within_window,
    institution_names_match,
    parse_incident_date,
)
from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti_v2.models import (
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    CanonicalTimelineEvent,
    PipelineTask,
    SourceIncident,
    SourceEnrichment,
)
from src.edu_cti_v2.normalization import normalize_ransomware_family, normalize_threat_actor_name
from src.edu_cti_v2.repositories import (
    AnalyticsRefreshRepository,
    CanonicalIncidentRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
    SourceIncidentRepository,
)
from src.edu_cti_v2.source_identity import (
    identity_matches_source_anchor,
    looks_geographic_only_identity,
    recover_source_identity,
)

_VENDOR_LIKE_TYPES = {
    "edtech_platform",
    "education_vendor",
    "education_technology_provider",
    "third_party",
    "tutoring_service",
}
_VENDOR_FOLLOWUP_CUES = (
    "attorney general",
    "charges",
    "charged",
    "class action",
    "class-action",
    "guilty",
    "investigation",
    "lawsuit",
    "plead guilty",
    "prison",
    "prison term",
    "probe",
    "prosecutor",
    "prosecutors",
    "regulator",
    "regulators",
    "sentence",
    "sentenced",
    "sentencing",
    "settlement",
    "share blame",
    "sues",
    "sued",
)
_KNOWN_EDTECH_VENDOR_RE = re.compile(
    r"^(?:"
    r"blackbaud|blackboard|brightspace|canvas|finalsite|illuminate education|"
    r"instructure|powerschool|raptor technologies|skyward|veracross"
    r")$",
    re.IGNORECASE,
)
_KNOWN_EDTECH_VENDOR_ALIASES = {
    "canvas": "instructure",
    "canvas lms": "instructure",
    "canvas learning management system": "instructure",
    "instructure inc": "instructure",
    "instructure incorporated": "instructure",
}
_VENDOR_CONTEXT_CUES = (
    "education tech provider",
    "education technology provider",
    "edtech",
    "ed-tech",
    "learning management system",
    "parent company",
    "platform",
    "school software",
    "software vendor",
    "student information system",
    "technology provider",
    "vendor",
)
_GENERIC_EDU_ENTITY_RE = (
    r"(?:university|college|school|academy|institute|polytechnic|library|district|"
    r"school district|community college|technical college|research university|research institute)"
)
_GENERIC_INSTITUTION_RE = re.compile(
    r"^(?:(?:the\s+website\s+of\s+)?(?:a|an|the)\s+)?"
    r"(?:public\s+|private\s+|state\s+|local\s+|regional\s+|major\s+|leading\s+)?"
    rf"(?:{_GENERIC_EDU_ENTITY_RE})(?:\s+{_GENERIC_EDU_ENTITY_RE})*"
    r"(?:\s+in\b.*)?$",
    re.IGNORECASE,
)
_ORGANIZATIONAL_SUBUNIT_SUFFIXES = (
    "academy",
    "center",
    "centre",
    "college_of_",
    "department_of_",
    "faculty_of_",
    "hospital",
    "institute_of_",
    "laboratory",
    "labs",
    "library",
    "medical_center",
    "medical_school",
    "school_of_",
)

_DISCLOSURE_FIELD_LABELS = {
    "institution_name": "Institution",
    "institution_type": "Institution Type",
    "vendor_name": "Vendor",
    "country": "Country",
    "region": "Region",
    "city": "City",
    "incident_date": "Incident Date",
    "date_precision": "Date Precision",
    "attack_category": "Attack Category",
    "attack_vector": "Attack Vector",
    "threat_actor_name": "Threat Actor",
    "ransomware_family": "Ransomware Family",
    "severity": "Severity",
    "canonical_summary": "Incident Summary",
    "records_affected_exact": "Records Affected",
    "records_affected_min": "Records Affected Min",
    "records_affected_max": "Records Affected Max",
    "data_categories": "Data Categories",
    "data_exfiltrated": "Data Exfiltrated",
    "data_breached": "Data Breached",
    "systems_affected": "Systems Affected",
    "critical_systems_affected": "Critical Systems Affected",
    "third_party_vendor_impact": "Third-Party Vendor Impact",
    "vendor_name_detail": "Named Vendor",
    "total_individuals_affected": "Individuals Affected",
    "ransom_amount_exact": "Ransom Amount",
    "public_disclosure": "Public Disclosure",
    "public_disclosure_date": "Public Disclosure Date",
    "recovery_duration_days": "Recovery Duration",
}
_COMPLETENESS_SCORE_FIELDS = (
    "institution_name",
    "institution_type",
    "country",
    "region",
    "city",
    "incident_date",
    "date_precision",
    "attack_category",
    "attack_vector",
    "severity",
    "canonical_summary",
    "threat_actor_name",
    "ransomware_family",
    "records_affected_exact",
    "records_affected_min",
    "records_affected_max",
    "data_categories",
    "data_exfiltrated",
    "systems_affected",
    "critical_systems_affected",
    "third_party_vendor_impact",
    "vendor_name",
    "public_disclosure_date",
    "recovery_duration_days",
    "mitre_attack_techniques",
    "diamond_model",
    "timeline",
)


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, dict)) and not value:
            continue
        return value
    return None


def _as_candidate_list(value: Any) -> list[Any]:
    try:
        return list(value or [])
    except TypeError:
        return []


def _parse_date_only(value: Any) -> Optional[date]:
    parsed = parse_incident_date(value)
    return parsed.date() if parsed else None


def _normalize_canonical_date_precision(value: Any) -> Optional[str]:
    if value is None:
        return None

    text = str(value).strip().lower()
    if not text:
        return None

    mapping = {
        "exact": "day",
        "day_exact": "day",
        "day": "day",
        "week": "week",
        "week_only": "week",
        "month": "month",
        "month_only": "month",
        "year": "year",
        "year_only": "year",
        "approx": "approximate",
        "approximate": "approximate",
        "estimated": "approximate",
        "unknown": "unknown",
    }
    return mapping.get(text, "approximate")


def _count_present_fields(payload: Any) -> int:
    if payload is None:
        return 0
    if isinstance(payload, dict):
        return sum(_count_present_fields(value) for value in payload.values())
    if isinstance(payload, list):
        return sum(_count_present_fields(value) for value in payload)
    if isinstance(payload, str):
        return 1 if payload.strip() else 0
    return 1


def _canonical_completeness_score(payload: Any) -> float:
    """Return a bounded 0-100 completeness percentage for DB storage.

    The legacy implementation stored a recursive present-field count. Merged
    canonicals can contain large source/provenance arrays, so that count can
    exceed the NUMERIC(5,2) column limit. A fixed denominator keeps the metric
    comparable across incidents and safe for PostgreSQL.
    """

    if not isinstance(payload, dict) or not payload:
        return 0.0
    present = sum(1 for field in _COMPLETENESS_SCORE_FIELDS if _value_present(payload.get(field)))
    if present <= 0:
        return 0.0
    score = (present / len(_COMPLETENESS_SCORE_FIELDS)) * 100
    return round(min(100.0, max(0.0, score)), 2)


def _value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _normalize_disclosure_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        items = [_normalize_disclosure_value(item) for item in value]
        return [item for item in items if _value_present(item)]
    if isinstance(value, dict):
        normalized = {}
        for key, item in value.items():
            normalized_item = _normalize_disclosure_value(item)
            if _value_present(normalized_item):
                normalized[str(key)] = normalized_item
        return normalized or None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return value


def _json_fingerprint(value: Any) -> str:
    return json.dumps(_normalize_disclosure_value(value), sort_keys=True, ensure_ascii=True)


def _looks_generic_institution_label(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if looks_geographic_only_identity(text):
        return True
    if _GENERIC_INSTITUTION_RE.match(text):
        return True
    lowered = text.lower()
    words = text.split()
    if lowered.startswith(("several ", "multiple ", "various ", "few ", "many ", "some ")):
        return True
    if "website of" in lowered or "websites of" in lowered:
        return True
    if re.search(
        r"\b(?:few|several|multiple|various|many|some)\s+(?:colleges?|schools?|universities?|districts?)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if len(words) >= 10:
        return True
    if text.endswith("?"):
        return True
    if len(words) >= 6 and any(punct in text for punct in (":", ";")):
        return True
    return False


def _resolve_institution_name(
    source_incident, typed: Dict[str, Any], raw: Dict[str, Any]
) -> Optional[str]:
    extracted_candidates = [
        typed.get("institution_name"),
        typed.get("institution_name_en"),
        raw.get("institution_name"),
        raw.get("institution_name_en"),
    ]
    for candidate in extracted_candidates:
        cleaned = clean_institution_name(candidate)
        if (
            cleaned
            and not _looks_generic_institution_label(cleaned)
            and not is_headline_format(cleaned, source_incident.raw_title)
        ):
            return cleaned

    resolved = choose_best_institution_name(
        *extracted_candidates,
        source_incident.raw_institution_name,
        source_incident.raw_victim_name,
        source_incident.raw_subtitle,
        source_incident.raw_title,
    )
    recovered_source_identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    )
    if resolved and is_headline_format(resolved, source_incident.raw_title):
        resolved = recovered_source_identity or None
    if not resolved or _looks_generic_institution_label(resolved):
        resolved = recovered_source_identity or resolved
    if resolved and is_headline_format(resolved, source_incident.raw_title):
        return None
    if _looks_generic_institution_label(resolved):
        return None
    return resolved


def _resolved_projection_identity(projection: Dict[str, Any]) -> Optional[str]:
    for candidate in (projection.get("vendor_name"), projection.get("institution_name")):
        cleaned = clean_institution_name(candidate)
        if cleaned and not _looks_generic_institution_label(cleaned):
            return cleaned
    return None


def _should_trust_raw_identity_fallback(
    source_incident,
    institution_name: Optional[str],
    vendor_name: Optional[str],
) -> bool:
    resolved_identity = clean_institution_name(institution_name or vendor_name)
    raw_identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    ) or choose_best_institution_name(
        source_incident.raw_institution_name,
        source_incident.raw_victim_name,
    )
    if not resolved_identity or not raw_identity:
        return True
    return _identity_match_quality(resolved_identity, raw_identity) >= 85


def _normalize_country_fields(
    source_incident,
    typed: Dict[str, Any],
    raw: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    country = _first_present(
        typed.get("country"),
        typed.get("institution_country"),
        raw.get("country"),
        raw.get("institution_country"),
        source_incident.raw_country,
    )
    country_code = _first_present(
        typed.get("country_code"),
        raw.get("country_code"),
    )

    if country:
        country = normalize_country(str(country))
    elif country_code:
        country = normalize_country(str(country_code))

    if country_code:
        country_code = str(country_code).strip().upper()
    elif country:
        country_code = get_country_code(country)

    if not country and country_code:
        country = normalize_country(country_code)

    return country, country_code


def _choose_canonical_institution_name(
    existing: Optional[str], new: Optional[str]
) -> Optional[str]:
    existing_clean = clean_institution_name(existing)
    new_clean = clean_institution_name(new)

    if new_clean and (
        _looks_generic_institution_label(existing_clean)
        and not _looks_generic_institution_label(new_clean)
    ):
        return new_clean
    if existing_clean and (
        _looks_generic_institution_label(new_clean)
        and not _looks_generic_institution_label(existing_clean)
    ):
        return existing_clean

    return choose_best_institution_name(existing_clean, new_clean)


def _normalized_identity(value: Optional[str]) -> Optional[str]:
    cleaned = clean_institution_name(value)
    if not cleaned:
        return None
    normalized = re.sub(r"\s+", " ", cleaned).strip().lower()
    return normalized or None


def _known_edtech_vendor_identity(value: Optional[str]) -> Optional[str]:
    normalized = _normalized_identity(value)
    if not normalized:
        return None
    normalized = re.sub(
        r"\b(?:inc|incorporated|llc|ltd|corp|corporation)\.?\b",
        "",
        normalized,
    )
    normalized = re.sub(r"\s+", " ", normalized).strip(" .")
    return _KNOWN_EDTECH_VENDOR_ALIASES.get(normalized, normalized)


def _identity_match_quality(left: Optional[str], right: Optional[str]) -> int:
    left_normalized = _normalized_identity(left)
    right_normalized = _normalized_identity(right)
    if not left_normalized or not right_normalized:
        return 0
    left_vendor = _known_edtech_vendor_identity(left_normalized)
    right_vendor = _known_edtech_vendor_identity(right_normalized)
    if (
        left_vendor
        and right_vendor
        and left_vendor == right_vendor
        and (
            left_vendor != left_normalized
            or right_vendor != right_normalized
            or _is_known_edtech_vendor_name(left_normalized)
            or _is_known_edtech_vendor_name(right_normalized)
        )
    ):
        return 100
    if left_normalized == right_normalized:
        return 100
    left_stripped = re.sub(r"\s+\([A-Za-z0-9&.\- ]{2,}\)$", "", left_normalized).strip()
    right_stripped = re.sub(r"\s+\([A-Za-z0-9&.\- ]{2,}\)$", "", right_normalized).strip()
    if left_stripped and right_stripped and left_stripped == right_stripped:
        return 95
    if identity_matches_source_anchor(left_normalized, right_normalized, threshold=85):
        return 90
    if institution_names_match(left_normalized, right_normalized, threshold=92):
        return 92
    if institution_names_match(left_normalized, right_normalized, threshold=85):
        return 85
    if _is_subunit_identity_match(left_normalized, right_normalized):
        return 88
    return 0


def _is_subunit_identity_match(left_normalized: str, right_normalized: str) -> bool:
    def _matches_parent(base: str, extended: str) -> bool:
        if not extended.startswith(f"{base} "):
            return False
        remainder = extended[len(base) :].strip()
        if not remainder:
            return False
        remainder_key = re.sub(r"\s+", "_", remainder)
        return any(
            remainder_key == suffix or remainder_key.startswith(suffix)
            for suffix in _ORGANIZATIONAL_SUBUNIT_SUFFIXES
        )

    return _matches_parent(left_normalized, right_normalized) or _matches_parent(
        right_normalized,
        left_normalized,
    )


def _attack_category_family(value: Optional[str]) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text.startswith("data_breach_") or text.startswith("ransomware_"):
        return "data_compromise"
    if text.startswith("third_party") or text.startswith("supply_chain"):
        return "vendor_compromise"
    return text


def _projection_text(projection: Dict[str, Any]) -> str:
    return " ".join(
        str(value).strip().lower()
        for value in (
            projection.get("raw_title"),
            projection.get("raw_subtitle"),
            projection.get("canonical_summary"),
            projection.get("institution_type"),
        )
        if value
    )


def _is_known_edtech_vendor_name(value: Optional[str]) -> bool:
    identity = clean_institution_name(value)
    return bool(identity and _KNOWN_EDTECH_VENDOR_RE.match(identity))


def _is_vendor_like_projection(projection: Dict[str, Any]) -> bool:
    if projection.get("vendor_name"):
        return True
    institution_type = str(projection.get("institution_type") or "").strip().lower()
    if institution_type in _VENDOR_LIKE_TYPES:
        return True

    identity = clean_institution_name(projection.get("institution_name"))
    if _is_known_edtech_vendor_name(identity):
        return True

    text = _projection_text(projection)
    return bool(identity and any(cue in text for cue in _VENDOR_CONTEXT_CUES))


def _looks_vendor_followup_coverage(projection: Dict[str, Any]) -> bool:
    if not _is_vendor_like_projection(projection):
        return False
    text = _projection_text(projection)
    return any(cue in text for cue in _VENDOR_FOLLOWUP_CUES)


def _same_event_category_family(projection: Dict[str, Any], candidate: CanonicalIncident) -> bool:
    projection_family = _attack_category_family(projection.get("attack_category"))
    candidate_family = _attack_category_family(candidate.attack_category)
    return bool(projection_family and candidate_family and projection_family == candidate_family)


def _score_candidate_match(
    projection: Dict[str, Any],
    candidate: CanonicalIncident,
    *,
    relaxed_vendor_followup: bool = False,
    allow_exact_cross_country_same_event: bool = False,
) -> Tuple[float, Optional[str]]:
    incoming_identities = [
        ("institution_name", projection.get("institution_name")),
        ("vendor_name", projection.get("vendor_name")),
    ]
    candidate_identities = [
        ("institution_name", candidate.institution_name),
        ("vendor_name", candidate.vendor_name),
    ]

    best_quality = 0
    best_match_type: Optional[str] = None
    best_incoming_value: Optional[str] = None
    best_candidate_value: Optional[str] = None

    for incoming_kind, incoming_value in incoming_identities:
        for candidate_kind, candidate_value in candidate_identities:
            quality = _identity_match_quality(incoming_value, candidate_value)
            if quality <= best_quality:
                continue
            best_quality = quality
            if "vendor" in {incoming_kind, candidate_kind}:
                best_match_type = "vendor_date"
            else:
                best_match_type = "name_date"
            best_incoming_value = incoming_value
            best_candidate_value = candidate_value

    vendor_quality = _identity_match_quality(projection.get("vendor_name"), candidate.vendor_name)
    if vendor_quality and vendor_quality >= best_quality:
        best_quality = vendor_quality
        best_match_type = "vendor_date"
        best_incoming_value = projection.get("vendor_name")
        best_candidate_value = candidate.vendor_name

    if best_quality <= 0 or best_match_type is None:
        return 0.0, None
    exact_vendor_identity = best_match_type == "vendor_date" and best_quality >= 100

    projection_date = parse_incident_date(
        str(projection.get("incident_date")) if projection.get("incident_date") else None
    )
    candidate_date = parse_incident_date(
        str(candidate.incident_date) if candidate.incident_date else None
    )
    if projection_date and candidate_date:
        if dates_within_window(projection_date, candidate_date, 14):
            exact_known_vendor_campaign = exact_vendor_identity and (
                _is_known_edtech_vendor_name(best_incoming_value)
                or _is_known_edtech_vendor_name(best_candidate_value)
            )
            date_score = (
                20.0
                if exact_known_vendor_campaign
                or dates_within_window(projection_date, candidate_date, 3)
                else 12.0
            )
        elif (
            relaxed_vendor_followup
            and exact_vendor_identity
            and dates_within_window(projection_date, candidate_date, 400)
        ):
            date_score = 8.0
        else:
            return 0.0, None
    else:
        date_score = 4.0

    projection_country = projection.get("country_code")
    same_country = (
        projection_country
        and candidate.country_code
        and projection_country == candidate.country_code
    )
    if projection_country and candidate.country_code and not same_country:
        exact_same_event = (
            allow_exact_cross_country_same_event
            and best_quality >= 100
            and projection_date is not None
            and candidate_date is not None
            and dates_within_window(projection_date, candidate_date, 3)
            and _same_event_category_family(projection, candidate)
        )
        if not ((relaxed_vendor_followup and exact_vendor_identity) or exact_same_event):
            return 0.0, None

    score = float(best_quality)
    score += date_score

    if same_country:
        score += 8.0
    elif projection_country and candidate.country_code:
        score -= 4.0
    elif projection_country or candidate.country_code:
        score += 2.0

    if projection.get("attack_category") and candidate.attack_category == projection.get(
        "attack_category"
    ):
        score += 6.0
    else:
        projection_attack_family = _attack_category_family(projection.get("attack_category"))
        candidate_attack_family = _attack_category_family(candidate.attack_category)
        if (
            relaxed_vendor_followup
            and projection_attack_family
            and projection_attack_family == candidate_attack_family
        ):
            score += 4.0
        elif (
            relaxed_vendor_followup
            and exact_vendor_identity
            and (
                _is_known_edtech_vendor_name(best_incoming_value)
                or _is_known_edtech_vendor_name(best_candidate_value)
            )
        ):
            # Legal/regulatory follow-up articles often relabel the same vendor incident
            # as breach, extortion, or ransomware depending on the article angle.
            score -= 2.0
        elif (
            relaxed_vendor_followup
            and projection.get("attack_category")
            and candidate.attack_category
        ):
            return 0.0, None
    if projection.get("ransomware_family") and candidate.ransomware_family == projection.get(
        "ransomware_family"
    ):
        score += 4.0
    if projection.get("threat_actor_name") and candidate.threat_actor_name == projection.get(
        "threat_actor_name"
    ):
        score += 4.0

    if _looks_generic_institution_label(best_incoming_value) or _looks_generic_institution_label(
        best_candidate_value
    ):
        score -= 20.0
    if exact_vendor_identity and (
        _is_known_edtech_vendor_name(best_incoming_value)
        or _is_known_edtech_vendor_name(best_candidate_value)
    ):
        # Vendor-wide incidents are often covered over several days with
        # inconsistent incident dates. Prefer the larger existing campaign
        # cluster so late and early source articles converge instead of
        # fragmenting into date-nearest canonicals.
        score += min(len(getattr(candidate, "memberships", []) or []), 50) / 5.0

    return score, best_match_type


def _url_candidate_identity_compatible(
    projection: Dict[str, Any],
    candidate: CanonicalIncident,
) -> bool:
    incoming_identities = [
        projection.get("institution_name"),
        projection.get("vendor_name"),
    ]
    candidate_identities = [
        candidate.institution_name,
        candidate.vendor_name,
    ]

    best_quality = 0
    for incoming_value in incoming_identities:
        for candidate_value in candidate_identities:
            best_quality = max(
                best_quality, _identity_match_quality(incoming_value, candidate_value)
            )

    if best_quality < 85:
        return False

    projection_country = projection.get("country_code")
    if (
        projection_country
        and candidate.country_code
        and projection_country != candidate.country_code
        and best_quality < 100
    ):
        return False

    return True


def build_source_projection(source_incident, source_enrichment: SourceEnrichment) -> Dict[str, Any]:
    """Project a source incident + source enrichment into canonical-shaped fields."""
    typed = source_enrichment.typed_enrichment or {}
    raw = source_enrichment.raw_extraction or {}
    attack_dynamics = typed.get("attack_dynamics") or raw.get("attack_dynamics") or {}

    institution_name = _resolve_institution_name(source_incident, typed, raw)
    institution_type = _first_present(
        typed.get("institution_type"),
        raw.get("institution_type"),
        source_incident.raw_institution_type,
    )
    vendor_name = _first_present(typed.get("vendor_name"), raw.get("vendor_name"))
    if not vendor_name and institution_name and institution_type in _VENDOR_LIKE_TYPES:
        vendor_name = institution_name
    if not vendor_name and _is_known_edtech_vendor_name(institution_name):
        vendor_name = institution_name
    trust_raw_identity_fallback = _should_trust_raw_identity_fallback(
        source_incident,
        institution_name,
        vendor_name,
    )
    country, country_code = _normalize_country_fields(source_incident, typed, raw)
    if not trust_raw_identity_fallback:
        if (
            typed.get("country") is None
            and typed.get("institution_country") is None
            and raw.get("country") is None
            and raw.get("institution_country") is None
        ):
            country = None
        if (
            typed.get("country_code") is None
            and raw.get("country_code") is None
            and country is None
        ):
            country_code = None
    region = _first_present(
        typed.get("region"),
        raw.get("region"),
        source_incident.raw_region if trust_raw_identity_fallback else None,
    )
    city = _first_present(
        typed.get("city"),
        raw.get("city"),
        source_incident.raw_city if trust_raw_identity_fallback else None,
    )
    incident_date = _parse_date_only(
        _first_present(
            typed.get("incident_date"), raw.get("incident_date"), source_incident.raw_incident_date
        )
    )
    date_precision = _normalize_canonical_date_precision(
        _first_present(
            typed.get("incident_date_precision"),
            raw.get("incident_date_precision"),
            typed.get("date_precision"),
            raw.get("date_precision"),
            source_incident.raw_date_precision,
        )
    )
    attack_category = _first_present(typed.get("attack_category"), raw.get("attack_category"))
    attack_vector = _first_present(
        attack_dynamics.get("attack_vector"),
        typed.get("attack_vector"),
        raw.get("attack_vector"),
    )
    ransomware_family = normalize_ransomware_family(
        _first_present(
            attack_dynamics.get("ransomware_family"),
            typed.get("ransomware_family"),
            raw.get("ransomware_family"),
        )
    )
    threat_actor_name = normalize_threat_actor_name(
        _first_present(
            typed.get("threat_actor_name"),
            raw.get("threat_actor_name"),
            source_incident.raw_threat_actor,
        )
    )
    canonical_summary = _first_present(
        typed.get("enriched_summary"),
        raw.get("enriched_summary"),
        source_incident.raw_title,
    )
    return {
        "institution_name": institution_name,
        "institution_type": institution_type,
        "vendor_name": vendor_name,
        "raw_title": source_incident.raw_title,
        "raw_subtitle": source_incident.raw_subtitle,
        "country": country,
        "country_code": country_code,
        "region": region,
        "city": city,
        "incident_date": incident_date,
        "date_precision": date_precision,
        "source_published_at": source_incident.source_published_at,
        "attack_category": attack_category,
        "attack_vector": attack_vector,
        "threat_actor_name": threat_actor_name,
        "ransomware_family": ransomware_family,
        "is_education_related": source_enrichment.is_education_related,
        "severity": _first_present(typed.get("incident_severity"), raw.get("incident_severity")),
        "canonical_summary": canonical_summary,
        "typed_enrichment": typed,
        "raw_extraction": raw,
        "timeline": typed.get("timeline") or raw.get("timeline") or [],
    }


def _canonical_key_for_projection(projection: Dict[str, Any], source_incident_id) -> str:
    base = "|".join(
        [
            str(projection.get("institution_name") or projection.get("vendor_name") or "unknown"),
            str(projection.get("incident_date") or "unknown"),
            str(projection.get("country_code") or "unknown"),
            str(source_incident_id),
        ]
    )
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:24]


def _date_precision_rank(value: Optional[str]) -> int:
    ranks = {
        "day": 5,
        "week": 4,
        "month": 3,
        "year": 2,
        "approximate": 1,
        "unknown": 0,
        None: 0,
    }
    return ranks.get(value, 0)


def _safe_canonical_date(value: Optional[date]) -> bool:
    if value is None:
        return True
    lower_bound = date(1990, 1, 1)
    upper_bound = date.today() + timedelta(days=3)
    return lower_bound <= value <= upper_bound


def _apply_projection_to_canonical(
    canonical: CanonicalIncident,
    projection: Dict[str, Any],
    *,
    authoritative: bool = False,
) -> None:
    if authoritative:
        canonical.institution_name = _first_present(
            projection.get("institution_name"),
            projection.get("vendor_name"),
        )
        canonical.institution_type = projection.get("institution_type")
        canonical.vendor_name = projection.get("vendor_name")
        canonical.country = projection.get("country")
        if projection.get("country_code"):
            canonical.country_code = projection.get("country_code")
        elif canonical.country:
            canonical.country_code = get_country_code(canonical.country)
        else:
            canonical.country_code = None
        canonical.region = projection.get("region")
        canonical.city = projection.get("city")
    else:
        canonical.institution_name = _choose_canonical_institution_name(
            canonical.institution_name,
            projection.get("institution_name"),
        )
        if projection.get("institution_type"):
            canonical.institution_type = projection.get("institution_type")
        if projection.get("vendor_name"):
            canonical.vendor_name = projection.get("vendor_name")

        if projection.get("country"):
            canonical.country = projection.get("country")
        if projection.get("country_code"):
            canonical.country_code = projection.get("country_code")
        elif canonical.country and not canonical.country_code:
            canonical.country_code = get_country_code(canonical.country)

        if projection.get("region"):
            canonical.region = projection.get("region")
        if projection.get("city"):
            canonical.city = projection.get("city")

    projection_date = projection.get("incident_date")
    projection_precision = projection.get("date_precision")
    if authoritative:
        if projection_date:
            canonical.incident_date = projection_date
            canonical.date_precision = projection_precision
        elif projection_precision:
            canonical.date_precision = projection_precision
        canonical.source_published_at = projection.get("source_published_at")
        canonical.attack_category = projection.get("attack_category")
        canonical.attack_vector = projection.get("attack_vector")
        canonical.threat_actor_name = normalize_threat_actor_name(
            projection.get("threat_actor_name")
        )
        canonical.ransomware_family = normalize_ransomware_family(
            projection.get("ransomware_family")
        )
        canonical.is_education_related = projection.get("is_education_related")
        canonical.severity = projection.get("severity")
        canonical.canonical_summary = projection.get("canonical_summary")
    else:
        if projection_date and (
            canonical.incident_date is None
            or not _safe_canonical_date(canonical.incident_date)
            or _date_precision_rank(projection_precision)
            > _date_precision_rank(canonical.date_precision)
        ):
            canonical.incident_date = projection_date
            canonical.date_precision = projection_precision
        elif projection_precision and canonical.date_precision is None:
            canonical.date_precision = projection_precision

        if projection.get("source_published_at"):
            canonical.source_published_at = projection.get("source_published_at")
        if projection.get("attack_category"):
            canonical.attack_category = projection.get("attack_category")
        if projection.get("attack_vector"):
            canonical.attack_vector = projection.get("attack_vector")
        if projection.get("threat_actor_name"):
            canonical.threat_actor_name = normalize_threat_actor_name(
                projection.get("threat_actor_name")
            )
        if projection.get("ransomware_family"):
            canonical.ransomware_family = normalize_ransomware_family(
                projection.get("ransomware_family")
            )
        if projection.get("is_education_related") is not None:
            canonical.is_education_related = projection.get("is_education_related")
        if projection.get("severity"):
            canonical.severity = projection.get("severity")
        if projection.get("canonical_summary"):
            canonical.canonical_summary = projection.get("canonical_summary")


def _build_member_score_breakdown(
    source_name: str,
    projection: Dict[str, Any],
    source_enrichment: SourceEnrichment,
) -> Dict[str, int]:
    typed = projection.get("typed_enrichment") or {}
    timeline = projection.get("timeline") or []
    identity = projection.get("vendor_name") or projection.get("institution_name")
    institution_name = projection.get("institution_name")
    vendor_name = projection.get("vendor_name")
    raw_title = str(projection.get("raw_title") or "").strip().lower()
    raw_subtitle = str(projection.get("raw_subtitle") or "").strip().lower()

    breakdown: Dict[str, int] = {
        "source_rank": int(_SURVIVOR_SOURCE_RANK.get(source_name, 0)),
        "structured_field_coverage": int(_count_present_fields(typed)),
        "summary_richness": int(
            min(len(str(projection.get("canonical_summary") or "")) // 120, 10)
        ),
        "timeline_depth": int(min(len(timeline), 10) * 3),
    }
    if projection.get("institution_name"):
        breakdown["named_victim_bonus"] = 10
    if projection.get("incident_date"):
        breakdown["incident_date_bonus"] = 6
    if projection.get("ransomware_family") or projection.get("threat_actor_name"):
        breakdown["actor_or_family_bonus"] = 4
    if projection.get("country_code"):
        breakdown["country_bonus"] = 2
    if vendor_name:
        vendor_is_known_edtech = _is_known_edtech_vendor_name(vendor_name)
        same_vendor_identity = (
            not institution_name or _identity_match_quality(institution_name, vendor_name) >= 85
        )
        if vendor_is_known_edtech and same_vendor_identity:
            breakdown["vendor_wide_source_bonus"] = 18
        elif vendor_is_known_edtech and institution_name:
            breakdown["school_specific_vendor_source_penalty"] = -12
    if identity:
        normalized_identity = _normalized_identity(identity)
        if normalized_identity:
            tokens = [token for token in normalized_identity.split() if len(token) > 2]
            if tokens and any(token in raw_title or token in raw_subtitle for token in tokens[:4]):
                breakdown["identity_title_alignment_bonus"] = 8
            elif raw_title:
                breakdown["identity_title_alignment_penalty"] = -4
    confidence = source_enrichment.enrichment_confidence
    if confidence is not None:
        breakdown["enrichment_confidence_bonus"] = int(float(confidence) * 10)
    return breakdown


def _member_score(
    source_name: str, projection: Dict[str, Any], source_enrichment: SourceEnrichment
) -> int:
    return sum(_build_member_score_breakdown(source_name, projection, source_enrichment).values())


def _build_field_provenance(
    projection: Dict[str, Any],
    source_enrichment_id,
) -> Dict[str, str]:
    provenance: Dict[str, str] = {}
    enrichment_id = str(source_enrichment_id)
    for key in (
        "institution_name",
        "institution_type",
        "vendor_name",
        "country",
        "country_code",
        "region",
        "city",
        "incident_date",
        "date_precision",
        "attack_category",
        "attack_vector",
        "threat_actor_name",
        "ransomware_family",
        "severity",
        "canonical_summary",
    ):
        if projection.get(key) is not None:
            provenance[key] = enrichment_id
    return provenance


def _extract_disclosure_field_values(
    projection: Dict[str, Any],
) -> Dict[str, Any]:
    typed = projection.get("typed_enrichment") or {}
    data_impact = typed.get("data_impact") or {}
    system_impact = typed.get("system_impact") or {}
    user_impact = typed.get("user_impact") or {}
    financial_impact = typed.get("financial_impact") or {}
    recovery_metrics = typed.get("recovery_metrics") or {}
    transparency_metrics = typed.get("transparency_metrics") or {}

    values = {
        "institution_name": projection.get("institution_name"),
        "institution_type": projection.get("institution_type"),
        "vendor_name": projection.get("vendor_name"),
        "country": projection.get("country"),
        "region": projection.get("region"),
        "city": projection.get("city"),
        "incident_date": projection.get("incident_date"),
        "date_precision": projection.get("date_precision"),
        "attack_category": projection.get("attack_category"),
        "attack_vector": projection.get("attack_vector"),
        "threat_actor_name": projection.get("threat_actor_name"),
        "ransomware_family": projection.get("ransomware_family"),
        "severity": projection.get("severity"),
        "canonical_summary": projection.get("canonical_summary"),
        "records_affected_exact": data_impact.get("records_affected_exact"),
        "records_affected_min": data_impact.get("records_affected_min"),
        "records_affected_max": data_impact.get("records_affected_max"),
        "data_categories": data_impact.get("data_categories")
        or data_impact.get("data_types_affected"),
        "data_exfiltrated": data_impact.get("data_exfiltrated"),
        "data_breached": typed.get("data_breached"),
        "systems_affected": system_impact.get("systems_affected"),
        "critical_systems_affected": system_impact.get("critical_systems_affected"),
        "third_party_vendor_impact": system_impact.get("third_party_vendor_impact"),
        "vendor_name_detail": system_impact.get("vendor_name"),
        "total_individuals_affected": user_impact.get("total_individuals_affected"),
        "ransom_amount_exact": financial_impact.get("ransom_amount_exact"),
        "public_disclosure": transparency_metrics.get("public_disclosure"),
        "public_disclosure_date": transparency_metrics.get("public_disclosure_date"),
        "recovery_duration_days": recovery_metrics.get("recovery_duration_days"),
    }
    normalized: Dict[str, Any] = {}
    for key, value in values.items():
        normalized_value = _normalize_disclosure_value(value)
        if _value_present(normalized_value):
            normalized[key] = normalized_value
    return normalized


def _build_source_disclosure_document(
    canonical: CanonicalIncident,
    member_documents: List[Dict[str, Any]],
    *,
    selected_source_enrichment_id: Optional[str],
    projection_field_sources: Optional[Dict[str, List[str]]] = None,
    resolved_field_values: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    selected_enrichment_id = (
        str(selected_source_enrichment_id) if selected_source_enrichment_id else None
    )
    source_entries: List[Dict[str, Any]] = []
    field_contributors: Dict[str, List[str]] = {}

    for document in member_documents:
        enrichment = document["source_enrichment"]
        source_incident = document["source_incident"]
        membership = document["membership"]
        projection = document["projection"]
        field_values = _extract_disclosure_field_values(projection)
        source_enrichment_id = str(enrichment.id)
        source_incident_id = str(source_incident.id)
        for field_name in field_values:
            field_contributors.setdefault(field_name, []).append(source_enrichment_id)

        source_entries.append(
            {
                "source_enrichment_id": source_enrichment_id,
                "source_incident_id": source_incident_id,
                "source_name": source_incident.source_name,
                "source_group": source_incident.source_group,
                "raw_title": source_incident.raw_title,
                "raw_subtitle": source_incident.raw_subtitle,
                "source_published_at": (
                    source_incident.source_published_at.isoformat()
                    if source_incident.source_published_at
                    else None
                ),
                "is_primary_member": bool(membership.is_primary_member),
                "survivor_score": float(membership.survivor_score or 0.0),
                "score_breakdown": document["score_breakdown"],
                "field_count": len(field_values),
                "disclosed_fields": sorted(field_values.keys()),
                "field_values": field_values,
            }
        )

    source_entries.sort(
        key=lambda item: (
            not item["is_primary_member"],
            -float(item.get("survivor_score") or 0.0),
            item.get("source_name") or "",
        )
    )

    canonical_fields = resolved_field_values or {
        "institution_name": _normalize_disclosure_value(canonical.institution_name),
        "institution_type": _normalize_disclosure_value(canonical.institution_type),
        "vendor_name": _normalize_disclosure_value(canonical.vendor_name),
        "country": _normalize_disclosure_value(canonical.country),
        "region": _normalize_disclosure_value(canonical.region),
        "city": _normalize_disclosure_value(canonical.city),
        "incident_date": _normalize_disclosure_value(canonical.incident_date),
        "date_precision": _normalize_disclosure_value(canonical.date_precision),
        "attack_category": _normalize_disclosure_value(canonical.attack_category),
        "attack_vector": _normalize_disclosure_value(canonical.attack_vector),
        "threat_actor_name": _normalize_disclosure_value(canonical.threat_actor_name),
        "ransomware_family": _normalize_disclosure_value(canonical.ransomware_family),
        "severity": _normalize_disclosure_value(canonical.severity),
        "canonical_summary": _normalize_disclosure_value(canonical.canonical_summary),
    }

    field_sources: Dict[str, str] = {}
    preferred_entries = sorted(
        source_entries,
        key=lambda item: (
            str(item.get("source_enrichment_id")) != selected_enrichment_id,
            not item.get("is_primary_member"),
            -float(item.get("survivor_score") or 0.0),
        ),
    )
    for field_name, canonical_value in canonical_fields.items():
        if not _value_present(canonical_value):
            continue
        canonical_fingerprint = _json_fingerprint(canonical_value)
        for entry in preferred_entries:
            entry_value = entry.get("field_values", {}).get(field_name)
            if not _value_present(entry_value):
                continue
            if _json_fingerprint(entry_value) == canonical_fingerprint:
                field_sources[field_name] = str(entry["source_enrichment_id"])
                break

    selected_source = next(
        (
            entry
            for entry in source_entries
            if str(entry.get("source_enrichment_id")) == selected_enrichment_id
        ),
        None,
    )
    return {
        "field_sources": field_sources,
        "field_contributors": field_contributors,
        "projection_field_sources": projection_field_sources or {},
        "source_disclosure": {
            "version": 1,
            "selection_basis": "highest_survivor_score",
            "selected_source_enrichment_id": selected_enrichment_id,
            "selected_source_incident_id": (
                selected_source.get("source_incident_id") if selected_source else None
            ),
            "tracked_field_labels": _DISCLOSURE_FIELD_LABELS,
            "sources": source_entries,
        },
    }


def _add_projection_source(
    provenance: Dict[str, List[str]], path: str, source_enrichment_id: str
) -> None:
    if not path:
        return
    contributors = provenance.setdefault(path, [])
    if source_enrichment_id not in contributors:
        contributors.append(source_enrichment_id)


def _seed_projection_provenance(
    value: Any,
    source_enrichment_id: str,
    provenance: Dict[str, List[str]],
    *,
    path: str = "",
) -> None:
    normalized_value = _normalize_disclosure_value(value)
    if not _value_present(normalized_value):
        return
    if isinstance(normalized_value, dict):
        for key, nested_value in normalized_value.items():
            child_path = f"{path}.{key}" if path else str(key)
            _seed_projection_provenance(
                nested_value, source_enrichment_id, provenance, path=child_path
            )
        return
    _add_projection_source(provenance, path, source_enrichment_id)


def _merge_typed_value(
    target: Any,
    source: Any,
    *,
    path: str,
    source_enrichment_id: str,
    provenance: Dict[str, List[str]],
) -> Any:
    source_normalized = _normalize_disclosure_value(source)
    if not _value_present(source_normalized):
        return target

    target_normalized = _normalize_disclosure_value(target)
    if isinstance(target_normalized, dict) and isinstance(source_normalized, dict):
        merged = dict(target_normalized)
        for key, source_value in source_normalized.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key not in merged:
                merged[key] = deepcopy(source_value)
                _seed_projection_provenance(
                    source_value, source_enrichment_id, provenance, path=child_path
                )
            else:
                merged[key] = _merge_typed_value(
                    merged[key],
                    source_value,
                    path=child_path,
                    source_enrichment_id=source_enrichment_id,
                    provenance=provenance,
                )
        return merged

    if isinstance(target_normalized, list) and isinstance(source_normalized, list):
        if not target_normalized:
            _add_projection_source(provenance, path, source_enrichment_id)
            return deepcopy(source_normalized)

        merged_list = list(target_normalized)
        seen = {_json_fingerprint(item) for item in target_normalized}
        added = False
        for item in source_normalized:
            fingerprint = _json_fingerprint(item)
            if fingerprint in seen:
                continue
            merged_list.append(deepcopy(item))
            seen.add(fingerprint)
            added = True
        if added:
            _add_projection_source(provenance, path, source_enrichment_id)
        return merged_list

    if not _value_present(target_normalized):
        _add_projection_source(provenance, path, source_enrichment_id)
        return deepcopy(source_normalized)
    return target_normalized


def _merge_projection_top_level(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key in (
        "institution_name",
        "institution_type",
        "vendor_name",
        "country",
        "country_code",
        "region",
        "city",
        "attack_category",
        "attack_vector",
        "threat_actor_name",
        "ransomware_family",
        "severity",
        "canonical_summary",
        "is_education_related",
    ):
        if not _value_present(_normalize_disclosure_value(merged.get(key))) and _value_present(
            _normalize_disclosure_value(incoming.get(key))
        ):
            merged[key] = incoming.get(key)

    incoming_date = incoming.get("incident_date")
    incoming_precision = incoming.get("date_precision")
    current_date = merged.get("incident_date")
    current_precision = merged.get("date_precision")
    if incoming_date and (
        current_date is None
        or not _safe_canonical_date(current_date)
        or _date_precision_rank(incoming_precision) > _date_precision_rank(current_precision)
    ):
        merged["incident_date"] = incoming_date
        if incoming_precision:
            merged["date_precision"] = incoming_precision
    elif not current_precision and incoming_precision:
        merged["date_precision"] = incoming_precision

    if (
        merged.get("source_published_at") is None
        and incoming.get("source_published_at") is not None
    ):
        merged["source_published_at"] = incoming.get("source_published_at")
    return merged


def _build_merged_projection(
    selected_projection: Dict[str, Any],
    member_documents: List[Dict[str, Any]],
    *,
    selected_source_enrichment_id: Optional[str],
) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    merged_projection = deepcopy(selected_projection)
    merged_typed = deepcopy(selected_projection.get("typed_enrichment") or {})
    selected_id = str(selected_source_enrichment_id) if selected_source_enrichment_id else None
    projection_field_sources: Dict[str, List[str]] = {}
    if selected_id:
        _seed_projection_provenance(merged_typed, selected_id, projection_field_sources)

    supporting_documents = sorted(
        member_documents,
        key=lambda document: (
            str(document["source_enrichment"].id) == selected_id,
            -float(document["membership"].survivor_score or 0.0),
            document["source_incident"].source_name,
        ),
    )

    for document in supporting_documents:
        source_enrichment = document["source_enrichment"]
        if selected_id and str(source_enrichment.id) == selected_id:
            continue
        incoming_projection = document["projection"]
        incoming_typed = incoming_projection.get("typed_enrichment") or {}
        merged_typed = _merge_typed_value(
            merged_typed,
            incoming_typed,
            path="",
            source_enrichment_id=str(source_enrichment.id),
            provenance=projection_field_sources,
        )
        merged_projection = _merge_projection_top_level(merged_projection, incoming_projection)

    merged_projection["typed_enrichment"] = merged_typed
    merged_projection["timeline"] = (
        merged_typed.get("timeline") or merged_projection.get("timeline") or []
    )
    return merged_projection, projection_field_sources


class V2CanonicalizationService:
    """Create and update canonical incidents with lineage retained."""

    MATCHER_VERSION = "v2-canonicalizer-2"

    def __init__(
        self,
        *,
        canonical_repository: Optional[CanonicalIncidentRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        analytics_refresh_repository: Optional[AnalyticsRefreshRepository] = None,
    ) -> None:
        self.canonical_repository = canonical_repository or CanonicalIncidentRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.source_enrichment_repository = (
            source_enrichment_repository or SourceEnrichmentRepository()
        )
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.analytics_refresh_repository = (
            analytics_refresh_repository or AnalyticsRefreshRepository()
        )

    def _find_existing_canonical(
        self,
        session: Session,
        source_incident,
        projection: Dict[str, Any],
        *,
        exclude_canonical_ids: Optional[Sequence[object]] = None,
    ) -> Tuple[Optional[CanonicalIncident], str, float]:
        excluded_ids = {str(value) for value in (exclude_canonical_ids or ()) if value is not None}
        normalized_urls = [
            row.normalized_url
            for row in (source_incident.urls or [])
            if row.url_kind == "article" and not row.is_wrapper and row.normalized_url
        ]
        url_candidates = [
            candidate
            for candidate in self.canonical_repository.find_by_url_candidates(
                session, normalized_urls
            )
            if str(candidate.id) not in excluded_ids
        ]
        compatible_url_candidates = [
            candidate
            for candidate in url_candidates
            if _url_candidate_identity_compatible(projection, candidate)
        ]
        if compatible_url_candidates:
            return compatible_url_candidates[0], "url_exact", 100.0

        candidate_name = projection.get("institution_name") or projection.get("vendor_name")
        if not candidate_name:
            return None, "seed", 0.0

        candidates = [
            candidate
            for candidate in self.canonical_repository.find_name_date_candidates(
                session,
                incident_date=projection.get("incident_date"),
                country_code=projection.get("country_code"),
            )
            if str(candidate.id) not in excluded_ids
        ]
        best_candidate: Optional[CanonicalIncident] = None
        best_match_type = "seed"
        best_score = 0.0
        for candidate in candidates:
            score, candidate_match_type = _score_candidate_match(projection, candidate)
            if score <= best_score or candidate_match_type is None:
                continue
            best_candidate = candidate
            best_match_type = candidate_match_type
            best_score = score

        if best_candidate is not None and best_score >= 95.0:
            return best_candidate, best_match_type, best_score

        if _looks_vendor_followup_coverage(projection):
            identity_values = [
                value
                for value in (
                    projection.get("institution_name"),
                    projection.get("vendor_name"),
                )
                if value
            ]
            relaxed_candidates = [
                candidate
                for candidate in _as_candidate_list(
                    self.canonical_repository.find_identity_candidates(session, identity_values)
                )
                if str(candidate.id) not in excluded_ids
            ]
            best_candidate = None
            best_score = 0.0
            for candidate in relaxed_candidates:
                score, candidate_match_type = _score_candidate_match(
                    projection,
                    candidate,
                    relaxed_vendor_followup=True,
                )
                if score <= best_score or candidate_match_type is None:
                    continue
                best_candidate = candidate
                best_score = score

            if best_candidate is not None and best_score >= 94.0:
                return best_candidate, "vendor_followup", best_score

        identity_values = [
            value
            for value in (
                projection.get("institution_name"),
                projection.get("vendor_name"),
            )
            if value
        ]
        if identity_values:
            exact_identity_candidates = [
                candidate
                for candidate in _as_candidate_list(
                    self.canonical_repository.find_identity_candidates(session, identity_values)
                )
                if str(candidate.id) not in excluded_ids
            ]
            best_candidate = None
            best_score = 0.0
            for candidate in exact_identity_candidates:
                score, candidate_match_type = _score_candidate_match(
                    projection,
                    candidate,
                    allow_exact_cross_country_same_event=True,
                )
                if score <= best_score or candidate_match_type is None:
                    continue
                best_candidate = candidate
                best_score = score

            if best_candidate is not None and best_score >= 110.0:
                return best_candidate, "exact_identity_same_event", best_score

        return None, "seed", 0.0

    def _enqueue_refresh_tasks(
        self,
        session: Session,
        *,
        canonical_id,
        now: datetime,
    ) -> int:
        existing_refresh = self.pipeline_task_repository.get_active_for_target(
            session,
            task_type="refresh_analytics",
            target_table="canonical_incidents",
            target_id=canonical_id,
        )
        if existing_refresh is not None:
            return 0
        self.pipeline_task_repository.enqueue(
            session,
            PipelineTask(
                run_id=None,
                task_type="refresh_analytics",
                target_table="canonical_incidents",
                target_id=canonical_id,
                status="queued",
                priority=150,
                payload={"canonical_incident_id": str(canonical_id)},
                result={},
                available_at=now,
                attempt_count=0,
                max_attempts=5,
            ),
        )
        return 1

    def _enqueue_dashboard_refresh_task(
        self,
        session: Session,
        *,
        canonical_id,
        now: datetime,
    ) -> int:
        self.analytics_refresh_repository.mark_needs_refresh(
            session,
            refresh_key="dashboard:global",
            refresh_scope="global",
            default_state_payload={},
        )
        existing_dashboard_refresh = self.pipeline_task_repository.get_active_for_target(
            session,
            task_type="refresh_analytics",
            target_table="analytics_refresh_state",
            target_id=None,
        )
        if existing_dashboard_refresh is not None:
            return 0
        self.pipeline_task_repository.enqueue(
            session,
            PipelineTask(
                run_id=None,
                task_type="refresh_analytics",
                target_table="analytics_refresh_state",
                target_id=None,
                status="queued",
                priority=160,
                payload={
                    "refresh_key": "dashboard:global",
                    "refresh_scope": "global",
                    "canonical_incident_id": str(canonical_id),
                },
                result={},
                available_at=now,
                attempt_count=0,
                max_attempts=5,
            ),
        )
        return 1

    def _retire_empty_canonical(
        self,
        session: Session,
        *,
        canonical: CanonicalIncident,
        now: datetime,
    ) -> int:
        memberships = self.canonical_repository.list_memberships(session, str(canonical.id))
        if memberships:
            self._recalculate_primary_membership(session, canonical)
            session.add(canonical)
            return 0

        canonical.status = "excluded"
        canonical.primary_source_incident_id = None
        canonical.resolution_metadata = {
            **(canonical.resolution_metadata or {}),
            "retired_reason": "membership_moved",
            "retired_at": now.isoformat(),
        }
        session.add(canonical)
        return self._enqueue_refresh_tasks(session, canonical_id=canonical.id, now=now)

    def _recalculate_primary_membership(
        self,
        session: Session,
        canonical: CanonicalIncident,
    ) -> None:
        memberships = self.canonical_repository.list_memberships(session, str(canonical.id))
        if not memberships:
            return
        best = max(memberships, key=lambda membership: float(membership.survivor_score or 0))
        for membership in memberships:
            membership.is_primary_member = membership.id == best.id
            session.add(membership)
        canonical.primary_source_incident_id = best.source_incident_id
        session.add(canonical)

    def _upsert_canonical_enrichment(
        self,
        session: Session,
        canonical: CanonicalIncident,
        source_incident: SourceIncident,
        source_enrichment: SourceEnrichment,
        projection: Dict[str, Any],
    ) -> CanonicalEnrichment:
        existing = session.execute(
            select(CanonicalEnrichment)
            .where(CanonicalEnrichment.canonical_incident_id == canonical.id)
            .limit(1)
        ).scalar_one_or_none()
        if existing is None:
            existing = CanonicalEnrichment(canonical_incident_id=canonical.id)

        member_rows = session.execute(
            select(CanonicalMembership, SourceEnrichment, SourceIncident)
            .join(
                SourceEnrichment,
                SourceEnrichment.source_incident_id == CanonicalMembership.source_incident_id,
            )
            .join(SourceIncident, SourceIncident.id == CanonicalMembership.source_incident_id)
            .where(CanonicalMembership.canonical_incident_id == canonical.id)
        ).all()
        if not isinstance(member_rows, list):
            member_rows = []
        member_documents: List[Dict[str, Any]] = []
        merged_ids = []
        for membership, member_enrichment, source_incident in member_rows:
            if member_enrichment.id is not None:
                merged_ids.append(member_enrichment.id)
            member_projection = build_source_projection(source_incident, member_enrichment)
            member_documents.append(
                {
                    "membership": membership,
                    "source_enrichment": member_enrichment,
                    "source_incident": source_incident,
                    "projection": member_projection,
                    "score_breakdown": _build_member_score_breakdown(
                        source_incident.source_name,
                        member_projection,
                        member_enrichment,
                    ),
                }
            )
        if not member_documents:
            if source_enrichment.id is not None:
                merged_ids.append(source_enrichment.id)
            member_documents.append(
                {
                    "membership": SimpleNamespace(
                        is_primary_member=str(
                            canonical.primary_source_incident_id or source_incident.id
                        )
                        == str(source_incident.id),
                        survivor_score=float(
                            _member_score(
                                source_incident.source_name, projection, source_enrichment
                            )
                        ),
                    ),
                    "source_enrichment": source_enrichment,
                    "source_incident": source_incident,
                    "projection": projection,
                    "score_breakdown": _build_member_score_breakdown(
                        source_incident.source_name,
                        projection,
                        source_enrichment,
                    ),
                }
            )

        merged_projection, projection_field_sources = _build_merged_projection(
            projection,
            member_documents,
            selected_source_enrichment_id=(
                str(source_enrichment.id) if source_enrichment.id else None
            ),
        )
        _apply_projection_to_canonical(canonical, merged_projection, authoritative=True)
        session.add(canonical)

        typed = merged_projection.get("typed_enrichment") or {}
        resolved_field_values = _extract_disclosure_field_values(merged_projection)
        field_provenance = _build_source_disclosure_document(
            canonical,
            member_documents,
            selected_source_enrichment_id=(
                str(source_enrichment.id) if source_enrichment.id else None
            ),
            projection_field_sources=projection_field_sources,
            resolved_field_values=resolved_field_values,
        )
        existing.selected_source_enrichment_id = source_enrichment.id
        existing.merged_from_source_enrichment_ids = merged_ids
        existing.canonical_projection = typed
        existing.analytics_projection = {
            "institution_name": merged_projection.get("institution_name"),
            "institution_type": merged_projection.get("institution_type"),
            "vendor_name": merged_projection.get("vendor_name"),
            "country": merged_projection.get("country"),
            "country_code": merged_projection.get("country_code"),
            "incident_date": (
                merged_projection.get("incident_date").isoformat()
                if merged_projection.get("incident_date")
                else None
            ),
            "attack_category": merged_projection.get("attack_category"),
            "attack_vector": merged_projection.get("attack_vector"),
            "threat_actor_name": merged_projection.get("threat_actor_name"),
            "ransomware_family": merged_projection.get("ransomware_family"),
            "is_education_related": merged_projection.get("is_education_related"),
            "severity": merged_projection.get("severity"),
        }
        existing.field_provenance = field_provenance
        existing.completeness_score = _canonical_completeness_score(typed)
        session.add(existing)

        session.execute(
            select(CanonicalTimelineEvent).where(
                CanonicalTimelineEvent.canonical_incident_id == canonical.id
            )
        ).scalars().all()
        session.query(CanonicalTimelineEvent).filter_by(canonical_incident_id=canonical.id).delete()
        for index, event in enumerate(merged_projection.get("timeline") or [], start=1):
            session.add(
                CanonicalTimelineEvent(
                    canonical_incident_id=canonical.id,
                    seq_order=index,
                    event_date=_parse_date_only(event.get("date")),
                    date_precision=_normalize_canonical_date_precision(event.get("date_precision")),
                    event_type=event.get("event_type") or "other",
                    event_description=event.get("event_description"),
                    actor_attribution=event.get("actor_attribution"),
                    source_enrichment_id=source_enrichment.id,
                    created_at=datetime.now(timezone.utc),
                )
            )

        return existing

    def _resolve_primary_projection(
        self,
        session: Session,
        canonical: CanonicalIncident,
    ) -> Tuple[Optional[SourceEnrichment], Optional[Dict[str, Any]]]:
        if canonical.primary_source_incident_id is None:
            return None, None
        primary_source_incident = self.source_incident_repository.get_by_id(
            session, canonical.primary_source_incident_id
        )
        if primary_source_incident is None:
            return None, None
        primary_source_enrichment = self.source_enrichment_repository.get_by_source_incident(
            session,
            canonical.primary_source_incident_id,
        )
        if primary_source_enrichment is None or not primary_source_enrichment.typed_enrichment:
            return None, None
        primary_projection = build_source_projection(
            primary_source_incident, primary_source_enrichment
        )
        return primary_source_enrichment, primary_projection

    def _refresh_canonical_from_valid_members(
        self,
        session: Session,
        *,
        canonical: CanonicalIncident,
    ) -> Tuple[bool, Optional[SourceEnrichment], Optional[Dict[str, Any]]]:
        memberships = self.canonical_repository.list_memberships(session, str(canonical.id))
        if not memberships:
            canonical.primary_source_incident_id = None
            session.add(canonical)
            return False, None, None

        valid_members: List[Tuple[CanonicalMembership, SourceEnrichment, Dict[str, Any]]] = []
        for membership in memberships:
            enrichment = self.source_enrichment_repository.get_by_source_incident(
                session, membership.source_incident_id
            )
            if (
                enrichment is None
                or enrichment.manual_review_required
                or enrichment.is_education_related is False
                or not enrichment.typed_enrichment
            ):
                continue
            source_incident = self.source_incident_repository.get_by_id(
                session, membership.source_incident_id
            )
            if source_incident is None:
                continue
            projection = build_source_projection(source_incident, enrichment)
            if _resolved_projection_identity(projection) is None:
                continue
            valid_members.append((membership, enrichment, projection))

        if not valid_members:
            for membership in memberships:
                membership.is_primary_member = False
                session.add(membership)
            canonical.primary_source_incident_id = None
            session.add(canonical)
            return False, None, None

        best_membership, best_enrichment, best_projection = max(
            valid_members,
            key=lambda item: float(item[0].survivor_score or 0.0),
        )
        for membership in memberships:
            membership.is_primary_member = membership.id == best_membership.id
            session.add(membership)
        canonical.primary_source_incident_id = best_membership.source_incident_id
        session.add(canonical)
        return True, best_enrichment, best_projection

    def _handle_invalid_source_membership(
        self,
        session: Session,
        *,
        source_incident,
        existing_membership: Optional[CanonicalMembership],
        reason: str,
        metadata_field: str,
    ) -> Dict[str, object]:
        if existing_membership is None:
            return {"canonicalized": False, "reason": reason}

        canonical = self.canonical_repository.get_by_id(
            session, str(existing_membership.canonical_incident_id)
        )
        if canonical is None:
            return {"canonicalized": False, "reason": "dangling_membership"}

        now = datetime.now(timezone.utc)
        existing_membership.survivor_score = -1
        existing_membership.field_contribution = {}
        session.add(existing_membership)

        has_valid_member, primary_source_enrichment, primary_projection = (
            self._refresh_canonical_from_valid_members(
                session,
                canonical=canonical,
            )
        )
        canonical.status = "open" if has_valid_member else "excluded"
        canonical.is_education_related = True if has_valid_member else False
        if (
            has_valid_member
            and primary_source_enrichment is not None
            and primary_projection is not None
        ):
            _apply_projection_to_canonical(canonical, primary_projection, authoritative=True)
            self._upsert_canonical_enrichment(
                session,
                canonical,
                primary_source_enrichment,
                primary_projection,
            )
        canonical.resolution_metadata = {
            **(canonical.resolution_metadata or {}),
            "last_match_type": existing_membership.match_type,
            "last_match_score": float(existing_membership.match_score or 0.0),
            metadata_field: str(source_incident.id),
        }
        session.add(canonical)
        refresh_tasks_enqueued = self._enqueue_refresh_tasks(
            session,
            canonical_id=canonical.id,
            now=now,
        )
        dashboard_refresh_tasks_enqueued = self._enqueue_dashboard_refresh_task(
            session,
            canonical_id=canonical.id,
            now=now,
        )

        return {
            "canonicalized": False,
            "reason": reason,
            "canonical_incident_id": str(canonical.id),
            "canonical_status": canonical.status,
            "refresh_tasks_enqueued": refresh_tasks_enqueued,
            "dashboard_refresh_tasks_enqueued": dashboard_refresh_tasks_enqueued,
        }

    def _handle_non_education_source(
        self,
        session: Session,
        *,
        source_incident,
        source_enrichment: SourceEnrichment,
        existing_membership: Optional[CanonicalMembership],
    ) -> Dict[str, object]:
        return self._handle_invalid_source_membership(
            session,
            source_incident=source_incident,
            existing_membership=existing_membership,
            reason="not_education_related",
            metadata_field="last_non_education_source_incident_id",
        )

    def canonicalize_source_incident(
        self, session: Session, source_incident_id
    ) -> Dict[str, object]:
        source_incident = self.source_incident_repository.get_by_id(session, source_incident_id)
        if source_incident is None:
            return {"canonicalized": False, "reason": "missing_source_incident"}

        source_enrichment = self.source_enrichment_repository.get_by_source_incident(
            session, source_incident.id
        )
        if source_enrichment is None:
            return {"canonicalized": False, "reason": "missing_source_enrichment"}
        existing_membership = self.canonical_repository.get_membership_for_source_incident(
            session,
            str(source_incident.id),
        )
        if source_enrichment.manual_review_required:
            return self._handle_invalid_source_membership(
                session,
                source_incident=source_incident,
                existing_membership=existing_membership,
                reason="manual_review_required",
                metadata_field="last_manual_review_source_incident_id",
            )
        if source_enrichment.is_education_related is False:
            return self._handle_non_education_source(
                session,
                source_incident=source_incident,
                source_enrichment=source_enrichment,
                existing_membership=existing_membership,
            )
        if not source_enrichment.typed_enrichment:
            return {"canonicalized": False, "reason": "missing_typed_enrichment"}

        projection = build_source_projection(source_incident, source_enrichment)
        if _resolved_projection_identity(projection) is None:
            return self._handle_invalid_source_membership(
                session,
                source_incident=source_incident,
                existing_membership=existing_membership,
                reason="missing_identity",
                metadata_field="last_missing_identity_source_incident_id",
            )
        member_score = _member_score(source_incident.source_name, projection, source_enrichment)
        now = datetime.now(timezone.utc)

        old_canonical = None
        if existing_membership is not None:
            old_canonical = self.canonical_repository.get_by_id(
                session, str(existing_membership.canonical_incident_id)
            )
            canonical = old_canonical
            if canonical is None:
                return {"canonicalized": False, "reason": "dangling_membership"}
            replacement_canonical, replacement_match_type, replacement_match_score = (
                self._find_existing_canonical(
                    session,
                    source_incident,
                    projection,
                    exclude_canonical_ids=(canonical.id,),
                )
            )
            if replacement_canonical is not None and str(replacement_canonical.id) != str(
                canonical.id
            ):
                existing_membership.canonical_incident_id = replacement_canonical.id
                existing_membership.match_type = replacement_match_type
                existing_membership.match_score = replacement_match_score
                existing_membership.matched_at = now
                canonical = replacement_canonical
            existing_membership.survivor_score = member_score
            existing_membership.field_contribution = _build_field_provenance(
                projection, source_enrichment.id
            )
            session.add(existing_membership)
            match_type = existing_membership.match_type
            match_score = float(existing_membership.match_score or 0.0)
        else:
            canonical, match_type, match_score = self._find_existing_canonical(
                session, source_incident, projection
            )
            if canonical is None:
                canonical = CanonicalIncident(
                    canonical_key=_canonical_key_for_projection(projection, source_incident.id),
                    status="open",
                    first_seen_at=source_incident.collected_at,
                    last_seen_at=source_incident.collected_at,
                    resolution_version=self.MATCHER_VERSION,
                    resolution_metadata={},
                )
                self.canonical_repository.add(session, canonical)
                session.flush()
                match_type = "seed"
                match_score = 100.0
            membership = CanonicalMembership(
                canonical_incident_id=canonical.id,
                source_incident_id=source_incident.id,
                match_type=match_type,
                match_score=match_score,
                survivor_score=member_score,
                is_primary_member=False,
                field_contribution=_build_field_provenance(projection, source_enrichment.id),
                matcher_version=self.MATCHER_VERSION,
                matched_at=now,
            )
            self.canonical_repository.add_membership(session, membership)

        _apply_projection_to_canonical(canonical, projection)
        canonical.first_seen_at = min(canonical.first_seen_at, source_incident.collected_at)
        canonical.last_seen_at = max(canonical.last_seen_at, source_incident.collected_at)
        canonical.status = "open"
        canonical.is_education_related = True
        canonical.resolution_metadata = {
            **(canonical.resolution_metadata or {}),
            "last_match_type": match_type,
            "last_match_score": float(match_score),
        }
        session.add(canonical)
        session.flush()

        self._recalculate_primary_membership(session, canonical)
        primary_source_enrichment, primary_projection = self._resolve_primary_projection(
            session, canonical
        )
        if primary_source_enrichment is not None and primary_projection is not None:
            _apply_projection_to_canonical(canonical, primary_projection, authoritative=True)
            session.add(canonical)
            source_enrichment = primary_source_enrichment
            projection = primary_projection
        canonical_enrichment = self._upsert_canonical_enrichment(
            session,
            canonical,
            source_incident,
            source_enrichment,
            projection,
        )

        refresh_task_enqueued = self._enqueue_refresh_tasks(
            session,
            canonical_id=canonical.id,
            now=now,
        )
        retired_refresh_tasks_enqueued = 0
        if old_canonical is not None and str(old_canonical.id) != str(canonical.id):
            session.flush()
            retired_refresh_tasks_enqueued = self._retire_empty_canonical(
                session,
                canonical=old_canonical,
                now=now,
            )

        dashboard_refresh_enqueued = self._enqueue_dashboard_refresh_task(
            session,
            canonical_id=canonical.id,
            now=now,
        )

        return {
            "canonicalized": True,
            "canonical_incident_id": str(canonical.id),
            "match_type": match_type,
            "match_score": float(match_score),
            "primary_source_incident_id": (
                str(canonical.primary_source_incident_id)
                if canonical.primary_source_incident_id
                else None
            ),
            "canonical_enrichment_id": (
                str(canonical_enrichment.id) if canonical_enrichment.id else None
            ),
            "refresh_tasks_enqueued": refresh_task_enqueued,
            "retired_refresh_tasks_enqueued": retired_refresh_tasks_enqueued,
            "dashboard_refresh_tasks_enqueued": dashboard_refresh_enqueued,
        }
