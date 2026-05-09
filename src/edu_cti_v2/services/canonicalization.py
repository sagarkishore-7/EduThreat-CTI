"""Canonical incident creation and update for the v2 runtime."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
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
from src.edu_cti_v2.models import (
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    CanonicalTimelineEvent,
    PipelineTask,
    SourceEnrichment,
)
from src.edu_cti_v2.repositories import (
    CanonicalIncidentRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
    SourceIncidentRepository,
)

_EDTECH_LIKE_TYPES = {"edtech_platform", "education_vendor", "tutoring_service"}
_GENERIC_INDEFINITE_INSTITUTION_RE = re.compile(
    r"^(?:a|an)\s+"
    r"(?:public\s+|private\s+|state\s+|local\s+|regional\s+|major\s+|leading\s+)?"
    r"(?:university|college|school|academy|institute|polytechnic|library|"
    r"school district|community college|technical college|research institute)\b",
    re.IGNORECASE,
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


def _looks_generic_institution_label(value: Optional[str]) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(_GENERIC_INDEFINITE_INSTITUTION_RE.match(text))


def _resolve_institution_name(source_incident, typed: Dict[str, Any], raw: Dict[str, Any]) -> Optional[str]:
    extracted_candidates = [
        typed.get("institution_name"),
        typed.get("institution_name_en"),
        raw.get("institution_name"),
        raw.get("institution_name_en"),
    ]
    for candidate in extracted_candidates:
        cleaned = clean_institution_name(candidate)
        if cleaned and not _looks_generic_institution_label(cleaned):
            return cleaned

    return choose_best_institution_name(
        *extracted_candidates,
        source_incident.raw_institution_name,
        source_incident.raw_victim_name,
        source_incident.raw_title,
    )


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


def _choose_canonical_institution_name(existing: Optional[str], new: Optional[str]) -> Optional[str]:
    existing_clean = clean_institution_name(existing)
    new_clean = clean_institution_name(new)

    if new_clean and (_looks_generic_institution_label(existing_clean) and not _looks_generic_institution_label(new_clean)):
        return new_clean
    if existing_clean and (_looks_generic_institution_label(new_clean) and not _looks_generic_institution_label(existing_clean)):
        return existing_clean

    return choose_best_institution_name(existing_clean, new_clean)


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
    country, country_code = _normalize_country_fields(source_incident, typed, raw)
    region = _first_present(typed.get("region"), raw.get("region"), source_incident.raw_region)
    city = _first_present(typed.get("city"), raw.get("city"), source_incident.raw_city)
    incident_date = _parse_date_only(
        _first_present(typed.get("incident_date"), raw.get("incident_date"), source_incident.raw_incident_date)
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
    ransomware_family = _first_present(
        attack_dynamics.get("ransomware_family"),
        typed.get("ransomware_family"),
        raw.get("ransomware_family"),
    )
    threat_actor_name = _first_present(
        typed.get("threat_actor_name"),
        raw.get("threat_actor_name"),
        source_incident.raw_threat_actor,
    )
    canonical_summary = _first_present(
        typed.get("enriched_summary"),
        raw.get("enriched_summary"),
        source_incident.raw_title,
    )
    vendor_name = _first_present(typed.get("vendor_name"), raw.get("vendor_name"))
    if not vendor_name and institution_name and institution_type in _EDTECH_LIKE_TYPES:
        vendor_name = institution_name

    return {
        "institution_name": institution_name,
        "institution_type": institution_type,
        "vendor_name": vendor_name,
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


def _member_score(source_name: str, projection: Dict[str, Any], source_enrichment: SourceEnrichment) -> int:
    typed = projection.get("typed_enrichment") or {}
    timeline = projection.get("timeline") or []
    score = 0
    score += _SURVIVOR_SOURCE_RANK.get(source_name, 0)
    score += _count_present_fields(typed)
    score += min(len(str(projection.get("canonical_summary") or "")) // 120, 10)
    score += min(len(timeline), 10) * 3
    if projection.get("institution_name"):
        score += 10
    if projection.get("incident_date"):
        score += 6
    if projection.get("ransomware_family") or projection.get("threat_actor_name"):
        score += 4
    if projection.get("country_code"):
        score += 2
    confidence = source_enrichment.enrichment_confidence
    if confidence is not None:
        score += int(float(confidence) * 10)
    return score


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


class V2CanonicalizationService:
    """Create and update canonical incidents with lineage retained."""

    MATCHER_VERSION = "v2-canonicalizer-1"

    def __init__(
        self,
        *,
        canonical_repository: Optional[CanonicalIncidentRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.canonical_repository = canonical_repository or CanonicalIncidentRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.source_enrichment_repository = source_enrichment_repository or SourceEnrichmentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def _find_existing_canonical(
        self,
        session: Session,
        source_incident,
        projection: Dict[str, Any],
    ) -> Tuple[Optional[CanonicalIncident], str, float]:
        normalized_urls = [
            row.normalized_url
            for row in (source_incident.urls or [])
            if row.url_kind == "article" and not row.is_wrapper and row.normalized_url
        ]
        url_candidates = self.canonical_repository.find_by_url_candidates(session, normalized_urls)
        if url_candidates:
            return url_candidates[0], "url_exact", 100.0

        candidate_name = projection.get("institution_name") or projection.get("vendor_name")
        if not candidate_name:
            return None, "seed", 0.0

        candidates = self.canonical_repository.find_name_date_candidates(
            session,
            incident_date=projection.get("incident_date"),
            country_code=projection.get("country_code"),
        )
        for candidate in candidates:
            existing_name = candidate.institution_name or candidate.vendor_name
            if not existing_name:
                continue
            if not institution_names_match(candidate_name, existing_name, threshold=85):
                continue
            if not dates_within_window(
                parse_incident_date(str(projection.get("incident_date")) if projection.get("incident_date") else None),
                parse_incident_date(str(candidate.incident_date) if candidate.incident_date else None),
                14,
            ):
                continue
            if (
                projection.get("country_code")
                and candidate.country_code
                and projection.get("country_code") != candidate.country_code
            ):
                continue
            return candidate, "name_date", 90.0

        return None, "seed", 0.0

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
        source_enrichment: SourceEnrichment,
        projection: Dict[str, Any],
    ) -> CanonicalEnrichment:
        existing = session.execute(
            select(CanonicalEnrichment).where(CanonicalEnrichment.canonical_incident_id == canonical.id).limit(1)
        ).scalar_one_or_none()
        if existing is None:
            existing = CanonicalEnrichment(canonical_incident_id=canonical.id)

        member_enrichments = session.execute(
            select(SourceEnrichment)
            .join(CanonicalMembership, CanonicalMembership.source_incident_id == SourceEnrichment.source_incident_id)
            .where(CanonicalMembership.canonical_incident_id == canonical.id)
        ).scalars().all()
        merged_ids = [enrichment.id for enrichment in member_enrichments if enrichment.id is not None]

        typed = projection.get("typed_enrichment") or {}
        field_provenance = _build_field_provenance(projection, source_enrichment.id)
        existing.selected_source_enrichment_id = source_enrichment.id
        existing.merged_from_source_enrichment_ids = merged_ids
        existing.canonical_projection = typed
        existing.analytics_projection = {
            "institution_name": projection.get("institution_name"),
            "institution_type": projection.get("institution_type"),
            "vendor_name": projection.get("vendor_name"),
            "country": projection.get("country"),
            "country_code": projection.get("country_code"),
            "incident_date": projection.get("incident_date").isoformat() if projection.get("incident_date") else None,
            "attack_category": projection.get("attack_category"),
            "attack_vector": projection.get("attack_vector"),
            "threat_actor_name": projection.get("threat_actor_name"),
            "ransomware_family": projection.get("ransomware_family"),
            "is_education_related": projection.get("is_education_related"),
            "severity": projection.get("severity"),
        }
        existing.field_provenance = field_provenance
        existing.completeness_score = _count_present_fields(typed)
        session.add(existing)

        session.execute(
            select(CanonicalTimelineEvent).where(CanonicalTimelineEvent.canonical_incident_id == canonical.id)
        ).scalars().all()
        session.query(CanonicalTimelineEvent).filter_by(canonical_incident_id=canonical.id).delete()
        for index, event in enumerate(projection.get("timeline") or [], start=1):
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

    def canonicalize_source_incident(self, session: Session, source_incident_id) -> Dict[str, object]:
        source_incident = self.source_incident_repository.get_by_id(session, source_incident_id)
        if source_incident is None:
            return {"canonicalized": False, "reason": "missing_source_incident"}

        source_enrichment = self.source_enrichment_repository.get_by_source_incident(session, source_incident.id)
        if source_enrichment is None:
            return {"canonicalized": False, "reason": "missing_source_enrichment"}
        if source_enrichment.is_education_related is False:
            return {"canonicalized": False, "reason": "not_education_related"}
        if not source_enrichment.typed_enrichment:
            return {"canonicalized": False, "reason": "missing_typed_enrichment"}

        existing_membership = self.canonical_repository.get_membership_for_source_incident(
            session,
            str(source_incident.id),
        )
        projection = build_source_projection(source_incident, source_enrichment)
        member_score = _member_score(source_incident.source_name, projection, source_enrichment)
        now = datetime.now(timezone.utc)

        if existing_membership is not None:
            canonical = self.canonical_repository.get_by_id(session, str(existing_membership.canonical_incident_id))
            if canonical is None:
                return {"canonicalized": False, "reason": "dangling_membership"}
            existing_membership.survivor_score = member_score
            existing_membership.field_contribution = _build_field_provenance(projection, source_enrichment.id)
            session.add(existing_membership)
            match_type = existing_membership.match_type
            match_score = float(existing_membership.match_score or 0.0)
        else:
            canonical, match_type, match_score = self._find_existing_canonical(session, source_incident, projection)
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

        canonical.institution_name = _choose_canonical_institution_name(
            canonical.institution_name,
            projection.get("institution_name"),
        )
        canonical.institution_type = canonical.institution_type or projection.get("institution_type")
        canonical.vendor_name = canonical.vendor_name or projection.get("vendor_name")
        canonical.country = normalize_country(canonical.country) if canonical.country else projection.get("country")
        canonical.country_code = canonical.country_code or projection.get("country_code") or get_country_code(canonical.country or "")
        canonical.region = canonical.region or projection.get("region")
        canonical.city = canonical.city or projection.get("city")
        canonical.incident_date = canonical.incident_date or projection.get("incident_date")
        canonical.date_precision = canonical.date_precision or projection.get("date_precision")
        canonical.source_published_at = canonical.source_published_at or projection.get("source_published_at")
        canonical.attack_category = canonical.attack_category or projection.get("attack_category")
        canonical.attack_vector = canonical.attack_vector or projection.get("attack_vector")
        canonical.threat_actor_name = canonical.threat_actor_name or projection.get("threat_actor_name")
        canonical.ransomware_family = canonical.ransomware_family or projection.get("ransomware_family")
        canonical.is_education_related = (
            projection.get("is_education_related")
            if canonical.is_education_related is None
            else canonical.is_education_related
        )
        canonical.severity = canonical.severity or projection.get("severity")
        canonical.canonical_summary = canonical.canonical_summary or projection.get("canonical_summary")
        canonical.first_seen_at = min(canonical.first_seen_at, source_incident.collected_at)
        canonical.last_seen_at = max(canonical.last_seen_at, source_incident.collected_at)
        canonical.resolution_metadata = {
            **(canonical.resolution_metadata or {}),
            "last_match_type": match_type,
            "last_match_score": float(match_score),
        }
        session.add(canonical)
        session.flush()

        self._recalculate_primary_membership(session, canonical)
        canonical_enrichment = self._upsert_canonical_enrichment(
            session,
            canonical,
            source_enrichment,
            projection,
        )

        existing_refresh = self.pipeline_task_repository.get_active_for_target(
            session,
            task_type="refresh_analytics",
            target_table="canonical_incidents",
            target_id=canonical.id,
        )
        refresh_task_enqueued = 0
        if existing_refresh is None:
            self.pipeline_task_repository.enqueue(
                session,
                PipelineTask(
                    run_id=None,
                    task_type="refresh_analytics",
                    target_table="canonical_incidents",
                    target_id=canonical.id,
                    status="queued",
                    priority=150,
                    payload={"canonical_incident_id": str(canonical.id)},
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=5,
                ),
            )
            refresh_task_enqueued = 1

        return {
            "canonicalized": True,
            "canonical_incident_id": str(canonical.id),
            "match_type": match_type,
            "match_score": float(match_score),
            "primary_source_incident_id": str(canonical.primary_source_incident_id) if canonical.primary_source_incident_id else None,
            "canonical_enrichment_id": str(canonical_enrichment.id) if canonical_enrichment.id else None,
            "refresh_tasks_enqueued": refresh_task_enqueued,
        }
