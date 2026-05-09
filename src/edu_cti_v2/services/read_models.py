"""Read-side helpers for Postgres-backed canonical incidents."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional, Sequence

from sqlalchemy.orm import Session

from src.edu_cti_v2.models import (
    ArticleFetchAttempt,
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    CanonicalTimelineEvent,
)
from src.edu_cti_v2.repositories import AnalyticsRefreshRepository, ArticleRepository, CanonicalIncidentRepository


def _serialize_membership(
    membership: CanonicalMembership,
    *,
    source_details: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload = {
        "source_incident_id": str(membership.source_incident_id),
        "match_type": membership.match_type,
        "match_score": float(membership.match_score or 0.0),
        "survivor_score": float(membership.survivor_score or 0.0),
        "is_primary_member": bool(membership.is_primary_member),
        "field_contribution": membership.field_contribution or {},
        "matcher_version": membership.matcher_version,
        "matched_at": membership.matched_at.isoformat() if membership.matched_at else None,
    }
    if source_details:
        payload.update(
            {
                "source_name": source_details.get("source_name"),
                "source_group": source_details.get("source_group"),
                "collected_at": source_details.get("collected_at"),
                "source_published_at": source_details.get("source_published_at"),
                "raw_title": source_details.get("raw_title"),
                "raw_subtitle": source_details.get("raw_subtitle"),
                "raw_victim_name": source_details.get("raw_victim_name"),
                "raw_institution_name": source_details.get("raw_institution_name"),
                "raw_institution_type": source_details.get("raw_institution_type"),
                "raw_country": source_details.get("raw_country"),
                "raw_region": source_details.get("raw_region"),
                "raw_city": source_details.get("raw_city"),
            }
        )
    return payload


def _serialize_timeline_event(event: CanonicalTimelineEvent) -> dict[str, Any]:
    return {
        "seq_order": event.seq_order,
        "event_date": event.event_date.isoformat() if event.event_date else None,
        "date_precision": event.date_precision,
        "event_type": event.event_type,
        "event_description": event.event_description,
        "actor_attribution": event.actor_attribution,
        "source_enrichment_id": str(event.source_enrichment_id) if event.source_enrichment_id else None,
    }


def _serialize_fetch_attempt(attempt: ArticleFetchAttempt) -> dict[str, Any]:
    return {
        "fetch_tier": attempt.fetch_tier,
        "attempted_at": attempt.attempted_at.isoformat() if attempt.attempted_at else None,
        "worker_id": attempt.worker_id,
        "success": bool(attempt.success),
        "http_status": attempt.http_status,
        "latency_ms": attempt.latency_ms,
        "content_length": attempt.content_length,
        "error_code": attempt.error_code,
        "error_message": attempt.error_message,
        "response_metadata": attempt.response_metadata or {},
    }


def _summary_from_canonical(
    canonical: CanonicalIncident,
    enrichment: Optional[CanonicalEnrichment],
    *,
    membership_count: int,
) -> dict[str, Any]:
    analytics_projection = (enrichment.analytics_projection if enrichment else None) or {}
    display_name = canonical.institution_name or canonical.vendor_name
    return {
        "canonical_incident_id": str(canonical.id),
        "display_name": display_name,
        "institution_name": canonical.institution_name,
        "vendor_name": canonical.vendor_name,
        "institution_type": canonical.institution_type,
        "country": canonical.country,
        "country_code": canonical.country_code,
        "region": canonical.region,
        "city": canonical.city,
        "incident_date": canonical.incident_date.isoformat() if canonical.incident_date else None,
        "date_precision": canonical.date_precision,
        "attack_category": canonical.attack_category,
        "attack_vector": canonical.attack_vector,
        "threat_actor_name": canonical.threat_actor_name,
        "ransomware_family": canonical.ransomware_family,
        "is_education_related": canonical.is_education_related,
        "severity": canonical.severity,
        "canonical_summary": canonical.canonical_summary,
        "status": canonical.status,
        "membership_count": membership_count,
        "selected_source_enrichment_id": (
            str(enrichment.selected_source_enrichment_id)
            if enrichment and enrichment.selected_source_enrichment_id
            else None
        ),
        "analytics_projection": analytics_projection,
        "first_seen_at": canonical.first_seen_at.isoformat() if canonical.first_seen_at else None,
        "last_seen_at": canonical.last_seen_at.isoformat() if canonical.last_seen_at else None,
        "updated_at": canonical.updated_at.isoformat() if canonical.updated_at else None,
    }


def _to_count_by_category(
    items: list[dict[str, Any]],
    *,
    label_key: str,
    count_key: str = "incident_count",
    country_code_key: Optional[str] = None,
) -> list[dict[str, Any]]:
    total = sum(int(item.get(count_key) or 0) for item in items) or 0
    results: list[dict[str, Any]] = []
    for item in items:
        count = int(item.get(count_key) or 0)
        label = item.get(label_key)
        if label is None:
            continue
        payload = {
            "category": label,
            "count": count,
            "percentage": (count / total * 100.0) if total else 0.0,
        }
        if country_code_key:
            payload["country_code"] = item.get(country_code_key)
        results.append(payload)
    return results


def _to_time_series(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "date": str(item.get("bucket_start")),
            "count": int(item.get("incident_count") or 0),
        }
        for item in items
        if item.get("bucket_start") is not None
    ]


def _to_recent_incidents(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "incident_id": item["canonical_incident_id"],
            "institution_name": item.get("display_name") or item.get("institution_name") or "Unknown",
            "country": item.get("country"),
            "attack_category": item.get("attack_category"),
            "ransomware_family": item.get("ransomware_family"),
            "incident_date": item.get("incident_date"),
            "title": item.get("canonical_summary"),
            "enriched_summary": item.get("canonical_summary"),
            "threat_actor_name": item.get("threat_actor_name"),
        }
        for item in items
    ]


def _dashboard_stats_from_rollup(rollup: dict[str, Any], *, refreshed_at: str) -> dict[str, Any]:
    total_incidents = int(rollup.get("canonical_incident_count") or 0)
    enriched_incidents = int(rollup.get("enriched_canonical_count") or 0)
    return {
        "total_incidents": total_incidents,
        "education_incidents": int(rollup.get("education_related_count") or 0),
        "enriched_incidents": enriched_incidents,
        "unenriched_incidents": max(total_incidents - enriched_incidents, 0),
        "incidents_with_ransomware": int(rollup.get("incidents_with_ransomware") or 0),
        "incidents_with_data_breach": int(rollup.get("incidents_with_data_breach") or 0),
        "countries_affected": int(rollup.get("countries_affected") or 0),
        "unique_threat_actors": int(rollup.get("unique_threat_actors") or 0),
        "unique_ransomware_families": int(rollup.get("unique_ransomware_families") or 0),
        "data_sources": 0,
        "avg_recovery_days": None,
        "total_financial_impact": 0,
        "incidents_with_mitre": 0,
        "last_updated": refreshed_at,
    }


def _is_full_dashboard_snapshot(payload: dict[str, Any]) -> bool:
    required = {
        "totals",
        "stats",
        "incidents_by_country",
        "incidents_by_attack_type",
        "incidents_by_ransomware",
        "incidents_over_time",
        "recent_incidents",
    }
    return required.issubset(payload.keys())


def _to_legacy_incident_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "incident_id": item["canonical_incident_id"],
        "institution_name": item.get("display_name") or item.get("institution_name") or "Unknown",
        "institution_type": item.get("institution_type"),
        "country": item.get("country"),
        "country_code": item.get("country_code"),
        "region": item.get("region"),
        "city": item.get("city"),
        "incident_date": item.get("incident_date"),
        "date_precision": item.get("date_precision"),
        "title": item.get("canonical_summary"),
        "subtitle": None,
        "enriched_summary": item.get("canonical_summary"),
        "attack_type_hint": item.get("attack_category"),
        "attack_category": item.get("attack_category"),
        "ransomware_family": item.get("ransomware_family"),
        "threat_actor_name": item.get("threat_actor_name"),
        "status": item.get("status") or "open",
        "source_confidence": "medium",
        "llm_enriched": bool(item.get("selected_source_enrichment_id")),
        "llm_enriched_at": item.get("updated_at"),
        "ingested_at": item.get("first_seen_at"),
        "sources": [],
    }


def _to_legacy_timeline(timeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "date": item.get("event_date"),
            "date_precision": item.get("date_precision"),
            "event_description": item.get("event_description"),
            "event_type": item.get("event_type"),
            "actor_attribution": item.get("actor_attribution"),
            "indicators": None,
        }
        for item in timeline
    ]


def _to_legacy_sources(memberships: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "source": item.get("source_name"),
            "source_event_id": item.get("source_incident_id"),
            "first_seen_at": item.get("collected_at"),
            "confidence": None,
        }
        for item in memberships
        if item.get("source_name")
    ]


def _collect_urls(selected_source: dict[str, Any] | None) -> list[str]:
    urls: list[str] = []
    if not selected_source:
        return urls
    for candidate in (
        selected_source.get("article_resolved_url"),
        selected_source.get("article_url"),
    ):
        if candidate and candidate not in urls:
            urls.append(candidate)
    return urls


class V2CanonicalReadService:
    """Build API-friendly read models from canonical incident tables."""

    def __init__(
        self,
        *,
        canonical_repository: Optional[CanonicalIncidentRepository] = None,
        analytics_refresh_repository: Optional[AnalyticsRefreshRepository] = None,
        article_repository: Optional[ArticleRepository] = None,
    ) -> None:
        self.canonical_repository = canonical_repository or CanonicalIncidentRepository()
        self.analytics_refresh_repository = analytics_refresh_repository or AnalyticsRefreshRepository()
        self.article_repository = article_repository or ArticleRepository()

    def list_recent_incidents(
        self,
        session: Session,
        *,
        limit: int = 50,
        statuses: Sequence[str] = ("open",),
    ) -> list[dict[str, Any]]:
        return self.list_incidents(
            session,
            limit=limit,
            statuses=statuses,
        )["items"]

    def list_incidents(
        self,
        session: Session,
        *,
        limit: int = 50,
        offset: int = 0,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        sort_by: str = "last_seen_at",
        sort_order: str = "desc",
    ) -> dict[str, Any]:
        rows = self.canonical_repository.list_recent_with_enrichment(
            session,
            statuses=statuses,
            limit=limit,
            offset=offset,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        total = self.canonical_repository.count_recent(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        items: list[dict[str, Any]] = []
        for canonical, enrichment, membership_count in rows:
            items.append(
                _summary_from_canonical(
                    canonical,
                    enrichment,
                    membership_count=int(membership_count or 0),
                )
            )
        return {
            "items": items,
            "total": total,
        }

    def list_legacy_incidents(
        self,
        session: Session,
        *,
        limit: int = 20,
        offset: int = 0,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        sort_by: str = "incident_date",
        sort_order: str = "desc",
    ) -> dict[str, Any]:
        result = self.list_incidents(
            session,
            limit=limit,
            offset=offset,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        total = int(result["total"])
        per_page = max(limit, 1)
        page = (offset // per_page) + 1
        total_pages = (total + per_page - 1) // per_page if total else 0
        incidents = [_to_legacy_incident_summary(item) for item in result["items"]]
        return {
            "incidents": incidents,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages,
                "has_next": offset + per_page < total,
                "has_prev": offset > 0,
            },
        }

    def get_incident_detail(self, session: Session, canonical_incident_id: str) -> dict[str, Any] | None:
        canonical = self.canonical_repository.get_by_id(session, canonical_incident_id)
        if canonical is None:
            return None
        enrichment = self.canonical_repository.get_enrichment(session, canonical_incident_id)
        membership_details = self.canonical_repository.list_membership_details(session, canonical_incident_id)
        timeline = self.canonical_repository.list_timeline_events(session, canonical_incident_id)
        selected_source = self.canonical_repository.get_selected_source_details(session, canonical_incident_id)
        fetch_attempts = []
        if selected_source and selected_source.get("source_incident_id"):
            fetch_attempts = [
                _serialize_fetch_attempt(attempt)
                for attempt in self.article_repository.list_fetch_attempts(
                    session,
                    selected_source["source_incident_id"],
                    limit=10,
                )
            ]
        snapshot = self.analytics_refresh_repository.get_by_key(
            session,
            f"canonical:{canonical_incident_id}",
        )
        return {
            **_summary_from_canonical(
                canonical,
                enrichment,
                membership_count=len(membership_details),
            ),
            "resolution_metadata": canonical.resolution_metadata or {},
            "field_provenance": (enrichment.field_provenance if enrichment else None) or {},
            "canonical_projection": (enrichment.canonical_projection if enrichment else None) or {},
            "selected_source": selected_source,
            "fetch_attempts": fetch_attempts,
            "memberships": [
                _serialize_membership(detail["membership"], source_details=detail)
                for detail in membership_details
            ],
            "timeline": [_serialize_timeline_event(event) for event in timeline],
            "snapshot": (snapshot.state_payload if snapshot else None) or {},
        }

    def get_legacy_incident_detail(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> dict[str, Any] | None:
        detail = self.get_incident_detail(session, canonical_incident_id)
        if detail is None:
            return None

        projection = detail.get("canonical_projection") or {}
        attack_dynamics = projection.get("attack_dynamics") or {}
        selected_source = detail.get("selected_source") or {}
        timeline = _to_legacy_timeline(detail.get("timeline") or [])
        urls = _collect_urls(selected_source)
        attack_vector = detail.get("attack_vector") or attack_dynamics.get("attack_vector") or projection.get("attack_vector")
        ransomware_family = detail.get("ransomware_family") or attack_dynamics.get("ransomware_family") or projection.get("ransomware_family")

        return {
            "incident_id": detail["canonical_incident_id"],
            "institution_name": detail.get("display_name") or detail.get("institution_name") or "Unknown",
            "institution_type": detail.get("institution_type"),
            "institution_size": projection.get("institution_size"),
            "country": detail.get("country"),
            "country_code": detail.get("country_code"),
            "region": detail.get("region"),
            "city": detail.get("city"),
            "incident_date": detail.get("incident_date"),
            "date_precision": detail.get("date_precision"),
            "discovery_date": projection.get("discovery_date"),
            "source_published_date": selected_source.get("article_publish_date"),
            "ingested_at": detail.get("first_seen_at"),
            "title": selected_source.get("article_title") or detail.get("canonical_summary"),
            "subtitle": selected_source.get("raw_subtitle"),
            "enriched_summary": detail.get("canonical_summary"),
            "initial_access_description": projection.get("initial_access_description"),
            "primary_url": urls[0] if urls else None,
            "all_urls": urls,
            "leak_site_url": projection.get("threat_actor_claim_url") or projection.get("leak_site_url"),
            "source_detail_url": projection.get("source_detail_url"),
            "screenshot_url": projection.get("screenshot_url"),
            "attack_type_hint": detail.get("attack_category"),
            "attack_category": detail.get("attack_category"),
            "incident_severity": detail.get("severity"),
            "status": detail.get("status") or "open",
            "source_confidence": "medium",
            "academic_period_affected": projection.get("academic_period_affected"),
            "dark_web_posting_confirmed": projection.get("dark_web_posting_confirmed"),
            "prior_breach_same_institution": projection.get("prior_breach_same_institution"),
            "threat_actor": projection.get("threat_actor") or detail.get("threat_actor_name"),
            "threat_actor_name": detail.get("threat_actor_name"),
            "threat_actor_category": projection.get("threat_actor_category"),
            "threat_actor_motivation": projection.get("threat_actor_motivation"),
            "threat_actor_origin_country": projection.get("threat_actor_origin_country"),
            "threat_actor_claim_url": projection.get("threat_actor_claim_url"),
            "timeline": timeline,
            "mitre_attack_techniques": projection.get("mitre_attack_techniques"),
            "attack_dynamics": {
                "attack_vector": attack_vector,
                "attack_chain": attack_dynamics.get("attack_chain"),
                "ransomware_family": ransomware_family,
                "data_exfiltration": projection.get("data_exfiltrated"),
                "encryption_impact": attack_dynamics.get("encryption_impact"),
                "ransom_demanded": projection.get("was_ransom_demanded") or attack_dynamics.get("ransom_demanded"),
                "ransom_amount": projection.get("ransom_amount") or attack_dynamics.get("ransom_amount"),
                "ransom_paid": projection.get("ransom_paid") or attack_dynamics.get("ransom_paid"),
                "recovery_timeframe_days": projection.get("recovery_duration_days") or attack_dynamics.get("recovery_timeframe_days"),
                "business_impact": projection.get("business_impact") or attack_dynamics.get("business_impact"),
                "operational_impact": projection.get("operational_impact") or attack_dynamics.get("operational_impact"),
            },
            "data_impact": {
                "data_breached": projection.get("data_breached"),
                "data_exfiltrated": projection.get("data_exfiltrated"),
                "data_categories": projection.get("data_categories"),
                "records_affected_exact": projection.get("records_affected_exact"),
                "records_affected_min": projection.get("records_affected_min"),
                "records_affected_max": projection.get("records_affected_max"),
                "pii_records_leaked": projection.get("pii_records_leaked"),
            },
            "system_impact": {
                "systems_affected": projection.get("systems_affected"),
                "critical_systems_affected": projection.get("critical_systems_affected"),
                "network_compromised": projection.get("network_compromised"),
                "email_system_affected": projection.get("email_system_affected"),
                "student_portal_affected": projection.get("student_portal_affected"),
                "research_systems_affected": projection.get("research_systems_affected"),
                "hospital_systems_affected": projection.get("hospital_systems_affected"),
                "cloud_services_affected": projection.get("cloud_services_affected"),
                "third_party_vendor_impact": projection.get("third_party_vendor_impact"),
                "vendor_name": detail.get("vendor_name"),
            },
            "user_impact": {
                "students_affected": projection.get("students_affected"),
                "staff_affected": projection.get("staff_affected"),
                "faculty_affected": projection.get("faculty_affected"),
                "alumni_affected": projection.get("alumni_affected"),
                "parents_affected": projection.get("parents_affected"),
                "applicants_affected": projection.get("applicants_affected"),
                "patients_affected": projection.get("patients_affected"),
                "users_affected_min": projection.get("users_affected_min"),
                "users_affected_max": projection.get("users_affected_max"),
                "users_affected_exact": projection.get("users_affected_exact"),
                "total_individuals_affected": projection.get("total_individuals_affected"),
            },
            "financial_impact": {
                "estimated_total_cost_usd": projection.get("estimated_total_cost_usd"),
                "ransom_cost_usd": projection.get("ransom_cost_usd"),
                "recovery_cost_usd": projection.get("recovery_cost_usd"),
                "legal_cost_usd": projection.get("legal_cost_usd"),
                "notification_cost_usd": projection.get("notification_cost_usd"),
                "insurance_claim": projection.get("insurance_claim"),
                "insurance_payout_usd": projection.get("insurance_payout_usd"),
                "business_impact": projection.get("business_impact"),
            },
            "regulatory_impact": {
                "applicable_regulations": projection.get("applicable_regulations"),
                "gdpr_breach": projection.get("gdpr_breach"),
                "hipaa_breach": projection.get("hipaa_breach"),
                "ferpa_breach": projection.get("ferpa_breach"),
                "breach_notification_required": projection.get("breach_notification_required"),
                "notification_sent": projection.get("notification_sent"),
                "notification_sent_date": projection.get("notification_sent_date"),
                "notification_delay_days": projection.get("notification_delay_days"),
                "dpa_notified": projection.get("dpa_notified"),
                "investigation_opened": projection.get("investigation_opened"),
                "fine_imposed": projection.get("fine_imposed"),
                "fine_amount_usd": projection.get("fine_amount_usd"),
                "lawsuits_filed": projection.get("lawsuits_filed"),
                "class_action_filed": projection.get("class_action_filed"),
            },
            "research_impact": {
                "research_projects_affected": projection.get("research_projects_affected"),
                "research_data_compromised": projection.get("research_data_compromised"),
                "publications_delayed": projection.get("publications_delayed"),
                "grants_affected": projection.get("grants_affected"),
                "research_area": projection.get("research_area"),
            },
            "recovery_metrics": {
                "recovery_method": projection.get("recovery_method"),
                "recovery_duration_days": projection.get("recovery_duration_days"),
                "from_backup": projection.get("from_backup"),
                "backup_status": projection.get("backup_status"),
                "backup_age_days": projection.get("backup_age_days"),
                "mfa_implemented": projection.get("mfa_implemented"),
                "law_enforcement_involved": projection.get("law_enforcement_involved"),
                "law_enforcement_agency": projection.get("law_enforcement_agency"),
                "ir_firm_engaged": projection.get("ir_firm_engaged"),
                "forensics_firm": projection.get("forensics_firm"),
                "security_improvements": projection.get("security_improvements"),
            },
            "transparency_metrics": {
                "public_disclosure": projection.get("public_disclosure"),
                "public_disclosure_date": projection.get("public_disclosure_date"),
                "disclosure_delay_days": projection.get("disclosure_delay_days"),
                "transparency_level": projection.get("transparency_level"),
            },
            "llm_enriched": bool(detail.get("selected_source_enrichment_id")),
            "llm_enriched_at": detail.get("updated_at"),
            "sources": _to_legacy_sources(detail.get("memberships") or []),
            "notes": projection.get("notes"),
            "data_breached": projection.get("data_breached"),
            "data_exfiltrated": projection.get("data_exfiltrated"),
            "records_affected_exact": projection.get("records_affected_exact"),
            "records_affected_min": projection.get("records_affected_min"),
            "records_affected_max": projection.get("records_affected_max"),
            "pii_records_leaked": projection.get("pii_records_leaked"),
            "systems_affected": projection.get("systems_affected"),
            "teaching_impacted": projection.get("teaching_impacted"),
            "research_impacted": projection.get("research_impacted"),
            "classes_cancelled": projection.get("classes_cancelled"),
            "exams_postponed": projection.get("exams_postponed"),
            "downtime_days": projection.get("downtime_days"),
            "recovery_costs_min": projection.get("recovery_costs_min"),
            "recovery_costs_max": projection.get("recovery_costs_max"),
            "ransom_amount": projection.get("ransom_amount"),
            "ransom_currency": projection.get("ransom_currency"),
            "ransom_paid": projection.get("ransom_paid"),
            "ransom_paid_amount": projection.get("ransom_paid_amount"),
            "fine_amount": projection.get("fine_amount"),
            "attack_vector": attack_vector,
            "access_vector": projection.get("access_vector") or attack_vector,
            "ransomware_family": ransomware_family,
        }

    def get_dashboard_summary(self, session: Session) -> dict[str, Any]:
        snapshot = self.analytics_refresh_repository.get_by_key(session, "dashboard:global")
        if snapshot is not None and snapshot.state_payload and _is_full_dashboard_snapshot(snapshot.state_payload):
            return snapshot.state_payload

        return self.build_dashboard_payload(session)

    def get_dashboard_stats(self, session: Session) -> dict[str, Any]:
        return self.get_dashboard_summary(session)["stats"]

    def build_dashboard_payload(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        refreshed_at: Optional[str] = None,
    ) -> dict[str, Any]:
        effective_refreshed_at = refreshed_at or datetime.now(timezone.utc).isoformat()
        rollup = self.canonical_repository.get_dashboard_rollup(session, statuses=statuses)
        countries = self.canonical_repository.get_country_breakdown(session, statuses=statuses)
        attacks = self.canonical_repository.get_attack_breakdown(session, statuses=statuses)
        ransomware = self.canonical_repository.get_ransomware_breakdown(session, statuses=statuses)
        trend = self.canonical_repository.get_incident_trend(
            session,
            statuses=statuses,
            bucket="month",
            limit=24,
        )
        recent = self.list_incidents(
            session,
            limit=10,
            offset=0,
            statuses=statuses,
            sort_by="incident_date",
            sort_order="desc",
        )["items"]

        return {
            "totals": rollup,
            "stats": _dashboard_stats_from_rollup(rollup, refreshed_at=effective_refreshed_at),
            "incidents_by_country": _to_count_by_category(
                countries,
                label_key="country",
                country_code_key="country_code",
            ),
            "incidents_by_attack_type": _to_count_by_category(
                attacks,
                label_key="attack_category",
            ),
            "incidents_by_ransomware": _to_count_by_category(
                ransomware,
                label_key="ransomware_family",
            ),
            "incidents_over_time": _to_time_series(trend),
            "recent_incidents": _to_recent_incidents(recent),
            "top_countries": countries,
            "top_attack_categories": attacks,
            "top_ransomware_families": ransomware,
            "refreshed_at": effective_refreshed_at,
        }

    def get_incident_facets(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        facet_limit: int = 20,
    ) -> dict[str, Any]:
        return {
            "countries": self.canonical_repository.get_country_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
            "attack_categories": self.canonical_repository.get_attack_category_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
            "institution_types": self.canonical_repository.get_institution_type_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
            "severities": self.canonical_repository.get_severity_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
        }

    def get_analytics_breakdowns(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        breakdown_limit: int = 20,
    ) -> dict[str, Any]:
        return self.get_incident_facets(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            facet_limit=breakdown_limit,
        )

    def get_country_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 20,
    ) -> dict[str, Any]:
        data = _to_count_by_category(
            self.canonical_repository.get_country_breakdown(session, statuses=statuses, limit=limit),
            label_key="country",
            country_code_key="country_code",
        )
        return {"data": data, "total": sum(item["count"] for item in data)}

    def get_attack_type_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 15,
    ) -> dict[str, Any]:
        data = _to_count_by_category(
            self.canonical_repository.get_attack_breakdown(session, statuses=statuses, limit=limit),
            label_key="attack_category",
        )
        return {"data": data, "total": sum(item["count"] for item in data)}

    def get_ransomware_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 15,
    ) -> dict[str, Any]:
        data = _to_count_by_category(
            self.canonical_repository.get_ransomware_breakdown(session, statuses=statuses, limit=limit),
            label_key="ransomware_family",
        )
        return {"data": data, "total": sum(item["count"] for item in data)}

    def get_incident_trend(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        bucket: str = "month",
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        return self.canonical_repository.get_incident_trend(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            bucket=bucket,
            limit=limit,
        )

    def get_timeline_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        months: int = 24,
    ) -> dict[str, Any]:
        items = _to_time_series(
            self.canonical_repository.get_incident_trend(
                session,
                statuses=statuses,
                bucket="month",
                limit=months,
            )
        )
        return {"data": items, "total": sum(item["count"] for item in items)}

    def get_threat_actor_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 20,
    ) -> dict[str, Any]:
        return self.canonical_repository.get_threat_actor_breakdown(
            session,
            statuses=statuses,
            limit=limit,
        )

    def get_filter_options(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, Any]:
        return self.canonical_repository.get_filter_options(
            session,
            statuses=statuses,
        )
