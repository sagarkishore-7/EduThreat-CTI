"""Read-side helpers for Postgres-backed canonical incidents."""

from __future__ import annotations

from datetime import date
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

    def get_dashboard_summary(self, session: Session) -> dict[str, Any]:
        snapshot = self.analytics_refresh_repository.get_by_key(session, "dashboard:global")
        if snapshot is not None and snapshot.state_payload:
            return snapshot.state_payload

        rollup = self.canonical_repository.get_dashboard_rollup(session)
        return {
            "totals": rollup,
            "top_countries": self.canonical_repository.get_country_breakdown(session),
            "top_attack_categories": self.canonical_repository.get_attack_breakdown(session),
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
