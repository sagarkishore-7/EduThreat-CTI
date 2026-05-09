"""Repository helpers for canonical incident tables."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Sequence

from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.orm import Session, selectinload

from src.edu_cti_v2.models import (
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    CanonicalTimelineEvent,
    SourceIncidentUrl,
)


class CanonicalIncidentRepository:
    """Repository boundary for canonical incident and membership access."""

    @staticmethod
    def build_get_by_id_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalIncident)
            .options(selectinload(CanonicalIncident.memberships))
            .where(CanonicalIncident.id == canonical_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_get_by_source_incident_stmt(source_incident_id: str) -> Select:
        return (
            select(CanonicalMembership)
            .where(CanonicalMembership.source_incident_id == source_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_list_memberships_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalMembership)
            .where(CanonicalMembership.canonical_incident_id == canonical_incident_id)
            .order_by(CanonicalMembership.is_primary_member.desc(), CanonicalMembership.matched_at.asc())
        )

    @staticmethod
    def build_get_enrichment_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalEnrichment)
            .where(CanonicalEnrichment.canonical_incident_id == canonical_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_list_timeline_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalTimelineEvent)
            .where(CanonicalTimelineEvent.canonical_incident_id == canonical_incident_id)
            .order_by(CanonicalTimelineEvent.seq_order.asc(), CanonicalTimelineEvent.created_at.asc())
        )

    @staticmethod
    def build_list_recent_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 50,
    ) -> Select:
        membership_count = (
            select(func.count(CanonicalMembership.id))
            .where(CanonicalMembership.canonical_incident_id == CanonicalIncident.id)
            .correlate(CanonicalIncident)
            .scalar_subquery()
        )
        return (
            select(
                CanonicalIncident,
                CanonicalEnrichment,
                membership_count.label("membership_count"),
            )
            .outerjoin(CanonicalEnrichment, CanonicalEnrichment.canonical_incident_id == CanonicalIncident.id)
            .where(CanonicalIncident.status.in_(list(statuses)))
            .order_by(CanonicalIncident.last_seen_at.desc(), CanonicalIncident.created_at.desc())
            .limit(limit)
        )

    @staticmethod
    def build_dashboard_rollup_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(
                func.count(CanonicalIncident.id).label("canonical_incident_count"),
                func.count(CanonicalEnrichment.id).label("enriched_canonical_count"),
                func.sum(
                    case(
                        (CanonicalIncident.is_education_related.is_(True), 1),
                        else_=0,
                    )
                ).label("education_related_count"),
            )
            .select_from(CanonicalIncident)
            .outerjoin(CanonicalEnrichment, CanonicalEnrichment.canonical_incident_id == CanonicalIncident.id)
            .where(CanonicalIncident.status.in_(list(statuses)))
        )

    @staticmethod
    def build_country_breakdown_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> Select:
        return (
            select(
                CanonicalIncident.country_code,
                CanonicalIncident.country,
                func.count(CanonicalIncident.id).label("incident_count"),
            )
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.country_code.is_not(None))
            .group_by(CanonicalIncident.country_code, CanonicalIncident.country)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.country.asc())
            .limit(limit)
        )

    @staticmethod
    def build_attack_breakdown_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> Select:
        return (
            select(
                CanonicalIncident.attack_category,
                func.count(CanonicalIncident.id).label("incident_count"),
            )
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.attack_category.is_not(None))
            .group_by(CanonicalIncident.attack_category)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.attack_category.asc())
            .limit(limit)
        )

    @staticmethod
    def build_find_by_url_candidates_stmt(normalized_urls: Sequence[str]) -> Select:
        return (
            select(CanonicalIncident)
            .join(CanonicalMembership, CanonicalMembership.canonical_incident_id == CanonicalIncident.id)
            .join(SourceIncidentUrl, SourceIncidentUrl.source_incident_id == CanonicalMembership.source_incident_id)
            .options(selectinload(CanonicalIncident.memberships))
            .where(SourceIncidentUrl.normalized_url.in_(list(normalized_urls)))
            .distinct()
        )

    @staticmethod
    def build_find_name_date_candidates_stmt(
        *,
        incident_date: Optional[date],
        country_code: Optional[str],
        window_days: int = 14,
    ) -> Select:
        stmt = select(CanonicalIncident).options(selectinload(CanonicalIncident.memberships))
        if country_code:
            stmt = stmt.where(
                or_(
                    CanonicalIncident.country_code == country_code,
                    CanonicalIncident.country_code.is_(None),
                )
            )
        if incident_date:
            start = incident_date - timedelta(days=window_days)
            end = incident_date + timedelta(days=window_days)
            stmt = stmt.where(
                or_(
                    CanonicalIncident.incident_date.between(start, end),
                    CanonicalIncident.incident_date.is_(None),
                )
            )
        return stmt

    def get_by_id(self, session: Session, canonical_incident_id: str) -> CanonicalIncident | None:
        return session.execute(self.build_get_by_id_stmt(canonical_incident_id)).scalar_one_or_none()

    def get_membership_for_source_incident(
        self,
        session: Session,
        source_incident_id: str,
    ) -> CanonicalMembership | None:
        stmt = self.build_get_by_source_incident_stmt(source_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def list_memberships(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> list[CanonicalMembership]:
        stmt = self.build_list_memberships_stmt(canonical_incident_id)
        return list(session.execute(stmt).scalars().all())

    def find_by_url_candidates(
        self,
        session: Session,
        normalized_urls: Sequence[str],
    ) -> list[CanonicalIncident]:
        if not normalized_urls:
            return []
        stmt = self.build_find_by_url_candidates_stmt(normalized_urls)
        return list(session.execute(stmt).scalars().all())

    def find_name_date_candidates(
        self,
        session: Session,
        *,
        incident_date: Optional[date],
        country_code: Optional[str],
        window_days: int = 14,
    ) -> list[CanonicalIncident]:
        stmt = self.build_find_name_date_candidates_stmt(
            incident_date=incident_date,
            country_code=country_code,
            window_days=window_days,
        )
        return list(session.execute(stmt).scalars().all())

    def get_enrichment(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> CanonicalEnrichment | None:
        stmt = self.build_get_enrichment_stmt(canonical_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def list_timeline_events(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> list[CanonicalTimelineEvent]:
        stmt = self.build_list_timeline_stmt(canonical_incident_id)
        return list(session.execute(stmt).scalars().all())

    def list_recent_with_enrichment(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 50,
    ):
        stmt = self.build_list_recent_stmt(statuses=statuses, limit=limit)
        return list(session.execute(stmt).all())

    def get_dashboard_rollup(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, int]:
        stmt = self.build_dashboard_rollup_stmt(statuses=statuses)
        row = session.execute(stmt).one()
        return {
            "canonical_incident_count": int(row.canonical_incident_count or 0),
            "enriched_canonical_count": int(row.enriched_canonical_count or 0),
            "education_related_count": int(row.education_related_count or 0),
        }

    def get_country_breakdown(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> list[dict[str, object]]:
        stmt = self.build_country_breakdown_stmt(statuses=statuses, limit=limit)
        return [
            {
                "country_code": row.country_code,
                "country": row.country,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_attack_breakdown(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> list[dict[str, object]]:
        stmt = self.build_attack_breakdown_stmt(statuses=statuses, limit=limit)
        return [
            {
                "attack_category": row.attack_category,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def add(self, session: Session, canonical_incident: CanonicalIncident) -> CanonicalIncident:
        session.add(canonical_incident)
        return canonical_incident

    def add_membership(self, session: Session, membership: CanonicalMembership) -> CanonicalMembership:
        session.add(membership)
        return membership
