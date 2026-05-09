"""Repository helpers for canonical incident tables."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Sequence

from sqlalchemy import Select, and_, or_, select
from sqlalchemy.orm import Session, selectinload

from src.edu_cti_v2.models import CanonicalIncident, CanonicalMembership, SourceIncidentUrl


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

    def add(self, session: Session, canonical_incident: CanonicalIncident) -> CanonicalIncident:
        session.add(canonical_incident)
        return canonical_incident

    def add_membership(self, session: Session, membership: CanonicalMembership) -> CanonicalMembership:
        session.add(membership)
        return membership
