"""Repository helpers for canonical incident tables."""

from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import CanonicalIncident, CanonicalMembership


class CanonicalIncidentRepository:
    """Repository boundary for canonical incident and membership access."""

    @staticmethod
    def build_get_by_id_stmt(canonical_incident_id: str) -> Select:
        return select(CanonicalIncident).where(CanonicalIncident.id == canonical_incident_id).limit(1)

    @staticmethod
    def build_get_by_source_incident_stmt(source_incident_id: str) -> Select:
        return (
            select(CanonicalMembership)
            .where(CanonicalMembership.source_incident_id == source_incident_id)
            .limit(1)
        )

    def get_by_id(self, session: Session, canonical_incident_id: str) -> CanonicalIncident | None:
        return session.execute(self.build_get_by_id_stmt(canonical_incident_id)).scalar_one_or_none()

    def get_membership_for_source_incident(
        self,
        session: Session,
        source_incident_id: str,
    ) -> CanonicalMembership | None:
        stmt = self.build_get_by_source_incident_stmt(source_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def add(self, session: Session, canonical_incident: CanonicalIncident) -> CanonicalIncident:
        session.add(canonical_incident)
        return canonical_incident

    def add_membership(self, session: Session, membership: CanonicalMembership) -> CanonicalMembership:
        session.add(membership)
        return membership
