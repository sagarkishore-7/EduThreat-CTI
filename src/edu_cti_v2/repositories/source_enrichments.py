"""Repository helpers for v2 source enrichment rows."""

from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import SourceEnrichment


class SourceEnrichmentRepository:
    """Repository boundary for source-level enrichments."""

    @staticmethod
    def build_get_by_source_incident_stmt(source_incident_id) -> Select:
        return (
            select(SourceEnrichment)
            .where(SourceEnrichment.source_incident_id == source_incident_id)
            .limit(1)
        )

    def get_by_source_incident(
        self,
        session: Session,
        source_incident_id,
    ) -> SourceEnrichment | None:
        stmt = self.build_get_by_source_incident_stmt(source_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def add(self, session: Session, enrichment: SourceEnrichment) -> SourceEnrichment:
        session.add(enrichment)
        return enrichment
