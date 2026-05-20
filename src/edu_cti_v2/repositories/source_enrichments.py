"""Repository helpers for v2 source enrichment rows."""

from __future__ import annotations

from sqlalchemy import Select, or_, select
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

    @staticmethod
    def build_quality_sweep_stmt(*, limit: int | None = None) -> Select:
        stmt = (
            select(SourceEnrichment)
            .where(or_(SourceEnrichment.is_education_related.is_(True), SourceEnrichment.is_education_related.is_(None)))
            .where(SourceEnrichment.manual_review_required.is_(False))
            .order_by(SourceEnrichment.updated_at.asc(), SourceEnrichment.created_at.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return stmt

    @staticmethod
    def build_recanonicalize_candidates_stmt(*, limit: int | None = None) -> Select:
        stmt = (
            select(SourceEnrichment.source_incident_id)
            .where(SourceEnrichment.typed_enrichment.is_not(None))
            .where(or_(SourceEnrichment.is_education_related.is_(True), SourceEnrichment.is_education_related.is_(None)))
            .order_by(SourceEnrichment.updated_at.asc(), SourceEnrichment.created_at.asc())
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return stmt

    @staticmethod
    def build_manual_review_queue_stmt(*, limit: int = 100) -> Select:
        return (
            select(SourceEnrichment)
            .where(SourceEnrichment.manual_review_required.is_(True))
            .order_by(SourceEnrichment.updated_at.desc(), SourceEnrichment.created_at.desc())
            .limit(limit)
        )

    @staticmethod
    def build_rejected_enrichments_stmt(*, limit: int = 100) -> Select:
        return (
            select(SourceEnrichment)
            .where(SourceEnrichment.is_education_related.is_(False))
            .where(SourceEnrichment.manual_review_required.is_(False))
            .order_by(SourceEnrichment.updated_at.desc(), SourceEnrichment.created_at.desc())
            .limit(limit)
        )

    def list_for_quality_sweep(
        self,
        session: Session,
        *,
        limit: int | None = None,
    ) -> list[SourceEnrichment]:
        stmt = self.build_quality_sweep_stmt(limit=limit)
        return list(session.execute(stmt).scalars().all())

    def list_manual_review_queue(
        self,
        session: Session,
        *,
        limit: int = 100,
    ) -> list[SourceEnrichment]:
        stmt = self.build_manual_review_queue_stmt(limit=limit)
        return list(session.execute(stmt).scalars().all())

    def list_rejected_enrichments(
        self,
        session: Session,
        *,
        limit: int = 100,
    ) -> list[SourceEnrichment]:
        stmt = self.build_rejected_enrichments_stmt(limit=limit)
        return list(session.execute(stmt).scalars().all())

    def list_source_incident_ids_for_recanonicalize(
        self,
        session: Session,
        *,
        limit: int | None = None,
    ) -> list[object]:
        stmt = self.build_recanonicalize_candidates_stmt(limit=limit)
        return list(session.execute(stmt).scalars().all())

    def add(self, session: Session, enrichment: SourceEnrichment) -> SourceEnrichment:
        session.add(enrichment)
        return enrichment
