"""Repository helpers for v2 article documents and fetch attempts."""

from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import ArticleDocument, ArticleFetchAttempt


class ArticleRepository:
    """Repository boundary for article persistence in v2."""

    @staticmethod
    def build_get_selected_document_stmt(source_incident_id) -> Select:
        return (
            select(ArticleDocument)
            .where(ArticleDocument.source_incident_id == source_incident_id)
            .order_by(ArticleDocument.is_selected_for_enrichment.desc(), ArticleDocument.fetched_at.desc())
            .limit(1)
        )

    @staticmethod
    def build_get_document_by_source_url_stmt(source_incident_url_id) -> Select:
        return (
            select(ArticleDocument)
            .where(ArticleDocument.source_incident_url_id == source_incident_url_id)
            .limit(1)
        )

    @staticmethod
    def build_list_fetch_attempts_stmt(source_incident_id, *, limit: int = 10) -> Select:
        return (
            select(ArticleFetchAttempt)
            .where(ArticleFetchAttempt.source_incident_id == source_incident_id)
            .order_by(ArticleFetchAttempt.attempted_at.desc())
            .limit(limit)
        )

    def get_selected_document(
        self,
        session: Session,
        source_incident_id,
    ) -> ArticleDocument | None:
        stmt = self.build_get_selected_document_stmt(source_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def get_document_by_source_url(
        self,
        session: Session,
        source_incident_url_id,
    ) -> ArticleDocument | None:
        stmt = self.build_get_document_by_source_url_stmt(source_incident_url_id)
        return session.execute(stmt).scalar_one_or_none()

    def list_fetch_attempts(
        self,
        session: Session,
        source_incident_id,
        *,
        limit: int = 10,
    ) -> list[ArticleFetchAttempt]:
        stmt = self.build_list_fetch_attempts_stmt(source_incident_id, limit=limit)
        return list(session.execute(stmt).scalars().all())

    def add_document(self, session: Session, document: ArticleDocument) -> ArticleDocument:
        session.add(document)
        return document

    def add_fetch_attempt(self, session: Session, attempt: ArticleFetchAttempt) -> ArticleFetchAttempt:
        session.add(attempt)
        return attempt
