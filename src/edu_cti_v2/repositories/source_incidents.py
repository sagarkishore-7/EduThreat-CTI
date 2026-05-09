"""Repository helpers for source observation tables."""

from __future__ import annotations

from typing import Iterable, Sequence

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import SourceIncident, SourceIncidentUrl


class SourceIncidentRepository:
    """Repository boundary for source incident reads and writes."""

    @staticmethod
    def build_get_by_source_event_key_stmt(source_name: str, source_event_key: str) -> Select:
        return (
            select(SourceIncident)
            .where(SourceIncident.source_name == source_name)
            .where(SourceIncident.source_event_key == source_event_key)
            .limit(1)
        )

    @staticmethod
    def build_candidate_urls_stmt(normalized_urls: Sequence[str]) -> Select:
        return (
            select(SourceIncidentUrl)
            .where(SourceIncidentUrl.normalized_url.in_(list(normalized_urls)))
        )

    def get_by_source_event_key(
        self,
        session: Session,
        source_name: str,
        source_event_key: str,
    ) -> SourceIncident | None:
        stmt = self.build_get_by_source_event_key_stmt(source_name, source_event_key)
        return session.execute(stmt).scalar_one_or_none()

    def add(self, session: Session, source_incident: SourceIncident) -> SourceIncident:
        session.add(source_incident)
        return source_incident

    def add_urls(
        self,
        session: Session,
        source_incident: SourceIncident,
        urls: Iterable[SourceIncidentUrl],
    ) -> None:
        for url in urls:
            url.source_incident = source_incident
            session.add(url)
