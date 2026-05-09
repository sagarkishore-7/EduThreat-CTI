"""Repository helpers for persisted v2 analytics refresh snapshots."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import AnalyticsRefreshState


class AnalyticsRefreshRepository:
    """Repository boundary for cached analytics/read-model snapshots."""

    @staticmethod
    def build_get_by_key_stmt(refresh_key: str) -> Select:
        return (
            select(AnalyticsRefreshState)
            .where(AnalyticsRefreshState.refresh_key == refresh_key)
            .limit(1)
        )

    def get_by_key(self, session: Session, refresh_key: str) -> AnalyticsRefreshState | None:
        stmt = self.build_get_by_key_stmt(refresh_key)
        return session.execute(stmt).scalar_one_or_none()

    def upsert_snapshot(
        self,
        session: Session,
        *,
        refresh_key: str,
        refresh_scope: str,
        state_payload: dict,
        needs_refresh: bool = False,
        last_refreshed_at: datetime | None = None,
    ) -> AnalyticsRefreshState:
        existing = self.get_by_key(session, refresh_key)
        now = datetime.now(timezone.utc)
        if existing is None:
            existing = AnalyticsRefreshState(
                refresh_key=refresh_key,
                refresh_scope=refresh_scope,
                needs_refresh=needs_refresh,
                last_refreshed_at=last_refreshed_at or now,
                state_payload=state_payload,
                updated_at=now,
            )
        else:
            existing.refresh_scope = refresh_scope
            existing.needs_refresh = needs_refresh
            existing.last_refreshed_at = last_refreshed_at or now
            existing.state_payload = state_payload
            existing.updated_at = now
        session.add(existing)
        return existing

