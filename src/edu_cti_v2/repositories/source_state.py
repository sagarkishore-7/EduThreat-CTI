"""Repository helpers for v2 source incremental state."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import SourceState


class SourceStateRepository:
    """Repository boundary for v2 source incremental state."""

    @staticmethod
    def build_get_state_stmt(
        source_name: str,
        *,
        state_scope: str = "default",
        cursor_key: str = "default",
    ) -> Select:
        return (
            select(SourceState)
            .where(SourceState.source_name == source_name)
            .where(SourceState.state_scope == state_scope)
            .where(SourceState.cursor_key == cursor_key)
            .limit(1)
        )

    def get_state(
        self,
        session: Session,
        source_name: str,
        *,
        state_scope: str = "default",
        cursor_key: str = "default",
    ) -> Optional[SourceState]:
        stmt = self.build_get_state_stmt(
            source_name,
            state_scope=state_scope,
            cursor_key=cursor_key,
        )
        return session.execute(stmt).scalar_one_or_none()

    def upsert_state(
        self,
        session: Session,
        *,
        source_name: str,
        state_payload: dict,
        last_seen_published_at: Optional[datetime] = None,
        state_scope: str = "default",
        cursor_key: str = "default",
    ) -> SourceState:
        state = self.get_state(
            session,
            source_name,
            state_scope=state_scope,
            cursor_key=cursor_key,
        )
        if state is None:
            state = SourceState(
                source_name=source_name,
                state_scope=state_scope,
                cursor_key=cursor_key,
                state_payload=state_payload,
                last_seen_published_at=last_seen_published_at,
                updated_at=datetime.now(timezone.utc),
            )
            session.add(state)
            return state

        state.state_payload = state_payload
        if last_seen_published_at and (
            state.last_seen_published_at is None
            or last_seen_published_at > state.last_seen_published_at
        ):
            state.last_seen_published_at = last_seen_published_at
        state.updated_at = datetime.now(timezone.utc)
        session.add(state)
        return state
