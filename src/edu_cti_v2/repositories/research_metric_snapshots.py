"""Repository helpers for durable v2 research-metrics snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import ResearchMetricSnapshot


class ResearchMetricSnapshotRepository:
    """Repository boundary for research snapshot history."""

    @staticmethod
    def build_latest_stmt(
        *,
        snapshot_key: str = "global",
        snapshot_scope: Optional[str] = None,
    ) -> Select:
        stmt = (
            select(ResearchMetricSnapshot)
            .where(ResearchMetricSnapshot.snapshot_key == snapshot_key)
            .order_by(ResearchMetricSnapshot.captured_at.desc(), ResearchMetricSnapshot.created_at.desc())
            .limit(1)
        )
        if snapshot_scope:
            stmt = stmt.where(ResearchMetricSnapshot.snapshot_scope == snapshot_scope)
        return stmt

    @staticmethod
    def build_list_recent_stmt(
        *,
        snapshot_key: Optional[str] = None,
        snapshot_scope: Optional[str] = None,
        run_ids: Optional[Sequence[object]] = None,
        limit: int = 20,
    ) -> Select:
        stmt = select(ResearchMetricSnapshot).order_by(
            ResearchMetricSnapshot.captured_at.desc(),
            ResearchMetricSnapshot.created_at.desc(),
        )
        if snapshot_key:
            stmt = stmt.where(ResearchMetricSnapshot.snapshot_key == snapshot_key)
        if snapshot_scope:
            stmt = stmt.where(ResearchMetricSnapshot.snapshot_scope == snapshot_scope)
        if run_ids:
            stmt = stmt.where(ResearchMetricSnapshot.run_id.in_(list(run_ids)))
        return stmt.limit(limit)

    def get_latest(
        self,
        session: Session,
        *,
        snapshot_key: str = "global",
        snapshot_scope: Optional[str] = None,
    ) -> ResearchMetricSnapshot | None:
        stmt = self.build_latest_stmt(snapshot_key=snapshot_key, snapshot_scope=snapshot_scope)
        return session.execute(stmt).scalar_one_or_none()

    def list_recent(
        self,
        session: Session,
        *,
        snapshot_key: Optional[str] = None,
        snapshot_scope: Optional[str] = None,
        run_ids: Optional[Sequence[object]] = None,
        limit: int = 20,
    ) -> list[ResearchMetricSnapshot]:
        stmt = self.build_list_recent_stmt(
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
            run_ids=run_ids,
            limit=limit,
        )
        return list(session.execute(stmt).scalars().all())

    def add_snapshot(
        self,
        session: Session,
        *,
        snapshot_key: str,
        snapshot_scope: str,
        payload: dict,
        run_id=None,
        captured_at: datetime | None = None,
    ) -> ResearchMetricSnapshot:
        snapshot = ResearchMetricSnapshot(
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
            run_id=run_id,
            captured_at=captured_at or datetime.now(timezone.utc),
            payload=payload,
        )
        session.add(snapshot)
        return snapshot
