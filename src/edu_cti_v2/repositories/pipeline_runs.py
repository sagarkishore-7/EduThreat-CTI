"""Repository helpers for v2 pipeline run history."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import PipelineRun


class PipelineRunRepository:
    """Repository boundary for v2 worker run history."""

    @staticmethod
    def build_get_by_id_stmt(run_id) -> Select:
        return select(PipelineRun).where(PipelineRun.id == run_id).limit(1)

    @staticmethod
    def build_list_recent_stmt(
        *,
        limit: int = 20,
        statuses: Optional[Sequence[str]] = None,
    ) -> Select:
        stmt = select(PipelineRun).order_by(PipelineRun.created_at.desc()).limit(limit)
        if statuses:
            stmt = stmt.where(PipelineRun.status.in_(list(statuses)))
        return stmt

    def add(self, session: Session, run: PipelineRun) -> PipelineRun:
        session.add(run)
        return run

    def get_by_id(self, session: Session, run_id) -> PipelineRun | None:
        stmt = self.build_get_by_id_stmt(run_id)
        return session.execute(stmt).scalar_one_or_none()

    def list_recent(
        self,
        session: Session,
        *,
        limit: int = 20,
        statuses: Optional[Sequence[str]] = None,
    ) -> list[PipelineRun]:
        stmt = self.build_list_recent_stmt(limit=limit, statuses=statuses)
        return list(session.execute(stmt).scalars().all())

    def mark_started(self, session: Session, run: PipelineRun) -> PipelineRun:
        run.status = "running"
        run.started_at = datetime.now(timezone.utc)
        session.add(run)
        return run

    def mark_finished(
        self,
        session: Session,
        run: PipelineRun,
        *,
        status: str,
        result: dict | None = None,
        error: str | None = None,
    ) -> PipelineRun:
        run.status = status
        run.result = result or {}
        run.error = error
        run.finished_at = datetime.now(timezone.utc)
        session.add(run)
        return run

