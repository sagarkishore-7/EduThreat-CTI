"""Repository helpers for durable worker task leasing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Sequence
from uuid import uuid4

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import PipelineTask


class PipelineTaskRepository:
    """Repository boundary for queueing and leasing pipeline tasks."""

    @staticmethod
    def build_active_target_task_stmt(
        *,
        task_type: str,
        target_table: str,
        target_id,
        statuses: Sequence[str] = ("queued", "leased"),
    ) -> Select:
        return (
            select(PipelineTask)
            .where(PipelineTask.task_type == task_type)
            .where(PipelineTask.target_table == target_table)
            .where(PipelineTask.target_id == target_id)
            .where(PipelineTask.status.in_(list(statuses)))
            .order_by(PipelineTask.created_at.desc())
            .limit(1)
        )

    @staticmethod
    def build_lease_batch_stmt(
        *,
        task_type: Optional[str] = None,
        limit: int = 10,
        now: Optional[datetime] = None,
    ) -> Select:
        now = now or datetime.now(timezone.utc)
        stmt = (
            select(PipelineTask)
            .where(PipelineTask.status == "queued")
            .where(PipelineTask.available_at <= now)
            .order_by(PipelineTask.priority.desc(), PipelineTask.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        if task_type:
            stmt = stmt.where(PipelineTask.task_type == task_type)
        return stmt

    @staticmethod
    def build_status_summary_stmt() -> Select:
        return (
            select(
                PipelineTask.task_type,
                PipelineTask.status,
                func.count(PipelineTask.id).label("task_count"),
            )
            .group_by(PipelineTask.task_type, PipelineTask.status)
            .order_by(PipelineTask.task_type.asc(), PipelineTask.status.asc())
        )

    @staticmethod
    def build_count_active_stmt(
        *,
        statuses: Sequence[str] = ("queued", "leased"),
        task_types: Optional[Sequence[str]] = None,
        exclude_task_types: Optional[Sequence[str]] = None,
        exclude_task_ids: Optional[Sequence[object]] = None,
    ) -> Select:
        stmt = select(func.count(PipelineTask.id)).where(PipelineTask.status.in_(list(statuses)))
        if task_types:
            stmt = stmt.where(PipelineTask.task_type.in_(list(task_types)))
        if exclude_task_types:
            stmt = stmt.where(~PipelineTask.task_type.in_(list(exclude_task_types)))
        if exclude_task_ids:
            stmt = stmt.where(~PipelineTask.id.in_(list(exclude_task_ids)))
        return stmt

    @staticmethod
    def build_recent_tasks_stmt(
        *,
        limit: int = 25,
        task_type: Optional[str] = None,
        statuses: Optional[Sequence[str]] = None,
    ) -> Select:
        stmt = select(PipelineTask).order_by(PipelineTask.created_at.desc()).limit(limit)
        if task_type:
            stmt = stmt.where(PipelineTask.task_type == task_type)
        if statuses:
            stmt = stmt.where(PipelineTask.status.in_(list(statuses)))
        return stmt

    def enqueue(self, session: Session, task: PipelineTask) -> PipelineTask:
        session.add(task)
        return task

    def get_active_for_target(
        self,
        session: Session,
        *,
        task_type: str,
        target_table: str,
        target_id,
        statuses: Sequence[str] = ("queued", "leased"),
    ) -> Optional[PipelineTask]:
        stmt = self.build_active_target_task_stmt(
            task_type=task_type,
            target_table=target_table,
            target_id=target_id,
            statuses=statuses,
        )
        return session.execute(stmt).scalar_one_or_none()

    def lease_batch(
        self,
        session: Session,
        *,
        worker_id: str,
        task_type: Optional[str] = None,
        limit: int = 10,
        lease_seconds: int = 300,
    ) -> List[PipelineTask]:
        now = datetime.now(timezone.utc)
        lease_token = str(uuid4())
        stmt = self.build_lease_batch_stmt(task_type=task_type, limit=limit, now=now)
        tasks = list(session.execute(stmt).scalars().all())
        expires_at = now + timedelta(seconds=lease_seconds)
        for task in tasks:
            task.status = "leased"
            task.lease_owner = worker_id
            task.lease_token = lease_token
            task.lease_expires_at = expires_at
            task.attempt_count += 1
        return tasks

    def get_status_summary(self, session: Session) -> list[dict[str, object]]:
        stmt = self.build_status_summary_stmt()
        return [
            {
                "task_type": row.task_type,
                "status": row.status,
                "task_count": int(row.task_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def count_active(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("queued", "leased"),
        task_types: Optional[Sequence[str]] = None,
        exclude_task_types: Optional[Sequence[str]] = None,
        exclude_task_ids: Optional[Sequence[object]] = None,
    ) -> int:
        stmt = self.build_count_active_stmt(
            statuses=statuses,
            task_types=task_types,
            exclude_task_types=exclude_task_types,
            exclude_task_ids=exclude_task_ids,
        )
        return int(session.execute(stmt).scalar_one() or 0)

    def list_recent(
        self,
        session: Session,
        *,
        limit: int = 25,
        task_type: Optional[str] = None,
        statuses: Optional[Sequence[str]] = None,
    ) -> list[PipelineTask]:
        stmt = self.build_recent_tasks_stmt(limit=limit, task_type=task_type, statuses=statuses)
        return list(session.execute(stmt).scalars().all())

    def mark_completed(self, session: Session, task: PipelineTask, result: Optional[dict] = None) -> None:
        task.status = "completed"
        task.result = result or {}
        task.lease_owner = None
        task.lease_token = None
        task.lease_expires_at = None
        session.add(task)

    def mark_failed(
        self,
        session: Session,
        task: PipelineTask,
        *,
        error: str,
        retry_at: Optional[datetime] = None,
        dead_letter: bool = False,
    ) -> None:
        task.error = error
        task.lease_owner = None
        task.lease_token = None
        task.lease_expires_at = None
        if dead_letter or task.attempt_count >= task.max_attempts:
            task.status = "dead_letter"
        else:
            task.status = "queued"
            task.available_at = retry_at or datetime.now(timezone.utc)
        session.add(task)
