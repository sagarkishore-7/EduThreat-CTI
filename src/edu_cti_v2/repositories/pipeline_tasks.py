"""Repository helpers for durable worker task leasing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Sequence
from uuid import uuid4

from sqlalchemy import Select, select
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
            .order_by(PipelineTask.priority.asc(), PipelineTask.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        if task_type:
            stmt = stmt.where(PipelineTask.task_type == task_type)
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
