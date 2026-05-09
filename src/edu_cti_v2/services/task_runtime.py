"""Minimal worker runtime for leasing and dispatching v2 pipeline tasks."""

from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository
from src.edu_cti_v2.services.fetching import V2FetchService


class V2TaskRuntime:
    """Lease and process one v2 task at a time."""

    def __init__(
        self,
        *,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        fetch_service: Optional[V2FetchService] = None,
    ) -> None:
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.fetch_service = fetch_service or V2FetchService(
            pipeline_task_repository=self.pipeline_task_repository
        )

    def process_next_task(
        self,
        session: Session,
        *,
        worker_id: str,
        task_type: Optional[str] = None,
        lease_seconds: int = 300,
    ):
        leased = self.pipeline_task_repository.lease_batch(
            session,
            worker_id=worker_id,
            task_type=task_type,
            limit=1,
            lease_seconds=lease_seconds,
        )
        if not leased:
            return None

        task = leased[0]
        try:
            if task.task_type == "fetch_article":
                source_incident = self.source_incident_repository.get_by_id(session, task.target_id)
                if source_incident is None:
                    raise ValueError(f"Source incident not found: {task.target_id}")
                result = self.fetch_service.fetch_articles_for_source_incident(
                    session,
                    source_incident,
                    worker_id=worker_id,
                )
                self.pipeline_task_repository.mark_completed(session, task, result)
                return task

            raise NotImplementedError(f"Task type not implemented yet: {task.task_type}")
        except Exception as exc:
            self.pipeline_task_repository.mark_failed(
                session,
                task,
                error=str(exc),
                dead_letter=isinstance(exc, NotImplementedError),
            )
            return task
