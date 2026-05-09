"""Initial v2 intake services for state tracking and worker task planning."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from src.edu_cti_v2.models import PipelineTask, SourceIncident
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceStateRepository


def determine_initial_task_type(source_incident: SourceIncident) -> str:
    """
    Pick the first worker task for a raw source incident.

    - `fetch_article` when the source already yielded a non-wrapper article URL
    - `resolve_url` otherwise, so the worker can discover a dedicated article
    """
    for url_row in source_incident.urls or []:
        if url_row.url_kind == "article" and not url_row.is_wrapper:
            return "fetch_article"
    return "resolve_url"


class V2IntakeService:
    """Coordinates first-touch state and queue writes for raw source observations."""

    def __init__(
        self,
        *,
        source_state_repository: Optional[SourceStateRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.source_state_repository = source_state_repository or SourceStateRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def record_incremental_state(
        self,
        session: Session,
        source_incident: SourceIncident,
        *,
        state_scope: str = "default",
        cursor_key: str = "default",
    ):
        payload = {
            "latest_source_event_key": source_incident.source_event_key,
            "latest_ingest_hash": source_incident.ingest_hash,
            "latest_collected_at": (
                source_incident.collected_at.astimezone(timezone.utc).isoformat()
                if source_incident.collected_at.tzinfo
                else source_incident.collected_at.replace(tzinfo=timezone.utc).isoformat()
            ),
        }
        return self.source_state_repository.upsert_state(
            session,
            source_name=source_incident.source_name,
            state_scope=state_scope,
            cursor_key=cursor_key,
            state_payload=payload,
            last_seen_published_at=source_incident.source_published_at,
        )

    def ensure_initial_processing_task(
        self,
        session: Session,
        source_incident: SourceIncident,
    ) -> PipelineTask:
        task_type = determine_initial_task_type(source_incident)
        existing = self.pipeline_task_repository.get_active_for_target(
            session,
            task_type=task_type,
            target_table="source_incidents",
            target_id=source_incident.id,
        )
        if existing is not None:
            return existing

        now = datetime.now(timezone.utc)
        task = PipelineTask(
            run_id=None,
            task_type=task_type,
            target_table="source_incidents",
            target_id=source_incident.id,
            status="queued",
            priority=40 if task_type == "fetch_article" else 60,
            payload={
                "source_incident_id": str(source_incident.id),
                "source_name": source_incident.source_name,
                "source_group": source_incident.source_group,
                "source_event_key": source_incident.source_event_key,
            },
            result={},
            available_at=now,
            attempt_count=0,
            max_attempts=5,
        )
        self.pipeline_task_repository.enqueue(session, task)
        return task
