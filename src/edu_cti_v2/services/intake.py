"""Initial v2 intake services for state tracking and worker task planning."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from src.edu_cti_v2.env import title_classify_enabled
from src.edu_cti_v2.models import PipelineTask, SourceIncident
from src.edu_cti_v2.repositories import (
    ArticleRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
    SourceStateRepository,
)

# Priority of the shared LLM title-classification sweep task. Sits between
# enrich_source (80) and fetch_article (60) so newly-collected titles get judged
# promptly without starving the heavier downstream stages.
CLASSIFY_SWEEP_PRIORITY = 70


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
        article_repository: Optional[ArticleRepository] = None,
        source_state_repository: Optional[SourceStateRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.article_repository = article_repository or ArticleRepository()
        self.source_state_repository = source_state_repository or SourceStateRepository()
        self.source_enrichment_repository = (
            source_enrichment_repository or SourceEnrichmentRepository()
        )
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

    def ensure_classify_sweep_task(
        self,
        session: Session,
        *,
        exclude_task_id: Optional[object] = None,
    ) -> Optional[PipelineTask]:
        """Ensure exactly one active ``classify_titles`` sweep task exists.

        The classifier processes *all* pending news/rss rows in batches, so a
        single shared sweep task is enough. De-dup against any queued/leased
        classify task so repeated collection never piles up redundant sweeps.
        ``exclude_task_id`` lets a running sweep task seed its own continuation
        without being blocked by its own (still-leased) lease.
        """
        active = self.pipeline_task_repository.count_active(
            session,
            statuses=("queued", "leased"),
            task_types=("classify_titles",),
            exclude_task_ids=[exclude_task_id] if exclude_task_id is not None else None,
        )
        if active > 0:
            return None
        now = datetime.now(timezone.utc)
        task = PipelineTask(
            run_id=None,
            task_type="classify_titles",
            target_table="source_incidents",
            target_id=None,
            status="queued",
            priority=CLASSIFY_SWEEP_PRIORITY,
            payload={},
            result={},
            available_at=now,
            attempt_count=0,
            max_attempts=5,
        )
        self.pipeline_task_repository.enqueue(session, task)
        return task

    def ensure_initial_processing_task(
        self,
        session: Session,
        source_incident: SourceIncident,
    ) -> Optional[PipelineTask]:
        if self.source_enrichment_repository.get_by_source_incident(session, source_incident.id):
            return None

        # LLM title-relevance gate: route news/rss through the bulk classifier
        # before any fetch; curated/api feeds are high-precision and bypass it.
        if title_classify_enabled():
            if source_incident.source_group in ("news", "rss"):
                status = source_incident.relevance_status
                if status == "pending":
                    # Defer fetch — the classifier decides relevance from the title.
                    self.ensure_classify_sweep_task(session)
                    return None
                if status == "irrelevant":
                    # Confident-negative title — never fetched (kept for audit).
                    return None
                # status == "relevant": classifier approved it; fall through to fetch.
            elif source_incident.relevance_status != "relevant":
                source_incident.relevance_status = "relevant"

        selected_document = self.article_repository.get_selected_document(
            session,
            source_incident.id,
        )
        task_type = (
            "enrich_source"
            if selected_document is not None
            else determine_initial_task_type(source_incident)
        )
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
            priority=(
                80
                if task_type == "enrich_source"
                else 60 if task_type == "fetch_article" else 20
            ),
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
