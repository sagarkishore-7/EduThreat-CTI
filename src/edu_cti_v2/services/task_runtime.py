"""Minimal worker runtime for leasing and dispatching v2 pipeline tasks."""

from __future__ import annotations

from typing import Optional, Sequence

from sqlalchemy.orm import Session

from src.edu_cti_v2.models import PipelineTask
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository
from src.edu_cti_v2.services.analytics import V2AnalyticsRefreshService
from src.edu_cti_v2.services.canonicalization import V2CanonicalizationService
from src.edu_cti_v2.services.enrichment import V2EnrichmentService
from src.edu_cti_v2.services.fetching import V2FetchService
from src.edu_cti_v2.services.orchestration import V2OrchestrationService
from src.edu_cti_v2.services.resolution import V2ResolveUrlService

DEFAULT_TASK_LEASE_ORDER = (
    "orchestrate_plan",
    "reenrich",
    "enrich_source",
    "canonicalize",
    "fetch_article",
    "refresh_analytics",
    "resolve_url",
)


class V2TaskRuntime:
    """Lease and process one v2 task at a time."""

    def __init__(
        self,
        *,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        fetch_service: Optional[V2FetchService] = None,
        resolve_url_service: Optional[V2ResolveUrlService] = None,
        enrichment_service: Optional[V2EnrichmentService] = None,
        canonicalization_service: Optional[V2CanonicalizationService] = None,
        analytics_refresh_service: Optional[V2AnalyticsRefreshService] = None,
        orchestration_service: Optional[V2OrchestrationService] = None,
    ) -> None:
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.fetch_service = fetch_service or V2FetchService(
            pipeline_task_repository=self.pipeline_task_repository
        )
        self.resolve_url_service = resolve_url_service
        self.enrichment_service = enrichment_service
        self.canonicalization_service = canonicalization_service
        self.analytics_refresh_service = analytics_refresh_service
        self.orchestration_service = orchestration_service

    def _lease_next_task(
        self,
        session: Session,
        *,
        worker_id: str,
        task_type: Optional[str],
        lease_seconds: int,
        exclude_task_types: Optional[Sequence[str]] = None,
    ):
        if task_type:
            if exclude_task_types and task_type in set(exclude_task_types):
                return None
            leased = self.pipeline_task_repository.lease_batch(
                session,
                worker_id=worker_id,
                task_type=task_type,
                exclude_task_types=exclude_task_types,
                limit=1,
                lease_seconds=lease_seconds,
            )
            return leased[0] if leased else None

        # Keep the expensive pipeline stages flowing before discovery keeps
        # expanding the queue, otherwise fetch/enrich work starves behind URL
        # resolution during large historical runs.
        for candidate_type in DEFAULT_TASK_LEASE_ORDER:
            if exclude_task_types and candidate_type in set(exclude_task_types):
                continue
            leased = self.pipeline_task_repository.lease_batch(
                session,
                worker_id=worker_id,
                task_type=candidate_type,
                exclude_task_types=exclude_task_types,
                limit=1,
                lease_seconds=lease_seconds,
            )
            if leased:
                return leased[0]
        return None

    def lease_next_task(
        self,
        session: Session,
        *,
        worker_id: str,
        task_type: Optional[str] = None,
        lease_seconds: int = 300,
        exclude_task_types: Optional[Sequence[str]] = None,
    ):
        self.pipeline_task_repository.requeue_expired_leases(session, limit=50)
        task = self._lease_next_task(
            session,
            worker_id=worker_id,
            task_type=task_type,
            lease_seconds=lease_seconds,
            exclude_task_types=exclude_task_types,
        )
        return getattr(task, "id", None) if task is not None else None

    def _process_task(
        self,
        session: Session,
        *,
        task,
        worker_id: str,
    ):
        if task.task_type == "orchestrate_plan":
            orchestration_service = self.orchestration_service or V2OrchestrationService(
                pipeline_task_repository=self.pipeline_task_repository,
            )
            self.orchestration_service = orchestration_service
            result = orchestration_service.execute_enqueued_plan(task, worker_id=worker_id)
            self.pipeline_task_repository.mark_completed(session, task, result)
            return task

        if task.task_type in {"fetch_article", "resolve_url", "enrich_source", "canonicalize", "reenrich"}:
            source_incident = self.source_incident_repository.get_by_id(session, task.target_id)
            if source_incident is None:
                raise ValueError(f"Source incident not found: {task.target_id}")
            if task.task_type == "fetch_article":
                result = self.fetch_service.fetch_articles_for_source_incident(
                    session,
                    source_incident,
                    worker_id=worker_id,
                )
            elif task.task_type == "resolve_url":
                resolve_url_service = self.resolve_url_service or V2ResolveUrlService(
                    source_incident_repository=self.source_incident_repository,
                    pipeline_task_repository=self.pipeline_task_repository,
                )
                self.resolve_url_service = resolve_url_service
                result = resolve_url_service.resolve_source_incident_urls(
                    session,
                    source_incident,
                )
            elif task.task_type == "enrich_source":
                enrichment_service = self.enrichment_service or V2EnrichmentService(
                    pipeline_task_repository=self.pipeline_task_repository,
                )
                self.enrichment_service = enrichment_service
                result = enrichment_service.enrich_source_incident(
                    session,
                    source_incident,
                )
            elif task.task_type == "reenrich":
                enrichment_service = self.enrichment_service or V2EnrichmentService(
                    pipeline_task_repository=self.pipeline_task_repository,
                )
                self.enrichment_service = enrichment_service
                result = enrichment_service.enrich_source_incident(
                    session,
                    source_incident,
                    re_enrich_attempts=task.payload.get("re_enrich_attempts"),
                    re_enrich_reason=task.payload.get("re_enrich_reason"),
                    force_canonicalize=True,
                )
            else:
                canonicalization_service = self.canonicalization_service or V2CanonicalizationService(
                    source_incident_repository=self.source_incident_repository,
                    pipeline_task_repository=self.pipeline_task_repository,
                )
                self.canonicalization_service = canonicalization_service
                result = canonicalization_service.canonicalize_source_incident(
                    session,
                    source_incident.id,
                )
            self.pipeline_task_repository.mark_completed(session, task, result)
            return task
        if task.task_type == "refresh_analytics":
            analytics_refresh_service = self.analytics_refresh_service or V2AnalyticsRefreshService()
            self.analytics_refresh_service = analytics_refresh_service
            payload = task.payload if isinstance(task.payload, dict) else {}
            refresh_key = payload.get("refresh_key")
            target_table = getattr(task, "target_table", None)
            if target_table == "analytics_refresh_state" or refresh_key == "dashboard:global":
                result = analytics_refresh_service.refresh_dashboard_snapshot(
                    session,
                    last_trigger_canonical_incident_id=payload.get("canonical_incident_id"),
                )
            else:
                canonical_incident_id = payload.get("canonical_incident_id") or task.target_id
                result = analytics_refresh_service.refresh_canonical_incident_snapshot(
                    session,
                    canonical_incident_id,
                )
            self.pipeline_task_repository.mark_completed(session, task, result)
            return task

        raise NotImplementedError(f"Task type not implemented yet: {task.task_type}")

    def process_leased_task(
        self,
        session: Session,
        *,
        task_id,
        worker_id: str,
    ):
        task = session.get(PipelineTask, task_id)
        if task is None:
            return None

        try:
            return self._process_task(session, task=task, worker_id=worker_id)
        except Exception as exc:
            session.rollback()
            failed_task = session.get(PipelineTask, task.id) if getattr(task, "id", None) is not None else task
            if failed_task is None:
                raise
            self.pipeline_task_repository.mark_failed(
                session,
                failed_task,
                error=str(exc),
                dead_letter=isinstance(exc, NotImplementedError),
            )
            return failed_task

    def process_next_task(
        self,
        session: Session,
        *,
        worker_id: str,
        task_type: Optional[str] = None,
        lease_seconds: int = 300,
        exclude_task_types: Optional[Sequence[str]] = None,
    ):
        task_id = self.lease_next_task(
            session,
            worker_id=worker_id,
            task_type=task_type,
            lease_seconds=lease_seconds,
            exclude_task_types=exclude_task_types,
        )
        if task_id is None:
            return None
        return self.process_leased_task(session, task_id=task_id, worker_id=worker_id)
