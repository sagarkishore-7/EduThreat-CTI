"""Minimal worker runtime for leasing and dispatching v2 pipeline tasks."""

from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository
from src.edu_cti_v2.services.analytics import V2AnalyticsRefreshService
from src.edu_cti_v2.services.canonicalization import V2CanonicalizationService
from src.edu_cti_v2.services.enrichment import V2EnrichmentService
from src.edu_cti_v2.services.fetching import V2FetchService
from src.edu_cti_v2.services.resolution import V2ResolveUrlService


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
            if task.task_type in {"fetch_article", "resolve_url", "enrich_source", "canonicalize"}:
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
                canonical_incident_id = task.payload.get("canonical_incident_id") or task.target_id
                result = analytics_refresh_service.refresh_for_canonical_incident(
                    session,
                    canonical_incident_id,
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
