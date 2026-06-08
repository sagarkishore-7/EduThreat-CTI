"""Minimal worker runtime for leasing and dispatching v2 pipeline tasks."""

from __future__ import annotations

import logging
import os
import time
from typing import Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import PipelineTask
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository
from src.edu_cti_v2.services.analytics import V2AnalyticsRefreshService
from src.edu_cti_v2.services.campaigns import V2CampaignService
from src.edu_cti_v2.services.canonicalization import V2CanonicalizationService
from src.edu_cti_v2.services.enrichment import V2EnrichmentService
from src.edu_cti_v2.services.fetching import V2FetchService
from src.edu_cti_v2.services.orchestration import V2OrchestrationService
from src.edu_cti_v2.services.resolution import V2ResolveUrlService
from src.edu_cti.core.logging_utils import bind_log_context, clear_log_context

logger = logging.getLogger(__name__)

MEMORY_HEAVY_TASK_TYPES = ("enrich_source", "reenrich")
MEMORY_HEAVY_LEASE_LOCK_KEY = 0xEDC71002

DEFAULT_TASK_LEASE_ORDER = (
    "orchestrate_plan",
    "reenrich",
    "enrich_source",
    "canonicalize",
    "fetch_article",
    "refresh_analytics",
    "campaign_correlate",
    "resolve_url",
)


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _default_max_active_enrich_tasks() -> int:
    configured = os.environ.get("EDU_CTI_V2_MAX_ACTIVE_ENRICH_TASKS")
    if configured is not None and configured.strip():
        requested = _env_int("EDU_CTI_V2_MAX_ACTIVE_ENRICH_TASKS", 0)
        if (
            os.environ.get("RAILWAY_ENVIRONMENT")
            and requested > 1
            and os.environ.get("EDU_CTI_V2_ALLOW_HIGH_ENRICH_CONCURRENCY", "").strip().lower()
            not in {"1", "true", "yes", "on"}
        ):
            return 1
        return requested
    # Railway hobby workers are memory-constrained; keep only one GLiNER/LLM
    # enrichment leased at a time unless an explicit override is configured.
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        return 1
    return 0


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
        campaign_service: Optional[V2CampaignService] = None,
        orchestration_service: Optional[V2OrchestrationService] = None,
        max_fetch_backlog: Optional[int] = None,
        max_active_enrich_tasks: Optional[int] = None,
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
        self.campaign_service = campaign_service
        self.orchestration_service = orchestration_service
        self.max_fetch_backlog = max_fetch_backlog if max_fetch_backlog is not None else int(
            os.environ.get("EDU_CTI_V2_MAX_FETCH_BACKLOG", "600")
        )
        self.max_active_enrich_tasks = (
            max_active_enrich_tasks
            if max_active_enrich_tasks is not None
            else _default_max_active_enrich_tasks()
        )

    def _enrich_concurrency_full(self, session: Session) -> bool:
        if self.max_active_enrich_tasks <= 0:
            return False
        active = self.pipeline_task_repository.count_active(
            session,
            statuses=("leased",),
            task_types=MEMORY_HEAVY_TASK_TYPES,
        )
        return active >= self.max_active_enrich_tasks

    def _try_acquire_memory_heavy_lease_lock(self, session: Session) -> bool:
        """Serialize memory-heavy task leases on Postgres.

        The active-count guard and row lease are separate statements. Without a
        transaction-level lock, multiple worker threads can all observe zero
        active enrich tasks and then lease different enrich tasks concurrently.
        """
        bind = session.get_bind()
        if getattr(getattr(bind, "dialect", None), "name", None) != "postgresql":
            return True
        acquired = session.execute(
            text("select pg_try_advisory_xact_lock(:lock_key)"),
            {"lock_key": MEMORY_HEAVY_LEASE_LOCK_KEY},
        ).scalar_one()
        return bool(acquired)

    def _lease_candidate_task(
        self,
        session: Session,
        *,
        worker_id: str,
        candidate_type: str,
        lease_seconds: int,
        exclude_task_types: Optional[Sequence[str]] = None,
    ):
        if candidate_type in MEMORY_HEAVY_TASK_TYPES:
            if not self._try_acquire_memory_heavy_lease_lock(session):
                return None
            if self._enrich_concurrency_full(session):
                return None
        leased = self.pipeline_task_repository.lease_batch(
            session,
            worker_id=worker_id,
            task_type=candidate_type,
            exclude_task_types=exclude_task_types,
            limit=1,
            lease_seconds=lease_seconds,
        )
        return leased[0] if leased else None

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
            if task_type == "resolve_url" and self.max_fetch_backlog > 0:
                fetch_backlog = self.pipeline_task_repository.count_active(
                    session,
                    task_types=("fetch_article",),
                )
                if fetch_backlog >= self.max_fetch_backlog:
                    return None
            return self._lease_candidate_task(
                session,
                worker_id=worker_id,
                candidate_type=task_type,
                exclude_task_types=exclude_task_types,
                lease_seconds=lease_seconds,
            )

        # Keep the expensive pipeline stages flowing before discovery keeps
        # expanding the queue, otherwise fetch/enrich work starves behind URL
        # resolution during large historical runs.
        for candidate_type in DEFAULT_TASK_LEASE_ORDER:
            if exclude_task_types and candidate_type in set(exclude_task_types):
                continue
            task = self._lease_candidate_task(
                session,
                worker_id=worker_id,
                candidate_type=candidate_type,
                exclude_task_types=exclude_task_types,
                lease_seconds=lease_seconds,
            )
            if task is not None:
                return task
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
            # Bind the incident id so the whole fetch -> resolve -> enrich ->
            # canonicalize chain for this incident is filterable as one unit.
            bind_log_context(source_incident_id=str(source_incident.id))
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
                task_payload = task.payload if isinstance(getattr(task, "payload", None), dict) else {}
                if task_payload.get("force_discovery"):
                    result = resolve_url_service.resolve_source_incident_urls(
                        session,
                        source_incident,
                        force_discovery=True,
                    )
                else:
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

        if task.task_type == "campaign_correlate":
            campaign_service = self.campaign_service or V2CampaignService(
                pipeline_task_repository=self.pipeline_task_repository,
            )
            self.campaign_service = campaign_service
            payload = task.payload if isinstance(task.payload, dict) else {}
            result = campaign_service.run_correlation(
                session,
                include_excluded=bool(payload.get("include_excluded", True)),
                limit=payload.get("limit"),
                correlation_version=payload.get("correlation_version") or "campaign_corr_v1",
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

        # Bind task identity to every downstream log line so a single task can be
        # traced across the resolve -> fetch -> enrich -> canonicalize stages.
        bind_log_context(
            task_id=str(task_id),
            task_type=task.task_type,
            run_id=str(task.run_id) if getattr(task, "run_id", None) else None,
            worker_id=worker_id,
        )
        started = time.monotonic()
        logger.debug("task_started")
        try:
            result = self._process_task(session, task=task, worker_id=worker_id)
            logger.info(
                "task_completed",
                extra={"elapsed_ms": round((time.monotonic() - started) * 1000)},
            )
            return result
        except Exception as exc:
            session.rollback()
            failed_task = session.get(PipelineTask, task.id) if getattr(task, "id", None) is not None else task
            if failed_task is None:
                raise
            dead_letter = isinstance(exc, NotImplementedError)
            self.pipeline_task_repository.mark_failed(
                session,
                failed_task,
                error=str(exc),
                dead_letter=dead_letter,
            )
            logger.warning(
                "task_dead_lettered" if dead_letter else "task_failed",
                extra={
                    "elapsed_ms": round((time.monotonic() - started) * 1000),
                    "error": str(exc),
                },
            )
            return failed_task
        finally:
            clear_log_context()

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
