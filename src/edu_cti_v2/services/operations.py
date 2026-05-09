"""Operational status and manual control helpers for the v2 runtime."""

from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional, Sequence
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import (
    AnalyticsRefreshState,
    ArticleDocument,
    CanonicalEnrichment,
    CanonicalIncident,
    PipelineRun,
    PipelineTask,
    SourceEnrichment,
    SourceIncident,
)
from src.edu_cti_v2.repositories import (
    AnalyticsRefreshRepository,
    CanonicalIncidentRepository,
    PipelineRunRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
)
from src.edu_cti_v2.worker import run_worker_loop

_CANONICAL_CONSISTENCY_FIELDS = (
    "institution_name",
    "institution_type",
    "vendor_name",
    "country",
    "country_code",
    "region",
    "city",
    "incident_date",
    "attack_category",
    "attack_vector",
    "threat_actor_name",
    "ransomware_family",
    "severity",
    "is_education_related",
)

_OPTIONAL_ANALYTICS_FIELDS = {
    "region",
    "city",
}


def _serialize_consistency_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _normalized_consistency_value(value: Any) -> Any:
    serialized = _serialize_consistency_value(value)
    if serialized is None:
        return None
    if isinstance(serialized, str):
        collapsed = " ".join(serialized.split()).strip().casefold()
        return collapsed or None
    return serialized


def _canonical_consistency_mismatches(
    canonical: CanonicalIncident,
    analytics_projection: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    projection = analytics_projection if isinstance(analytics_projection, dict) else {}
    mismatches: dict[str, dict[str, Any]] = {}
    for field in _CANONICAL_CONSISTENCY_FIELDS:
        actual = _serialize_consistency_value(getattr(canonical, field, None))
        expected = _serialize_consistency_value(projection.get(field))
        if expected is None and field in _OPTIONAL_ANALYTICS_FIELDS:
            continue
        if _normalized_consistency_value(actual) == _normalized_consistency_value(expected):
            continue
        if actual is None and expected is None:
            continue
        mismatches[field] = {
            "canonical": actual,
            "analytics_projection": expected,
        }
    return mismatches


def _serialize_task(task: PipelineTask) -> dict[str, Any]:
    return {
        "task_id": str(task.id),
        "run_id": str(task.run_id) if task.run_id else None,
        "task_type": task.task_type,
        "target_table": task.target_table,
        "target_id": str(task.target_id) if task.target_id else None,
        "status": task.status,
        "priority": task.priority,
        "available_at": task.available_at.isoformat() if task.available_at else None,
        "lease_owner": task.lease_owner,
        "lease_expires_at": task.lease_expires_at.isoformat() if task.lease_expires_at else None,
        "attempt_count": task.attempt_count,
        "max_attempts": task.max_attempts,
        "error": task.error,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "updated_at": task.updated_at.isoformat() if task.updated_at else None,
    }


def _serialize_run(run: PipelineRun) -> dict[str, Any]:
    return {
        "run_id": str(run.id),
        "run_type": run.run_type,
        "status": run.status,
        "service_name": run.service_name,
        "params": run.params or {},
        "result": run.result or {},
        "error": run.error,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "updated_at": run.updated_at.isoformat() if run.updated_at else None,
    }


class V2OperationsService:
    """Expose v2 queue status and bounded worker execution."""

    def __init__(
        self,
        *,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        pipeline_run_repository: Optional[PipelineRunRepository] = None,
        analytics_refresh_repository: Optional[AnalyticsRefreshRepository] = None,
        canonical_incident_repository: Optional[CanonicalIncidentRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        session_factory: Optional[Callable] = None,
    ) -> None:
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.pipeline_run_repository = pipeline_run_repository or PipelineRunRepository()
        self.analytics_refresh_repository = analytics_refresh_repository or AnalyticsRefreshRepository()
        self.canonical_incident_repository = canonical_incident_repository or CanonicalIncidentRepository()
        self.source_enrichment_repository = source_enrichment_repository or SourceEnrichmentRepository()
        self.session_factory = session_factory

    def get_runtime_status(self, session: Session, *, recent_limit: int = 10) -> dict[str, Any]:
        source_incident_count = int(session.execute(select(func.count(SourceIncident.id))).scalar_one() or 0)
        article_document_count = int(session.execute(select(func.count(ArticleDocument.id))).scalar_one() or 0)
        source_enrichment_count = int(session.execute(select(func.count(SourceEnrichment.id))).scalar_one() or 0)
        canonical_incident_count = int(session.execute(select(func.count(CanonicalIncident.id))).scalar_one() or 0)
        dashboard_snapshot = self.analytics_refresh_repository.get_by_key(session, "dashboard:global")
        latest_runs = [
            _serialize_run(run)
            for run in self.pipeline_run_repository.list_recent(session, limit=recent_limit)
        ]
        recent_tasks = [
            _serialize_task(task)
            for task in self.pipeline_task_repository.list_recent(session, limit=recent_limit)
        ]
        return {
            "counts": {
                "source_incidents": source_incident_count,
                "article_documents": article_document_count,
                "source_enrichments": source_enrichment_count,
                "canonical_incidents": canonical_incident_count,
            },
            "queue_health": {
                "expired_leases": self.pipeline_task_repository.count_expired_leases(session),
            },
            "task_summary": self.pipeline_task_repository.get_status_summary(session),
            "recent_tasks": recent_tasks,
            "recent_runs": latest_runs,
            "dashboard_snapshot": {
                "last_refreshed_at": (
                    dashboard_snapshot.last_refreshed_at.isoformat()
                    if dashboard_snapshot and dashboard_snapshot.last_refreshed_at
                    else None
                ),
                "needs_refresh": bool(dashboard_snapshot.needs_refresh) if dashboard_snapshot else None,
            },
        }

    def list_tasks(
        self,
        session: Session,
        *,
        limit: int = 25,
        task_type: Optional[str] = None,
        statuses: Optional[Sequence[str]] = None,
    ) -> list[dict[str, Any]]:
        return [
            _serialize_task(task)
            for task in self.pipeline_task_repository.list_recent(
                session,
                limit=limit,
                task_type=task_type,
                statuses=statuses,
            )
        ]

    def list_runs(
        self,
        session: Session,
        *,
        limit: int = 20,
        statuses: Optional[Sequence[str]] = None,
    ) -> list[dict[str, Any]]:
        return [
            _serialize_run(run)
            for run in self.pipeline_run_repository.list_recent(
                session,
                limit=limit,
                statuses=statuses,
            )
        ]

    def run_worker_batch(
        self,
        *,
        worker_id: str,
        task_type: Optional[str] = None,
        max_tasks: int = 25,
        stop_when_idle: bool = True,
        poll_interval: float = 0.0,
    ) -> dict[str, Any]:
        if self.session_factory is None:
            raise RuntimeError("session_factory is required for run_worker_batch")

        with self.session_factory() as session:
            run = PipelineRun(
                run_type="maintenance",
                status="pending",
                service_name="v2-api-manual-worker",
                params={
                    "worker_id": worker_id,
                    "task_type": task_type,
                    "max_tasks": max_tasks,
                    "stop_when_idle": stop_when_idle,
                },
                result={},
            )
            if run.id is None:
                run.id = uuid4()
            self.pipeline_run_repository.add(session, run)
            self.pipeline_run_repository.mark_started(session, run)
            flush = getattr(session, "flush", None)
            if callable(flush):
                flush()
            session.commit()
            run_id = run.id

        try:
            summary = run_worker_loop(
                session_factory=self.session_factory,
                worker_id=worker_id,
                task_type=task_type,
                max_tasks=max_tasks,
                stop_when_idle=stop_when_idle,
                poll_interval=poll_interval,
            )
            result = asdict(summary)
            with self.session_factory() as session:
                persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
                if persisted_run is not None:
                    self.pipeline_run_repository.mark_finished(
                        session,
                        persisted_run,
                        status="completed",
                        result=result,
                    )
                    session.commit()
            return {
                "run_id": str(run_id),
                "status": "completed",
                "result": result,
            }
        except Exception as exc:
            with self.session_factory() as session:
                persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
                if persisted_run is not None:
                    self.pipeline_run_repository.mark_finished(
                        session,
                        persisted_run,
                        status="failed",
                        result={},
                        error=str(exc),
                    )
                    session.commit()
            raise

    def queue_recanonicalization_sweep(
        self,
        session: Session,
        *,
        limit: int = 500,
        priority: int = 125,
    ) -> dict[str, Any]:
        source_incident_ids = self.source_enrichment_repository.list_source_incident_ids_for_recanonicalize(
            session,
            limit=limit,
        )
        queued = 0
        skipped_existing = 0
        now = datetime.now(timezone.utc)

        for source_incident_id in source_incident_ids:
            existing_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="canonicalize",
                target_table="source_incidents",
                target_id=source_incident_id,
            )
            if existing_task is not None:
                skipped_existing += 1
                continue

            task = PipelineTask(
                run_id=None,
                task_type="canonicalize",
                target_table="source_incidents",
                target_id=source_incident_id,
                status="queued",
                priority=priority,
                payload={
                    "trigger": "recanonicalize_sweep",
                },
                result={},
                available_at=now,
                attempt_count=0,
                max_attempts=5,
            )
            self.pipeline_task_repository.enqueue(session, task)
            queued += 1

        return {
            "limit": limit,
            "candidates_considered": len(source_incident_ids),
            "queued": queued,
            "skipped_existing": skipped_existing,
        }

    def queue_recanonicalization_for_canonical(
        self,
        session: Session,
        *,
        canonical_incident_id: str,
        priority: int = 110,
    ) -> dict[str, Any]:
        canonical = self.canonical_incident_repository.get_by_id(session, canonical_incident_id)
        if canonical is None:
            return {
                "canonical_incident_id": canonical_incident_id,
                "found": False,
                "membership_count": 0,
                "queued": 0,
                "skipped_existing": 0,
            }

        memberships = self.canonical_incident_repository.list_memberships(session, canonical_incident_id)
        queued = 0
        skipped_existing = 0
        now = datetime.now(timezone.utc)

        for membership in memberships:
            source_incident_id = membership.source_incident_id
            existing_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="canonicalize",
                target_table="source_incidents",
                target_id=source_incident_id,
            )
            if existing_task is not None:
                skipped_existing += 1
                continue

            self.pipeline_task_repository.enqueue(
                session,
                PipelineTask(
                    run_id=None,
                    task_type="canonicalize",
                    target_table="source_incidents",
                    target_id=source_incident_id,
                    status="queued",
                    priority=priority,
                    payload={
                        "trigger": "recanonicalize_canonical",
                        "canonical_incident_id": canonical_incident_id,
                    },
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=5,
                ),
            )
            queued += 1

        return {
            "canonical_incident_id": canonical_incident_id,
            "found": True,
            "membership_count": len(memberships),
            "queued": queued,
            "skipped_existing": skipped_existing,
        }

    def list_canonical_consistency_candidates(
        self,
        session: Session,
        *,
        limit: int = 100,
        scan_limit: int = 1000,
    ) -> list[dict[str, Any]]:
        rows = (
            session.execute(
                select(CanonicalIncident, CanonicalEnrichment)
                .join(
                    CanonicalEnrichment,
                    CanonicalEnrichment.canonical_incident_id == CanonicalIncident.id,
                )
                .where(CanonicalIncident.status == "open")
                .order_by(CanonicalIncident.updated_at.desc(), CanonicalIncident.created_at.desc())
                .limit(scan_limit)
            )
            .all()
        )
        items: list[dict[str, Any]] = []
        for canonical, canonical_enrichment in rows:
            mismatches = _canonical_consistency_mismatches(
                canonical,
                canonical_enrichment.analytics_projection,
            )
            if not mismatches:
                continue
            items.append(
                {
                    "canonical_incident_id": str(canonical.id),
                    "display_name": canonical.institution_name or canonical.vendor_name,
                    "institution_name": canonical.institution_name,
                    "vendor_name": canonical.vendor_name,
                    "institution_type": canonical.institution_type,
                    "updated_at": canonical.updated_at.isoformat() if canonical.updated_at else None,
                    "mismatch_fields": sorted(mismatches.keys()),
                    "mismatches": mismatches,
                }
            )
            if len(items) >= limit:
                break
        return items

    def queue_canonical_consistency_sweep(
        self,
        session: Session,
        *,
        limit: int = 100,
        scan_limit: int = 1000,
    ) -> dict[str, Any]:
        candidates = self.list_canonical_consistency_candidates(
            session,
            limit=limit,
            scan_limit=scan_limit,
        )
        canonicals_queued = 0
        queued_tasks = 0
        skipped_existing_tasks = 0
        for candidate in candidates:
            result = self.queue_recanonicalization_for_canonical(
                session,
                canonical_incident_id=str(candidate["canonical_incident_id"]),
            )
            if result.get("found"):
                canonicals_queued += 1
            queued_tasks += int(result.get("queued", 0) or 0)
            skipped_existing_tasks += int(result.get("skipped_existing", 0) or 0)
        return {
            "scan_limit": scan_limit,
            "candidates_considered": len(candidates),
            "canonicals_queued": canonicals_queued,
            "queued_tasks": queued_tasks,
            "skipped_existing_tasks": skipped_existing_tasks,
        }

    def requeue_dead_letter_tasks(
        self,
        session: Session,
        *,
        task_type: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, int | str | None]:
        requeued = self.pipeline_task_repository.requeue_dead_letters(
            session,
            task_type=task_type,
            limit=limit,
        )
        return {
            "task_type": task_type,
            "limit": limit,
            "requeued": requeued,
        }
