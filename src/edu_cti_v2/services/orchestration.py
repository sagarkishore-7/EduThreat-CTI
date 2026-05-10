"""Named orchestration plans for running the v2 Postgres pipeline end to end."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional
from uuid import uuid4

from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.models import PipelineRun, PipelineTask
from src.edu_cti_v2.repositories import PipelineRunRepository, PipelineTaskRepository
from src.edu_cti_v2.services.collection import V2CollectionService
from src.edu_cti_v2.services.data_quality import V2DataQualityService
from src.edu_cti_v2.services.research_metrics import V2ResearchMetricsService

if TYPE_CHECKING:
    from src.edu_cti_v2.services.operations import V2OperationsService


@dataclass(frozen=True)
class V2PlanDefinition:
    name: str
    description: str
    collect_kwargs: dict[str, Any]
    drain_tasks: bool = True
    worker_task_type: str | None = None
    worker_max_tasks: int = 500
    worker_stop_when_idle: bool = True
    run_data_quality_sweep: bool = False
    reenrich_worker_max_tasks: int = 250
    run_canonical_consistency_sweep: bool = False
    canonical_consistency_limit: int = 100
    canonical_consistency_scan_limit: int = 1000
    capture_research_metrics: bool = False


_PLAN_DEFINITIONS: dict[str, V2PlanDefinition] = {
    "historical_full": V2PlanDefinition(
        name="historical_full",
        description="Collect all source groups in full-historical mode and drain the v2 task queue.",
        collect_kwargs={
            "groups": ["curated", "news", "rss", "api"],
            "incremental": False,
            "include_paid_rss": False,
            "max_pages": None,
            "rss_max_age_days": 3650,
        },
        drain_tasks=True,
        worker_task_type=None,
        worker_max_tasks=5000,
        run_data_quality_sweep=True,
        reenrich_worker_max_tasks=1000,
        run_canonical_consistency_sweep=True,
        canonical_consistency_limit=250,
        canonical_consistency_scan_limit=2000,
        capture_research_metrics=True,
    ),
    "historical_max_coverage": V2PlanDefinition(
        name="historical_max_coverage",
        description="Historical run with paid RSS/search sources enabled for maximum coverage.",
        collect_kwargs={
            "groups": ["curated", "news", "rss", "api"],
            "incremental": False,
            "include_paid_rss": True,
            "max_pages": None,
            "rss_max_age_days": 3650,
        },
        drain_tasks=True,
        worker_task_type=None,
        worker_max_tasks=5000,
        run_data_quality_sweep=True,
        reenrich_worker_max_tasks=1000,
        run_canonical_consistency_sweep=True,
        canonical_consistency_limit=250,
        canonical_consistency_scan_limit=2000,
        capture_research_metrics=True,
    ),
    "incremental_refresh": V2PlanDefinition(
        name="incremental_refresh",
        description="Incremental refresh across all groups followed by a bounded drain of the v2 task queue.",
        collect_kwargs={
            "groups": ["curated", "news", "rss", "api"],
            "incremental": True,
            "include_paid_rss": False,
            "max_pages": 20,
            "rss_max_age_days": 30,
        },
        drain_tasks=True,
        worker_task_type=None,
        worker_max_tasks=1000,
        capture_research_metrics=True,
    ),
    "rss_fast_refresh": V2PlanDefinition(
        name="rss_fast_refresh",
        description="Quick incremental RSS-only refresh followed by queue draining.",
        collect_kwargs={
            "groups": ["rss"],
            "incremental": True,
            "include_paid_rss": False,
            "max_pages": None,
            "rss_max_age_days": 30,
        },
        drain_tasks=True,
        worker_task_type=None,
        worker_max_tasks=300,
        capture_research_metrics=True,
    ),
    "collect_only": V2PlanDefinition(
        name="collect_only",
        description="Collect all groups incrementally without draining worker tasks.",
        collect_kwargs={
            "groups": ["curated", "news", "rss", "api"],
            "incremental": True,
            "include_paid_rss": False,
            "max_pages": 20,
            "rss_max_age_days": 30,
        },
        drain_tasks=False,
    ),
    "daily_quality_refresh": V2PlanDefinition(
        name="daily_quality_refresh",
        description="Daily incremental refresh followed by a data-quality sweep and re-enrichment pass.",
        collect_kwargs={
            "groups": ["curated", "news", "rss", "api"],
            "incremental": True,
            "include_paid_rss": False,
            "max_pages": 20,
            "rss_max_age_days": 30,
        },
        drain_tasks=True,
        worker_task_type=None,
        worker_max_tasks=1200,
        run_data_quality_sweep=True,
        reenrich_worker_max_tasks=600,
        run_canonical_consistency_sweep=True,
        canonical_consistency_limit=150,
        canonical_consistency_scan_limit=1000,
        capture_research_metrics=True,
    ),
}


class V2OrchestrationService:
    """Run named v2 collection + worker-drain plans."""

    def __init__(
        self,
        *,
        session_factory: Optional[Callable] = None,
        collection_service: Optional[V2CollectionService] = None,
        operations_service: Optional["V2OperationsService"] = None,
        data_quality_service: Optional[V2DataQualityService] = None,
        research_metrics_service: Optional[V2ResearchMetricsService] = None,
        pipeline_run_repository: Optional[PipelineRunRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.session_factory = session_factory or create_session_factory()
        self.collection_service = collection_service or V2CollectionService(session_factory=self.session_factory)
        if operations_service is None:
            from src.edu_cti_v2.services.operations import V2OperationsService

            operations_service = V2OperationsService(session_factory=self.session_factory)
        self.operations_service = operations_service
        self.data_quality_service = data_quality_service or V2DataQualityService(session_factory=self.session_factory)
        self.research_metrics_service = research_metrics_service or V2ResearchMetricsService()
        self.pipeline_run_repository = pipeline_run_repository or PipelineRunRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def list_plans(self) -> list[dict[str, Any]]:
        return [
            {
                "name": plan.name,
                "description": plan.description,
                "collect_kwargs": plan.collect_kwargs,
                "drain_tasks": plan.drain_tasks,
                "worker_task_type": plan.worker_task_type,
                "worker_max_tasks": plan.worker_max_tasks,
                "worker_stop_when_idle": plan.worker_stop_when_idle,
                "run_data_quality_sweep": plan.run_data_quality_sweep,
                "reenrich_worker_max_tasks": plan.reenrich_worker_max_tasks,
                "run_canonical_consistency_sweep": plan.run_canonical_consistency_sweep,
                "canonical_consistency_limit": plan.canonical_consistency_limit,
                "canonical_consistency_scan_limit": plan.canonical_consistency_scan_limit,
                "capture_research_metrics": plan.capture_research_metrics,
            }
            for plan in _PLAN_DEFINITIONS.values()
        ]

    def _resolve_request(
        self,
        *,
        plan_name: str,
        collect_overrides: Optional[dict[str, Any]] = None,
        worker_max_tasks: Optional[int] = None,
        drain_tasks: Optional[bool] = None,
    ) -> tuple[V2PlanDefinition, dict[str, Any], bool, int]:
        if plan_name not in _PLAN_DEFINITIONS:
            raise ValueError(f"Unknown v2 plan: {plan_name}")
        plan = _PLAN_DEFINITIONS[plan_name]
        collect_kwargs = {**plan.collect_kwargs, **(collect_overrides or {})}
        should_drain = plan.drain_tasks if drain_tasks is None else drain_tasks
        effective_worker_max_tasks = worker_max_tasks or plan.worker_max_tasks
        return plan, collect_kwargs, should_drain, effective_worker_max_tasks

    def enqueue_plan(
        self,
        *,
        plan_name: str,
        worker_id: str = "admin-v2-plan",
        collect_overrides: Optional[dict[str, Any]] = None,
        worker_max_tasks: Optional[int] = None,
        drain_tasks: Optional[bool] = None,
    ) -> dict[str, Any]:
        plan, collect_kwargs, should_drain, effective_worker_max_tasks = self._resolve_request(
            plan_name=plan_name,
            collect_overrides=collect_overrides,
            worker_max_tasks=worker_max_tasks,
            drain_tasks=drain_tasks,
        )

        with self.session_factory() as session:
            run = PipelineRun(
                run_type="maintenance",
                status="pending",
                service_name="v2-plan-orchestrator",
                params={
                    "plan_name": plan_name,
                    "collect_kwargs": collect_kwargs,
                    "drain_tasks": should_drain,
                    "worker_max_tasks": effective_worker_max_tasks,
                    "worker_id": worker_id,
                    "execution_mode": "queued",
                    "run_canonical_consistency_sweep": plan.run_canonical_consistency_sweep,
                    "canonical_consistency_limit": plan.canonical_consistency_limit,
                    "canonical_consistency_scan_limit": plan.canonical_consistency_scan_limit,
                    "capture_research_metrics": plan.capture_research_metrics,
                },
                result={},
            )
            if run.id is None:
                run.id = uuid4()
            self.pipeline_run_repository.add(session, run)
            task = PipelineTask(
                run_id=run.id,
                task_type="orchestrate_plan",
                target_table="pipeline_runs",
                target_id=run.id,
                status="queued",
                priority=1000,
                payload={
                    "plan_name": plan_name,
                    "collect_kwargs": collect_kwargs,
                    "drain_tasks": should_drain,
                    "worker_max_tasks": effective_worker_max_tasks,
                    "worker_id": worker_id,
                    "run_data_quality_sweep": plan.run_data_quality_sweep,
                    "reenrich_worker_max_tasks": plan.reenrich_worker_max_tasks,
                    "run_canonical_consistency_sweep": plan.run_canonical_consistency_sweep,
                    "canonical_consistency_limit": plan.canonical_consistency_limit,
                    "canonical_consistency_scan_limit": plan.canonical_consistency_scan_limit,
                    "capture_research_metrics": plan.capture_research_metrics,
                },
                result={},
                available_at=datetime.now(timezone.utc),
                attempt_count=0,
                max_attempts=3,
            )
            self.pipeline_task_repository.enqueue(session, task)
            flush = getattr(session, "flush", None)
            if callable(flush):
                flush()
            session.commit()
            return {
                "run_id": str(run.id),
                "task_id": str(task.id),
                "plan_name": plan_name,
                "status": "queued",
                "worker_id": worker_id,
                "drain_tasks": should_drain,
                "worker_max_tasks": effective_worker_max_tasks,
                "collect_kwargs": collect_kwargs,
            }

    def _wait_for_queue_to_drain(
        self,
        *,
        exclude_task_id,
        poll_interval_seconds: float = 5.0,
    ) -> dict[str, Any]:
        polls = 0
        while True:
            with self.session_factory() as session:
                active_count = self.pipeline_task_repository.count_active(
                    session,
                    exclude_task_types=("orchestrate_plan",),
                    exclude_task_ids=(exclude_task_id,),
                )
            if active_count <= 0:
                return {
                    "status": "drained",
                    "polls": polls,
                    "active_task_count": 0,
                }
            polls += 1
            time.sleep(max(poll_interval_seconds, 0.0))

    def execute_enqueued_plan(self, task: PipelineTask, *, worker_id: str) -> dict[str, Any]:
        payload = task.payload or {}
        plan_name = payload["plan_name"]
        plan, collect_kwargs, should_drain, _ = self._resolve_request(
            plan_name=plan_name,
            collect_overrides=payload.get("collect_kwargs"),
            worker_max_tasks=payload.get("worker_max_tasks"),
            drain_tasks=payload.get("drain_tasks"),
        )
        run_id = task.run_id

        with self.session_factory() as session:
            persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
            if persisted_run is None:
                raise ValueError(f"Plan run not found for queued task: {run_id}")
            if persisted_run.status != "running":
                self.pipeline_run_repository.mark_started(session, persisted_run)
                session.commit()

        try:
            collect_result = self.collection_service.collect_into_v2(
                **collect_kwargs,
                persist_run=False,
            )
            worker_result = None
            data_quality_result = None
            reenrich_worker_result = None
            consistency_sweep_result = None
            consistency_worker_result = None
            research_metrics_result = None

            if should_drain:
                worker_result = self._wait_for_queue_to_drain(exclude_task_id=task.id)
            if plan.run_data_quality_sweep:
                data_quality_result = self.data_quality_service.run_sweep()
                if data_quality_result.get("requeued_for_reenrichment") and should_drain:
                    reenrich_worker_result = self._wait_for_queue_to_drain(exclude_task_id=task.id)
            if plan.run_canonical_consistency_sweep:
                with self.session_factory() as session:
                    consistency_sweep_result = self.operations_service.queue_canonical_consistency_sweep(
                        session,
                        limit=plan.canonical_consistency_limit,
                        scan_limit=plan.canonical_consistency_scan_limit,
                    )
                    session.commit()
                if consistency_sweep_result.get("queued_tasks") and should_drain:
                    consistency_worker_result = self._wait_for_queue_to_drain(exclude_task_id=task.id)
            with self.session_factory() as session:
                if plan.capture_research_metrics:
                    research_metrics_result = self.research_metrics_service.capture_snapshot(
                        session,
                        snapshot_key="global",
                        snapshot_scope="global",
                        run_id=run_id,
                        trigger={
                            "source": "queued_plan",
                            "plan_name": plan_name,
                        },
                    )
                result = {
                    "run_id": str(run_id),
                    "plan_name": plan_name,
                    "execution_mode": "queued",
                    "worker_id": worker_id,
                    "collect_result": collect_result,
                    "worker_result": worker_result,
                    "data_quality_result": data_quality_result,
                    "reenrich_worker_result": reenrich_worker_result,
                    "consistency_sweep_result": consistency_sweep_result,
                    "consistency_worker_result": consistency_worker_result,
                    "research_metrics_result": research_metrics_result,
                }
                persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
                if persisted_run is not None:
                    self.pipeline_run_repository.mark_finished(
                        session,
                        persisted_run,
                        status="completed",
                        result=result,
                    )
                    session.commit()
            return result
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

    def run_plan(
        self,
        *,
        plan_name: str,
        worker_id: str = "admin-v2-plan",
        collect_overrides: Optional[dict[str, Any]] = None,
        worker_max_tasks: Optional[int] = None,
        drain_tasks: Optional[bool] = None,
    ) -> dict[str, Any]:
        plan, collect_kwargs, should_drain, effective_worker_max_tasks = self._resolve_request(
            plan_name=plan_name,
            collect_overrides=collect_overrides,
            worker_max_tasks=worker_max_tasks,
            drain_tasks=drain_tasks,
        )

        with self.session_factory() as session:
            run = PipelineRun(
                run_type="maintenance",
                status="pending",
                service_name="v2-plan-orchestrator",
                params={
                    "plan_name": plan_name,
                    "collect_kwargs": collect_kwargs,
                    "drain_tasks": should_drain,
                    "worker_max_tasks": effective_worker_max_tasks,
                    "worker_id": worker_id,
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
            collect_result = self.collection_service.collect_into_v2(**collect_kwargs)
            worker_result = None
            data_quality_result = None
            reenrich_worker_result = None
            consistency_sweep_result = None
            consistency_worker_result = None
            research_metrics_result = None
            if should_drain:
                worker_result = self.operations_service.run_worker_batch(
                    worker_id=worker_id,
                    task_type=plan.worker_task_type,
                    max_tasks=effective_worker_max_tasks,
                    stop_when_idle=plan.worker_stop_when_idle,
                )
            if plan.run_data_quality_sweep:
                data_quality_result = self.data_quality_service.run_sweep()
                if data_quality_result.get("requeued_for_reenrichment"):
                    reenrich_worker_result = self.operations_service.run_worker_batch(
                        worker_id=f"{worker_id}:reenrich",
                        task_type="reenrich",
                        max_tasks=plan.reenrich_worker_max_tasks,
                        stop_when_idle=True,
                    )
            if plan.run_canonical_consistency_sweep:
                with self.session_factory() as session:
                    consistency_sweep_result = self.operations_service.queue_canonical_consistency_sweep(
                        session,
                        limit=plan.canonical_consistency_limit,
                        scan_limit=plan.canonical_consistency_scan_limit,
                    )
                    session.commit()
                if consistency_sweep_result.get("queued_tasks"):
                    consistency_worker_result = self.operations_service.run_worker_batch(
                        worker_id=f"{worker_id}:consistency",
                        task_type="canonicalize",
                        max_tasks=plan.canonical_consistency_limit * 10,
                        stop_when_idle=True,
                    )
            with self.session_factory() as session:
                if plan.capture_research_metrics:
                    research_metrics_result = self.research_metrics_service.capture_snapshot(
                        session,
                        snapshot_key="global",
                        snapshot_scope="global",
                        run_id=run_id,
                        trigger={
                            "source": "direct_plan",
                            "plan_name": plan_name,
                        },
                    )
                result = {
                    "run_id": str(run_id),
                    "plan_name": plan_name,
                    "collect_result": collect_result,
                    "worker_result": worker_result,
                    "data_quality_result": data_quality_result,
                    "reenrich_worker_result": reenrich_worker_result,
                    "consistency_sweep_result": consistency_sweep_result,
                    "consistency_worker_result": consistency_worker_result,
                    "research_metrics_result": research_metrics_result,
                }
                persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
                if persisted_run is not None:
                    self.pipeline_run_repository.mark_finished(
                        session,
                        persisted_run,
                        status="completed",
                        result=result,
                    )
                    session.commit()
            return result
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
