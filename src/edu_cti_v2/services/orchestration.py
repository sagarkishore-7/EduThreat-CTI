"""Named orchestration plans for running the v2 Postgres pipeline end to end."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional
from uuid import uuid4

from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.models import PipelineRun
from src.edu_cti_v2.repositories import PipelineRunRepository
from src.edu_cti_v2.services.collection import V2CollectionService
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
}


class V2OrchestrationService:
    """Run named v2 collection + worker-drain plans."""

    def __init__(
        self,
        *,
        session_factory: Optional[Callable] = None,
        collection_service: Optional[V2CollectionService] = None,
        operations_service: Optional[V2OperationsService] = None,
        pipeline_run_repository: Optional[PipelineRunRepository] = None,
    ) -> None:
        self.session_factory = session_factory or create_session_factory()
        self.collection_service = collection_service or V2CollectionService(session_factory=self.session_factory)
        self.operations_service = operations_service or V2OperationsService(session_factory=self.session_factory)
        self.pipeline_run_repository = pipeline_run_repository or PipelineRunRepository()

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
            }
            for plan in _PLAN_DEFINITIONS.values()
        ]

    def run_plan(
        self,
        *,
        plan_name: str,
        worker_id: str = "admin-v2-plan",
        collect_overrides: Optional[dict[str, Any]] = None,
        worker_max_tasks: Optional[int] = None,
        drain_tasks: Optional[bool] = None,
    ) -> dict[str, Any]:
        if plan_name not in _PLAN_DEFINITIONS:
            raise ValueError(f"Unknown v2 plan: {plan_name}")

        plan = _PLAN_DEFINITIONS[plan_name]
        collect_kwargs = {**plan.collect_kwargs, **(collect_overrides or {})}
        should_drain = plan.drain_tasks if drain_tasks is None else drain_tasks
        effective_worker_max_tasks = worker_max_tasks or plan.worker_max_tasks

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
            if should_drain:
                worker_result = self.operations_service.run_worker_batch(
                    worker_id=worker_id,
                    task_type=plan.worker_task_type,
                    max_tasks=effective_worker_max_tasks,
                    stop_when_idle=plan.worker_stop_when_idle,
                )

            result = {
                "run_id": str(run_id),
                "plan_name": plan_name,
                "collect_result": collect_result,
                "worker_result": worker_result,
            }
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
