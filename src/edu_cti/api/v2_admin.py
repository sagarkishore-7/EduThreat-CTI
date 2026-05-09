"""Authenticated admin control surface for the Postgres-backed v2 runtime."""

from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from src.edu_cti.api.admin import authenticate
from src.edu_cti.api.v2 import get_v2_session, get_v2_session_factory
from src.edu_cti_v2.services import V2OperationsService
from src.edu_cti_v2.services.collection import V2CollectionService
from src.edu_cti_v2.services.orchestration import V2OrchestrationService
from src.edu_cti_v2.services.scheduler import V2SchedulerService

router = APIRouter(prefix="/admin/v2", tags=["Admin", "V2"])


@lru_cache
def get_v2_operations_service() -> V2OperationsService:
    return V2OperationsService(session_factory=get_v2_session_factory())


@lru_cache
def get_v2_collection_service() -> V2CollectionService:
    return V2CollectionService(session_factory=get_v2_session_factory())


@lru_cache
def get_v2_orchestration_service() -> V2OrchestrationService:
    return V2OrchestrationService(session_factory=get_v2_session_factory())


@lru_cache
def get_v2_scheduler_service() -> V2SchedulerService:
    return V2SchedulerService()


@router.get("/status")
async def get_v2_runtime_status(
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Return queue, run, and snapshot status for the v2 runtime."""
    return operations.get_runtime_status(session)


@router.get("/tasks")
async def list_v2_tasks(
    limit: int = Query(25, ge=1, le=200),
    task_type: Optional[str] = Query(None),
    status: Optional[List[str]] = Query(None),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """List recent v2 pipeline tasks."""
    return {
        "items": operations.list_tasks(
            session,
            limit=limit,
            task_type=task_type,
            statuses=tuple(status) if status else None,
        ),
        "meta": {
            "limit": limit,
            "task_type": task_type,
            "statuses": status or [],
        },
    }


@router.get("/runs")
async def list_v2_runs(
    limit: int = Query(20, ge=1, le=100),
    status: Optional[List[str]] = Query(None),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """List recent v2 worker runs."""
    return {
        "items": operations.list_runs(
            session,
            limit=limit,
            statuses=tuple(status) if status else None,
        ),
        "meta": {
            "limit": limit,
            "statuses": status or [],
        },
    }


@router.post("/worker/run")
async def run_v2_worker_batch(
    max_tasks: int = Query(25, ge=1, le=500),
    task_type: Optional[str] = Query(None),
    stop_when_idle: bool = Query(True),
    worker_id: str = Query("admin-v2"),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Run a bounded v2 worker batch synchronously and persist a run record."""
    return operations.run_worker_batch(
        worker_id=worker_id,
        task_type=task_type,
        max_tasks=max_tasks,
        stop_when_idle=stop_when_idle,
    )


@router.post("/collect")
async def run_v2_collection(
    groups: Optional[List[str]] = Query(None),
    sources: Optional[List[str]] = Query(None),
    max_pages: Optional[int] = Query(None, ge=1),
    rss_max_age_days: int = Query(30, ge=1, le=3650),
    incremental: bool = Query(True),
    include_paid_rss: bool = Query(False),
    collection: V2CollectionService = Depends(get_v2_collection_service),
    _: bool = Depends(authenticate),
):
    """Collect fresh raw source observations directly into v2/Postgres."""
    return collection.collect_into_v2(
        groups=groups,
        sources=sources,
        max_pages=max_pages,
        rss_max_age_days=rss_max_age_days,
        incremental=incremental,
        include_paid_rss=include_paid_rss,
    )


@router.get("/plans")
async def list_v2_plans(
    orchestration: V2OrchestrationService = Depends(get_v2_orchestration_service),
    _: bool = Depends(authenticate),
):
    """List supported named v2 orchestration plans."""
    return {"items": orchestration.list_plans()}


@router.post("/run-plan")
async def run_v2_plan(
    plan_name: str = Query(...),
    worker_id: str = Query("admin-v2-plan"),
    worker_max_tasks: Optional[int] = Query(None, ge=1, le=20000),
    drain_tasks: Optional[bool] = Query(None),
    include_paid_rss: Optional[bool] = Query(None),
    orchestration: V2OrchestrationService = Depends(get_v2_orchestration_service),
    _: bool = Depends(authenticate),
):
    """Run a named v2 plan that bundles collection and optional task draining."""
    collect_overrides = {}
    if include_paid_rss is not None:
        collect_overrides["include_paid_rss"] = include_paid_rss
    return orchestration.run_plan(
        plan_name=plan_name,
        worker_id=worker_id,
        collect_overrides=collect_overrides or None,
        worker_max_tasks=worker_max_tasks,
        drain_tasks=drain_tasks,
    )


@router.get("/scheduler/status")
async def get_v2_scheduler_status(
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Return recurring v2 scheduler status and recent outcomes."""
    return scheduler.get_status()


@router.post("/scheduler/start")
async def start_v2_scheduler(
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Start the recurring v2 scheduler in-process."""
    return scheduler.start()


@router.post("/scheduler/stop")
async def stop_v2_scheduler(
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Stop the recurring v2 scheduler."""
    return scheduler.stop()


@router.post("/scheduler/trigger/{job_name}")
async def trigger_v2_scheduler_job(
    job_name: str,
    background: bool = Query(True),
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Trigger one named recurring v2 scheduler job on demand."""
    return scheduler.trigger_job(job_name, background=background)
