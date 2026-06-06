"""Authenticated admin control surface for the Postgres-backed v2 runtime."""

from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from src.edu_cti.api.v2 import get_v2_session, get_v2_session_factory
from src.edu_cti_v2.auth import (
    V2LoginRequest,
    V2LoginResponse,
    authenticate,
    create_session_token,
    revoke_session,
    verify_password,
)
from fastapi.responses import Response

from src.edu_cti_v2.services import (
    V2DataQualityService,
    V2OperationsService,
    V2PreflightService,
    V2ResearchMetricsService,
    V2SourceHealthService,
)
from src.edu_cti_v2.services.campaigns import ADMIN_CAMPAIGN_STATUSES, V2CampaignService
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


@lru_cache
def get_v2_preflight_service() -> V2PreflightService:
    return V2PreflightService()


@lru_cache
def get_v2_data_quality_service() -> V2DataQualityService:
    return V2DataQualityService(session_factory=get_v2_session_factory())


@lru_cache
def get_v2_research_metrics_service() -> V2ResearchMetricsService:
    return V2ResearchMetricsService()


@lru_cache
def get_v2_source_health_service() -> V2SourceHealthService:
    return V2SourceHealthService()


@lru_cache
def get_v2_campaign_service() -> V2CampaignService:
    return V2CampaignService()


@router.post("/login", response_model=V2LoginResponse)
async def v2_admin_login(request: V2LoginRequest):
    """Login endpoint for the dedicated v2 admin surface."""
    from src.edu_cti_v2.auth import ADMIN_USERNAME

    if request.username != ADMIN_USERNAME:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not verify_password(request.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    session_token, expires_at = create_session_token()
    return V2LoginResponse(
        success=True,
        session_token=session_token,
        expires_at=expires_at.isoformat(),
        message="Login successful",
    )


@router.post("/logout")
async def v2_admin_logout(
    x_session_token: Optional[str] = None,
    _: bool = Depends(authenticate),
):
    revoke_session(x_session_token)
    return {"success": True, "message": "Logged out"}


@router.get("/status")
async def get_v2_runtime_status(
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Return queue, run, and snapshot status for the v2 runtime."""
    return operations.get_runtime_status(session)


@router.get("/preflight")
async def get_v2_preflight_status(
    session=Depends(get_v2_session),
    preflight: V2PreflightService = Depends(get_v2_preflight_service),
    _: bool = Depends(authenticate),
):
    """Check whether the dedicated v2 runtime is ready for a fresh Postgres run."""
    return preflight.get_status(session)


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


@router.get("/metrics/research")
async def get_v2_research_metrics(
    session=Depends(get_v2_session),
    research_service: V2ResearchMetricsService = Depends(get_v2_research_metrics_service),
    _: bool = Depends(authenticate),
):
    """Return the latest persisted or live research-grade pipeline metrics."""
    return research_service.get_latest_or_live(session)


@router.get("/metrics/research/history")
async def list_v2_research_metric_history(
    limit: int = Query(20, ge=1, le=200),
    snapshot_key: str = Query("global"),
    session=Depends(get_v2_session),
    research_service: V2ResearchMetricsService = Depends(get_v2_research_metrics_service),
    _: bool = Depends(authenticate),
):
    """List recent persisted research-metrics snapshots across runs."""
    items = research_service.list_recent_snapshots(
        session,
        snapshot_key=snapshot_key,
        snapshot_scope="global",
        limit=limit,
    )
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "snapshot_key": snapshot_key,
            "returned": len(items),
        },
    }


@router.post("/metrics/research/refresh")
async def refresh_v2_research_metrics(
    session=Depends(get_v2_session),
    research_service: V2ResearchMetricsService = Depends(get_v2_research_metrics_service),
    _: bool = Depends(authenticate),
):
    """Persist a fresh research-metrics snapshot for the current v2 dataset state."""
    payload = research_service.capture_snapshot(
        session,
        snapshot_key="global",
        snapshot_scope="global",
        trigger={"source": "admin_refresh"},
    )
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return payload


@router.get("/metrics/research/prometheus")
async def get_v2_research_metrics_prometheus(
    session=Depends(get_v2_session),
    research_service: V2ResearchMetricsService = Depends(get_v2_research_metrics_service),
    _: bool = Depends(authenticate),
):
    """Return the latest research metrics in Prometheus text format."""
    payload = research_service.get_latest_or_live(session)
    return Response(
        content=research_service.render_prometheus_text(payload),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.get("/source-health")
async def get_v2_source_health(
    sample_limit: int = Query(25, ge=1, le=200),
    session=Depends(get_v2_session),
    source_health: V2SourceHealthService = Depends(get_v2_source_health_service),
    _: bool = Depends(authenticate),
):
    """Return a read-only source coverage, fetch, enrichment, and quality audit."""
    return source_health.get_source_health(session, sample_limit=sample_limit)


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
    include_paid_rss: Optional[bool] = Query(None),
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
    background: bool = Query(True),
    include_paid_rss: Optional[bool] = Query(None),
    orchestration: V2OrchestrationService = Depends(get_v2_orchestration_service),
    _: bool = Depends(authenticate),
):
    """Run a named v2 plan that bundles collection and optional task draining."""
    collect_overrides = {}
    if include_paid_rss is not None:
        collect_overrides["include_paid_rss"] = include_paid_rss
    if background:
        return orchestration.enqueue_plan(
            plan_name=plan_name,
            worker_id=worker_id,
            collect_overrides=collect_overrides or None,
            worker_max_tasks=worker_max_tasks,
            drain_tasks=drain_tasks,
        )
    return orchestration.run_plan(
        plan_name=plan_name,
        worker_id=worker_id,
        collect_overrides=collect_overrides or None,
        worker_max_tasks=worker_max_tasks,
        drain_tasks=drain_tasks,
    )


@router.post("/data-quality/sweep-now")
async def run_v2_data_quality_sweep(
    limit: Optional[int] = Query(None, ge=1, le=50000),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Sweep v2 source enrichments for invalid dates and headline-style institutions."""
    return data_quality.run_sweep(limit=limit)


@router.post("/data-quality/promote-drift-candidates")
async def promote_v2_drift_candidates(
    limit: int = Query(500, ge=1, le=10000),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Promote useful unselected fallback articles into independent source candidates."""
    return data_quality.run_drift_promotion_sweep(limit=limit)


@router.get("/campaigns")
async def list_v2_admin_campaigns(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0, le=100000),
    status: Optional[List[str]] = Query(None),
    campaign_type: Optional[str] = Query(None),
    vendor: Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    actor: Optional[str] = Query(None),
    cve: Optional[str] = Query(None),
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    q: Optional[str] = Query(None, min_length=1, max_length=200),
    session=Depends(get_v2_session),
    campaign_service: V2CampaignService = Depends(get_v2_campaign_service),
    _: bool = Depends(authenticate),
):
    """List campaign hypotheses, including analyst-only candidates."""
    statuses = tuple(status) if status else ADMIN_CAMPAIGN_STATUSES
    return campaign_service.list_campaigns(
        session,
        statuses=statuses,
        campaign_type=campaign_type,
        vendor=vendor,
        platform=platform,
        actor=actor,
        cve=cve,
        min_confidence=min_confidence,
        q=q,
        limit=limit,
        offset=offset,
    )


@router.get("/campaigns/{campaign_id}")
async def get_v2_admin_campaign_detail(
    campaign_id: str,
    member_limit: int = Query(500, ge=1, le=5000),
    evidence_limit: int = Query(1000, ge=1, le=10000),
    session=Depends(get_v2_session),
    campaign_service: V2CampaignService = Depends(get_v2_campaign_service),
    _: bool = Depends(authenticate),
):
    """Return one campaign hypothesis with full member and evidence detail."""
    detail = campaign_service.get_campaign_detail(
        session,
        campaign_id,
        statuses=ADMIN_CAMPAIGN_STATUSES,
        member_limit=member_limit,
        evidence_limit=evidence_limit,
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return detail


@router.get("/campaigns/{campaign_id}/graph")
async def get_v2_admin_campaign_graph(
    campaign_id: str,
    member_limit: int = Query(250, ge=1, le=2000),
    session=Depends(get_v2_session),
    campaign_service: V2CampaignService = Depends(get_v2_campaign_service),
    _: bool = Depends(authenticate),
):
    """Return graph-ready nodes and edges for one campaign hypothesis."""
    graph = campaign_service.get_campaign_graph(
        session,
        campaign_id,
        statuses=ADMIN_CAMPAIGN_STATUSES,
        member_limit=member_limit,
    )
    if graph is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return graph


@router.post("/campaigns/correlate")
async def run_v2_campaign_correlation(
    background: bool = Query(True),
    include_excluded: bool = Query(True),
    limit: Optional[int] = Query(None, ge=1, le=100000),
    session=Depends(get_v2_session),
    campaign_service: V2CampaignService = Depends(get_v2_campaign_service),
    _: bool = Depends(authenticate),
):
    """Run or enqueue deterministic production campaign correlation."""
    if background:
        result = campaign_service.enqueue_correlation(
            session,
            include_excluded=include_excluded,
            limit=limit,
        )
    else:
        result = campaign_service.run_correlation(
            session,
            include_excluded=include_excluded,
            limit=limit,
        )
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.patch("/campaigns/{campaign_id}")
async def update_v2_admin_campaign_review(
    campaign_id: str,
    payload: dict = Body(default_factory=dict),
    session=Depends(get_v2_session),
    campaign_service: V2CampaignService = Depends(get_v2_campaign_service),
    _: bool = Depends(authenticate),
):
    """Update analyst campaign status, pinned name, summary, or notes."""
    result = campaign_service.update_campaign_review(
        session,
        campaign_id,
        status=payload.get("status"),
        campaign_name=payload.get("campaign_name"),
        analyst_summary=payload.get("analyst_summary"),
        analyst_notes=payload.get("analyst_notes"),
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.patch("/campaigns/{campaign_id}/members/{canonical_incident_id}")
async def update_v2_admin_campaign_membership_review(
    campaign_id: str,
    canonical_incident_id: str,
    payload: dict = Body(default_factory=dict),
    session=Depends(get_v2_session),
    campaign_service: V2CampaignService = Depends(get_v2_campaign_service),
    _: bool = Depends(authenticate),
):
    """Update analyst review status or role for one campaign membership."""
    review_status = payload.get("review_status")
    if not review_status:
        raise HTTPException(status_code=400, detail="review_status is required")
    result = campaign_service.update_membership_review(
        session,
        campaign_id,
        canonical_incident_id,
        review_status=review_status,
        role=payload.get("role"),
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Campaign membership not found")
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.post("/canonicalize/sweep-now")
async def queue_v2_recanonicalization_sweep(
    limit: int = Query(500, ge=1, le=50000),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Queue a bounded canonicalization sweep for already-enriched v2 source incidents."""
    result = operations.queue_recanonicalization_sweep(session, limit=limit)
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.post("/canonicalize/by-canonical/{canonical_incident_id}")
async def queue_v2_recanonicalization_for_canonical(
    canonical_incident_id: str,
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Queue recanonicalization for all current members of one canonical incident."""
    result = operations.queue_recanonicalization_for_canonical(
        session,
        canonical_incident_id=canonical_incident_id,
    )
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.get("/canonicalize/consistency-candidates")
async def list_v2_canonical_consistency_candidates(
    limit: int = Query(100, ge=1, le=1000),
    scan_limit: int = Query(1000, ge=1, le=10000),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """List canonicals whose top-level fields diverge from authoritative analytics projection."""
    items = operations.list_canonical_consistency_candidates(
        session,
        limit=limit,
        scan_limit=scan_limit,
    )
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "scan_limit": scan_limit,
            "returned": len(items),
        },
    }


@router.post("/canonicalize/consistency-sweep-now")
async def queue_v2_canonical_consistency_sweep(
    limit: int = Query(100, ge=1, le=1000),
    scan_limit: int = Query(1000, ge=1, le=10000),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Queue recanonicalization for canonicals with detected projection drift."""
    result = operations.queue_canonical_consistency_sweep(
        session,
        limit=limit,
        scan_limit=scan_limit,
    )
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.post("/tasks/requeue-dead-letter")
async def requeue_v2_dead_letter_tasks(
    task_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Requeue dead-letter v2 tasks after a code or schema fix is deployed."""
    result = operations.requeue_dead_letter_tasks(
        session,
        task_type=task_type,
        limit=limit,
    )
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.get("/manual-review-queue")
async def list_v2_manual_review_queue(
    limit: int = Query(100, ge=1, le=1000),
    session=Depends(get_v2_session),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """List v2 enrichments that exhausted automatic re-enrichment attempts."""
    items = data_quality.list_manual_review_queue(session, limit=limit)
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "returned": len(items),
        },
    }


@router.get("/rejected-enrichments")
async def list_v2_rejected_enrichments(
    limit: int = Query(100, ge=1, le=1000),
    session=Depends(get_v2_session),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """List v2 enrichments that were hard-rejected as non-canonicalizable."""
    items = data_quality.list_rejected_enrichments(session, limit=limit)
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "returned": len(items),
        },
    }


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
