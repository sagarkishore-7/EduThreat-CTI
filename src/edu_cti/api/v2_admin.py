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
def v2_admin_login(request: V2LoginRequest):
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
def v2_admin_logout(
    x_session_token: Optional[str] = None,
    _: bool = Depends(authenticate),
):
    revoke_session(x_session_token)
    return {"success": True, "message": "Logged out"}


@router.get("/status")
def get_v2_runtime_status(
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Return queue, run, and snapshot status for the v2 runtime."""
    return operations.get_runtime_status(session)


@router.get("/preflight")
def get_v2_preflight_status(
    session=Depends(get_v2_session),
    preflight: V2PreflightService = Depends(get_v2_preflight_service),
    _: bool = Depends(authenticate),
):
    """Check whether the dedicated v2 runtime is ready for a fresh Postgres run."""
    return preflight.get_status(session)


@router.get("/tasks")
def list_v2_tasks(
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
def list_v2_runs(
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
def get_v2_research_metrics(
    session=Depends(get_v2_session),
    research_service: V2ResearchMetricsService = Depends(get_v2_research_metrics_service),
    _: bool = Depends(authenticate),
):
    """Return the latest persisted or live research-grade pipeline metrics."""
    return research_service.get_latest_or_live(session)


@router.get("/metrics/research/history")
def list_v2_research_metric_history(
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
def refresh_v2_research_metrics(
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
def get_v2_research_metrics_prometheus(
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
def get_v2_source_health(
    sample_limit: int = Query(25, ge=1, le=200),
    session=Depends(get_v2_session),
    source_health: V2SourceHealthService = Depends(get_v2_source_health_service),
    _: bool = Depends(authenticate),
):
    """Return a read-only source coverage, fetch, enrichment, and quality audit."""
    return source_health.get_source_health(session, sample_limit=sample_limit)


@router.get("/data-quality/unrecognized-vendors")
def get_v2_unrecognized_vendors(
    limit: int = Query(100, ge=1, le=500),
    session=Depends(get_v2_session),
    source_health: V2SourceHealthService = Depends(get_v2_source_health_service),
    _: bool = Depends(authenticate),
):
    """Read-only self-audit of vendor strings matching no platform-indicator registry line."""
    return source_health.get_unrecognized_vendors(session, limit=limit)


@router.post("/worker/run")
def run_v2_worker_batch(
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
def run_v2_collection(
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
def list_v2_plans(
    orchestration: V2OrchestrationService = Depends(get_v2_orchestration_service),
    _: bool = Depends(authenticate),
):
    """List supported named v2 orchestration plans."""
    return {"items": orchestration.list_plans()}


@router.post("/run-plan")
def run_v2_plan(
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
def run_v2_data_quality_sweep(
    limit: Optional[int] = Query(None, ge=1, le=50000),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Sweep v2 source enrichments for invalid dates and headline-style institutions."""
    return data_quality.run_sweep(limit=limit)


@router.post("/data-quality/recover-curated")
def recover_v2_curated_parked(
    limit: Optional[int] = Query(None, ge=1, le=50000),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Requeue parked curated/api incidents (manual-review + rejected) for re-enrichment.

    The quality sweep skips manual-review and rejected rows by design, so legacy
    curated/api incidents the OLD enricher wrongly parked or rejected never recover
    on their own. This re-runs the current enricher on them (Path-A keeps a structured
    victim despite a weak article; gate-2 still re-rejects a genuine non-edu row).
    One-time recovery backfill; idempotent (skips rows with an active reenrich task).
    """
    return data_quality.run_curated_recovery(limit=limit)


@router.post("/data-quality/normalize-actors")
def run_v2_actor_normalization(
    limit: Optional[int] = Query(None, ge=1, le=200000),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Re-apply actor normalization to stored canonical threat_actor_name values.

    Nulls generic/junk labels and collapses aliases to canonical form. Idempotent."""
    return data_quality.run_actor_normalization(limit=limit)


@router.post("/data-quality/promote-drift-candidates")
def promote_v2_drift_candidates(
    limit: int = Query(500, ge=1, le=10000),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Promote useful unselected fallback articles into independent source candidates."""
    return data_quality.run_drift_promotion_sweep(limit=limit)


@router.get("/campaigns")
def list_v2_admin_campaigns(
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
def get_v2_admin_campaign_detail(
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
def get_v2_admin_campaign_graph(
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
def run_v2_campaign_correlation(
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
def update_v2_admin_campaign_review(
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
def update_v2_admin_campaign_membership_review(
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
def queue_v2_recanonicalization_sweep(
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
def queue_v2_recanonicalization_for_canonical(
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
def list_v2_canonical_consistency_candidates(
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
def queue_v2_canonical_consistency_sweep(
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
def requeue_v2_dead_letter_tasks(
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


@router.post("/refetch-force")
def queue_v2_force_refetch(
    limit: int = Query(1000, ge=1, le=50000),
    session=Depends(get_v2_session),
    operations: V2OperationsService = Depends(get_v2_operations_service),
    _: bool = Depends(authenticate),
):
    """Queue force re-fetch (+ re-enrich) of previously-fetched source incidents so
    the improved publish-date extractor re-derives dates and re-dates the corpus."""
    result = operations.enqueue_force_refetch(session, limit=limit)
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return result


@router.post("/tasks/{task_id}/cancel")
def cancel_v2_task(
    task_id: str,
    session=Depends(get_v2_session),
    _: bool = Depends(authenticate),
):
    """Cancel a queued or leased pipeline task (e.g. a redundant queued run).

    Queued tasks are cancelled cleanly. A leased task is also marked cancelled and
    its lease cleared so it is not re-leased or continued — but a worker already
    mid-execution finishes its current pass before noticing. Terminal tasks
    (completed / cancelled / dead_letter) are returned unchanged.

    Implemented inline against the session + model so this stays an API-only
    change: shipping it does not redeploy the worker or interrupt a running sweep.
    """
    from uuid import UUID

    from src.edu_cti_v2.models import PipelineTask

    try:
        tid = UUID(str(task_id))
    except (ValueError, AttributeError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid task id")

    task = session.get(PipelineTask, tid)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    terminal = {"completed", "cancelled", "dead_letter"}
    if task.status in terminal:
        return {
            "task_id": str(task.id),
            "status": task.status,
            "cancelled": False,
            "message": f"Task already {task.status}",
        }

    previous_status = task.status
    task.status = "cancelled"
    task.lease_owner = None
    task.lease_token = None
    task.lease_expires_at = None
    commit = getattr(session, "commit", None)
    if callable(commit):
        commit()
    return {
        "task_id": str(task.id),
        "run_id": str(task.run_id) if task.run_id else None,
        "task_type": task.task_type,
        "previous_status": previous_status,
        "status": "cancelled",
        "cancelled": True,
    }


@router.get("/classifier-quality")
def get_v2_classifier_quality(
    sample_limit: int = Query(12, ge=1, le=50),
    debug: bool = Query(False),
    session=Depends(get_v2_session),
    _: bool = Depends(authenticate),
):
    """Quality telemetry for the LLM title-relevance gate and downstream extraction.

    The title classifier (gate 1) marks news/rss rows relevant/irrelevant from the
    headline; the full-article ``is_education_related`` check (gate 2) is the
    precision backstop. A title that gate 1 kept but gate 2 rejected is a
    title-classifier FALSE POSITIVE — its rate is the headline metric here, to be
    compared against the legacy keyword pre-filter (~61% of fetched articles were
    gate-2 rejected). Implemented inline (api-only) so it does not redeploy the
    worker.
    """
    from sqlalchemy import text

    def rows(sql: str, **params):
        return session.execute(text(sql), params).fetchall()

    def scalar(sql: str, **params):
        return session.execute(text(sql), params).scalar() or 0

    # --- gate 1: title relevance distribution (news/rss) --------------------
    relevance = {r[0]: int(r[1]) for r in rows(
        "SELECT relevance_status, count(*) FROM source_incidents "
        "WHERE source_group IN ('news','rss') AND is_deleted = false GROUP BY relevance_status"
    )}
    llm_classified = scalar(
        "SELECT count(*) FROM source_incidents "
        "WHERE source_group IN ('news','rss') AND title_classified_at IS NOT NULL AND is_deleted = false"
    )

    # --- gate 1 -> gate 2 outcome for LLM-classified relevant rows ----------
    gate2 = {str(r[0]): int(r[1]) for r in rows(
        "SELECT e.is_education_related, count(*) "
        "FROM source_incidents si JOIN source_enrichments e ON e.source_incident_id = si.id "
        "WHERE si.relevance_status = 'relevant' AND si.title_classified_at IS NOT NULL "
        "GROUP BY e.is_education_related"
    )}
    tp = gate2.get("True", 0)
    fp = gate2.get("False", 0)
    pending_enrich = gate2.get("None", 0)
    judged = tp + fp
    title_fp_rate = (fp / judged) if judged else None

    # overall gate-2 reject rate across every enrichment (reference baseline)
    overall = {str(r[0]): int(r[1]) for r in rows(
        "SELECT is_education_related, count(*) FROM source_enrichments GROUP BY is_education_related"
    )}
    o_true, o_false = overall.get("True", 0), overall.get("False", 0)
    overall_reject_rate = (o_false / (o_true + o_false)) if (o_true + o_false) else None

    # --- extraction quality of published (open) canonicals ------------------
    open_total = scalar("SELECT count(*) FROM canonical_incidents WHERE status = 'open'")
    eq = rows(
        "SELECT count(institution_name), count(incident_date), count(threat_actor_name), "
        "count(*) FILTER (WHERE is_education_related = true) "
        "FROM canonical_incidents WHERE status = 'open'"
    )[0]
    with_inst, with_date, with_actor, edu_true = (int(eq[0]), int(eq[1]), int(eq[2]), int(eq[3]))

    def pct(n: int, d: int) -> float:
        return round((n / d) * 100, 1) if d else 0.0

    # --- spot-check samples -------------------------------------------------
    if debug:
        # Rich view for auditing whether gate 2 rejected correctly: include the
        # gate-2 education_relevance_reasoning, the article URL, and an excerpt of
        # the article text the second gate actually read.
        fp_samples = [
            {
                "title": r[0],
                "title_reason": r[1],
                "title_score": float(r[2]) if r[2] is not None else None,
                "rejected_reason": r[3],
                "gate2_reasoning": r[4],
                "url": r[5],
                "article_excerpt": (r[6] or "")[:1400],
            }
            for r in rows(
                "SELECT si.raw_title, si.title_relevance_reason, si.title_relevance_score, "
                "e.failed_reason, e.raw_extraction->>'education_relevance_reasoning', "
                "u.resolved_url, a.content_text "
                "FROM source_incidents si "
                "JOIN source_enrichments e ON e.source_incident_id = si.id "
                "LEFT JOIN article_documents a ON a.source_incident_id = si.id "
                "AND a.is_selected_for_enrichment = true "
                "LEFT JOIN source_incident_urls u ON u.id = a.source_incident_url_id "
                "WHERE si.relevance_status = 'relevant' AND si.title_classified_at IS NOT NULL "
                "AND e.is_education_related = false "
                "ORDER BY si.title_classified_at DESC LIMIT :lim",
                lim=sample_limit,
            )
        ]
    else:
        fp_samples = [
            {
                "title": r[0],
                "title_reason": r[1],
                "title_score": float(r[2]) if r[2] is not None else None,
                "rejected_reason": r[3],
            }
            for r in rows(
                "SELECT si.raw_title, si.title_relevance_reason, si.title_relevance_score, e.failed_reason "
                "FROM source_incidents si JOIN source_enrichments e ON e.source_incident_id = si.id "
                "WHERE si.relevance_status = 'relevant' AND si.title_classified_at IS NOT NULL "
                "AND e.is_education_related = false "
                "ORDER BY si.title_classified_at DESC LIMIT :lim",
                lim=sample_limit,
            )
        ]
    tp_samples = [
        {
            "title": r[0],
            "title_reason": r[1],
            "institution_name": r[2],
            "incident_date": r[3].isoformat() if r[3] else None,
        }
        for r in rows(
            "SELECT si.raw_title, si.title_relevance_reason, ci.institution_name, ci.incident_date "
            "FROM source_incidents si "
            "JOIN canonical_memberships m ON m.source_incident_id = si.id "
            "JOIN canonical_incidents ci ON ci.id = m.canonical_incident_id "
            "WHERE si.relevance_status = 'relevant' AND si.title_classified_at IS NOT NULL "
            "AND ci.is_education_related = true AND ci.status = 'open' "
            "ORDER BY si.title_classified_at DESC LIMIT :lim",
            lim=sample_limit,
        )
    ]

    return {
        "title_relevance": {
            "pending": relevance.get("pending", 0),
            "relevant": relevance.get("relevant", 0),
            "irrelevant": relevance.get("irrelevant", 0),
            "llm_classified": int(llm_classified),
        },
        "second_gate": {
            "llm_gated": {
                "true_positive": tp,
                "false_positive": fp,
                "pending_enrichment": pending_enrich,
                "judged": judged,
                "fp_rate_pct": round(title_fp_rate * 100, 1) if title_fp_rate is not None else None,
                "tp_rate_pct": round((tp / judged) * 100, 1) if judged else None,
            },
            "overall_reference": {
                "edu_true": o_true,
                "edu_false": o_false,
                "reject_rate_pct": round(overall_reject_rate * 100, 1) if overall_reject_rate is not None else None,
            },
            "keyword_baseline_reject_pct": 61.0,
        },
        "extraction_quality": {
            "open_canonicals": int(open_total),
            "with_institution_pct": pct(with_inst, open_total),
            "with_date_pct": pct(with_date, open_total),
            "with_actor_pct": pct(with_actor, open_total),
            "edu_confirmed_pct": pct(edu_true, open_total),
        },
        "samples": {
            "false_positives": fp_samples,
            "true_positives": tp_samples,
        },
    }


@router.get("/extraction-samples")
def get_v2_extraction_samples(
    limit: int = Query(8, ge=1, le=30),
    session=Depends(get_v2_session),
    _: bool = Depends(authenticate),
):
    """Recent open canonicals with their extracted fields + source article excerpt.

    Quality-monitoring surface: lets an operator read the article the extraction
    was built from and verify the institution / date / actor / attack / country
    are faithful, and that the incident is genuinely an education cyber incident
    (not a wrongly-kept one). Inline SQL, api-only.
    """
    from sqlalchemy import text

    rows = session.execute(
        text(
            "SELECT ci.id, ci.institution_name, ci.vendor_name, ci.institution_type, "
            "ci.country, ci.incident_date, ci.attack_category, ci.attack_vector, "
            "ci.threat_actor_name, ci.severity, "
            "si.raw_title, "
            "e.is_education_related, e.manual_review_required, e.manual_review_reason, "
            "e.re_enrich_attempts, e.raw_extraction->>'education_relevance_reasoning', "
            "a.content_text "
            "FROM canonical_incidents ci "
            "JOIN canonical_memberships m ON m.canonical_incident_id = ci.id "
            "JOIN source_incidents si ON si.id = m.source_incident_id "
            "LEFT JOIN source_enrichments e ON e.source_incident_id = si.id "
            "LEFT JOIN article_documents a ON a.source_incident_id = si.id "
            "AND a.is_selected_for_enrichment = true "
            "WHERE ci.status = 'open' AND ci.is_education_related = true "
            "ORDER BY ci.created_at DESC LIMIT :lim"
        ),
        {"lim": limit},
    ).fetchall()

    samples = []
    for r in rows:
        samples.append(
            {
                "canonical_id": str(r[0]),
                "institution_name": r[1],
                "vendor_name": r[2],
                "institution_type": r[3],
                "country": r[4],
                "incident_date": r[5].isoformat() if r[5] else None,
                "attack_category": r[6],
                "attack_vector": r[7],
                "threat_actor_name": r[8],
                "severity": r[9],
                "raw_title": r[10],
                "is_education_related": r[11],
                "manual_review_required": r[12],
                "manual_review_reason": r[13],
                "re_enrich_attempts": r[14],
                "education_relevance_reasoning": r[15],
                "article_excerpt": (r[16] or "")[:1600],
            }
        )
    return {"samples": samples, "returned": len(samples)}


@router.get("/title-samples")
def get_v2_title_samples(
    limit: int = Query(50, ge=1, le=500),
    relevance: Optional[str] = Query(None, pattern="^(relevant|irrelevant|pending)$"),
    random: bool = Query(True),
    session=Depends(get_v2_session),
    _: bool = Depends(authenticate),
):
    """Classified news/rss titles with the pipeline's relevance verdict.

    Sampling surface for building a human-labelled title gold set: returns the
    raw title + snippet + source + the classifier's verdict/score, optionally
    filtered to one ``relevance`` bucket so positives can be oversampled (the
    corpus is ~9% relevant). ``random=true`` samples uniformly. Read-only, api-only.
    """
    from sqlalchemy import text

    where = (
        "si.source_group IN ('news','rss') AND si.is_deleted = false "
        "AND si.title_classified_at IS NOT NULL AND si.raw_title IS NOT NULL"
    )
    params: dict = {"lim": limit}
    if relevance:
        where += " AND si.relevance_status = :rel"
        params["rel"] = relevance
    order_by = "random()" if random else "si.title_classified_at DESC"
    rows = session.execute(
        text(
            "SELECT si.id, si.raw_title, si.raw_subtitle, si.source_name, "
            "si.relevance_status, si.title_relevance_score "
            "FROM source_incidents si "
            f"WHERE {where} ORDER BY {order_by} LIMIT :lim"
        ),
        params,
    ).fetchall()
    samples = [
        {
            "source_incident_id": str(r[0]),
            "raw_title": r[1],
            "raw_subtitle": r[2],
            "source_name": r[3],
            "relevance_status": r[4],
            "title_relevance_score": float(r[5]) if r[5] is not None else None,
        }
        for r in rows
    ]
    return {"samples": samples, "returned": len(samples)}


@router.get("/page-yield")
def get_v2_page_yield(
    source_name: Optional[str] = Query(None),
    session=Depends(get_v2_session),
    _: bool = Depends(authenticate),
):
    """Per-page edu-relevance yield for search-news sources.

    The news scrapers record the search page each title came from in raw_notes
    (``...;page=N``). This groups classified news titles by that page and reports
    how many the LLM title gate kept (relevant) vs dropped (irrelevant), so the
    operator can see where edu-relevant yield craters with depth and pick a
    sensible NEWS_MAX_PAGES / COLLECT_MAX_PAGES. Inline SQL, api-only.
    """
    from sqlalchemy import text

    params: dict = {}
    src_filter = ""
    if source_name:
        src_filter = "AND si.source_name = :src"
        params["src"] = source_name

    # Extract the integer page from raw_notes (e.g. '...;page=3'); only news rows
    # that have been title-classified contribute.
    sql = (
        "SELECT si.source_name, "
        "  (substring(si.raw_notes from 'page=([0-9]+)'))::int AS page, "
        "  count(*) FILTER (WHERE si.relevance_status = 'relevant')   AS relevant, "
        "  count(*) FILTER (WHERE si.relevance_status = 'irrelevant') AS irrelevant "
        "FROM source_incidents si "
        "WHERE si.source_group = 'news' AND si.title_classified_at IS NOT NULL "
        "  AND si.raw_notes ~ 'page=[0-9]+' "
        f"  {src_filter} "
        "GROUP BY si.source_name, page "
        "ORDER BY si.source_name, page"
    )
    rows = session.execute(text(sql), params).fetchall()

    by_source: dict[str, list[dict]] = {}
    for r in rows:
        src, page, relevant, irrelevant = r[0], r[1], int(r[2]), int(r[3])
        if page is None:
            continue
        total = relevant + irrelevant
        by_source.setdefault(src, []).append(
            {
                "page": page,
                "relevant": relevant,
                "irrelevant": irrelevant,
                "total": total,
                "relevant_pct": round((relevant / total) * 100, 1) if total else 0.0,
            }
        )

    # Cumulative relevance to suggest a cap: the page beyond which added pages
    # contribute < 5% of cumulative relevant rows.
    suggestions: dict[str, dict] = {}
    for src, pages in by_source.items():
        pages_sorted = sorted(pages, key=lambda p: p["page"])
        total_relevant = sum(p["relevant"] for p in pages_sorted)
        cum = 0
        knee = pages_sorted[-1]["page"] if pages_sorted else 0
        for p in pages_sorted:
            cum += p["relevant"]
            if total_relevant and (cum / total_relevant) >= 0.95:
                knee = p["page"]
                break
        suggestions[src] = {
            "total_relevant": total_relevant,
            "page_for_95pct_relevant": knee,
            "max_page_observed": pages_sorted[-1]["page"] if pages_sorted else 0,
        }

    return {"by_source": by_source, "suggestions": suggestions}


@router.get("/manual-review-queue")
def list_v2_manual_review_queue(
    limit: int = Query(100, ge=1, le=1000),
    session=Depends(get_v2_session),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """List v2 enrichments that exhausted automatic re-enrichment attempts."""
    items = data_quality.list_manual_review_queue(session, limit=limit)
    repo = getattr(data_quality, "source_enrichment_repository", None)
    total = repo.count_manual_review_queue(session) if repo is not None else len(items)
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "returned": len(items),
            "total": total,
        },
    }


@router.get("/rejected-enrichments")
def list_v2_rejected_enrichments(
    limit: int = Query(100, ge=1, le=1000),
    session=Depends(get_v2_session),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """List v2 enrichments that were hard-rejected as non-canonicalizable."""
    items = data_quality.list_rejected_enrichments(session, limit=limit)
    repo = getattr(data_quality, "source_enrichment_repository", None)
    total = repo.count_rejected_enrichments(session) if repo is not None else len(items)
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "returned": len(items),
            "total": total,
        },
    }


@router.post("/data-quality/purge-non-education")
def purge_v2_non_education(
    confirm: bool = Query(False),
    limit: Optional[int] = Query(None, ge=1, le=500000),
    session=Depends(get_v2_session),
    data_quality: V2DataQualityService = Depends(get_v2_data_quality_service),
    _: bool = Depends(authenticate),
):
    """Hard-delete keyword-era junk (enrichments rejected as not education-related).

    ``confirm=false`` returns a dry-run count and changes nothing; ``confirm=true``
    performs the cascade deletion so the fetched/enriched funnel counts reflect
    only genuine education incidents. Used once at the keyword→LLM cutover.
    """
    report = data_quality.purge_non_education_incidents(session, confirm=confirm, limit=limit)
    if confirm:
        session.commit()
    return report


@router.get("/scheduler/status")
def get_v2_scheduler_status(
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Return recurring v2 scheduler status and recent outcomes."""
    return scheduler.get_status()


@router.post("/scheduler/start")
def start_v2_scheduler(
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Start the recurring v2 scheduler in-process."""
    return scheduler.start()


@router.post("/scheduler/stop")
def stop_v2_scheduler(
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Stop the recurring v2 scheduler."""
    return scheduler.stop()


@router.post("/scheduler/trigger/{job_name}")
def trigger_v2_scheduler_job(
    job_name: str,
    background: bool = Query(True),
    scheduler: V2SchedulerService = Depends(get_v2_scheduler_service),
    _: bool = Depends(authenticate),
):
    """Trigger one named recurring v2 scheduler job on demand."""
    return scheduler.trigger_job(job_name, background=background)
