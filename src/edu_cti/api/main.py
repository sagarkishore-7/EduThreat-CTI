"""
EduThreat-CTI REST API

FastAPI application providing REST endpoints for the CTI dashboard.
"""

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, Query, HTTPException, Depends, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .models import (
    IncidentListResponse,
    IncidentDetail,
    IncidentSummary,
    IncidentSource,
    PaginationMeta,
    DashboardStats,
    DashboardResponse,
    CountByCategory,
    TimeSeriesPoint,
    RecentIncident,
    ThreatActorsResponse,
    ThreatActorSummary,
    FilterOptions,
    TimelineEvent,
    MITRETechnique,
    AttackDynamics,
    DataImpact,
    SystemImpact,
    UserImpact,
    FinancialImpact,
    RegulatoryImpact,
    ResearchImpact,
    RecoveryMetrics,
    TransparencyMetrics,
    # Advanced analytics models
    AttackTrendPoint,
    AttackTrendsResponse,
    MitreTacticItem,
    RansomwareTimelineItem,
    RansomwareFamilyDetail,
    RansomEconomics,
    RecoveryComparison,
    RecoveryComparisonResponse,
    RansomwareGeoItem,
    ActorTimelinePoint,
    ActorRansomwareMatrixResponse,
    ActorTargetingItem,
    DataImpactStats,
    RegulatoryImpactStats,
    RecoveryEffectiveness,
    TransparencyLevel,
    TransparencyStats,
    UserImpactTotals,
    FinancialImpactByYear,
    OperationalImpactItem,
    # Extended cross-dimensional analytics models
    InstitutionRiskItem,
    RecoveryByAttackTypeItem,
    AttackVectorByInstitutionResponse,
    BreachSeverityPoint,
    RansomPaymentByYearItem,
    RansomwareFamilyTrendResponse,
    ActorInstitutionResponse,
    ActorTTPResponse,
    DisclosureTimelinePoint,
    BreachByInstitutionItem,
    # Interactive Nivo visualization models
    AttackFlowResponse,
    MitreSunburstResponse,
    ActorNetworkResponse,
    RansomFlowResponse,
    CountryAttackMatrixResponse,
)
from .database import (
    get_api_connection,
    get_incidents_paginated,
    get_incident_by_id,
    get_dashboard_stats,
    get_incidents_by_country,
    get_incidents_by_attack_type,
    get_incidents_by_ransomware_family,
    get_incidents_over_time,
    get_recent_incidents,
    get_threat_actors,
    get_filter_options,
    # Advanced analytics
    get_attack_trends,
    get_attack_vectors,
    get_mitre_tactics,
    get_initial_access_methods,
    get_system_impact_stats,
    get_ransomware_timeline,
    get_ransomware_families_detail,
    get_ransom_economics,
    get_ransomware_recovery_comparison,
    get_ransomware_geo,
    get_threat_actor_categories,
    get_threat_actor_motivations,
    get_threat_actor_timeline,
    get_actor_ransomware_matrix,
    get_actor_targeting,
    get_institution_types,
    get_operational_impact,
    get_financial_impact_by_year,
    get_data_impact_stats,
    get_regulatory_impact_stats,
    get_recovery_effectiveness,
    get_transparency_metrics as get_transparency_metrics_db,
    get_user_impact_totals,
    get_raw_incident_data,
    # Extended cross-dimensional analytics
    get_institution_risk_matrix,
    get_recovery_by_attack_type,
    get_attack_vector_by_institution,
    get_breach_severity_timeline,
    get_ransom_payment_by_year,
    get_ransomware_family_trend,
    get_actor_institution_targeting,
    get_actor_ttp_profile,
    get_disclosure_timeline,
    get_breach_by_institution_type,
    # Interactive Nivo visualizations
    get_attack_flow,
    get_mitre_sunburst,
    get_actor_network,
    get_ransom_flow,
    get_country_attack_matrix,
)
from .cache import cache_get, cache_set
from .schema_docs import SCHEMA_DOC, SchemaResponse

logger = logging.getLogger(__name__)


# ============================================================
# App Lifecycle
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting EduThreat-CTI API...")

    # Initialize database on startup
    try:
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.core.config import DB_PATH, METRICS_DB_PATH
        from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table
        from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table

        logger.info(f"Initializing database at: {DB_PATH}")

        # Ensure data directory exists
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database (creates tables if they don't exist)
        conn = get_connection()
        init_db(conn)

        # Initialize enrichment tables
        init_incident_enrichments_table(conn)
        init_articles_table(conn)

        conn.commit()

        # Truncate the SQLite WAL file. The WAL grows during writes and is only
        # auto-checkpointed when it crosses wal_autocheckpoint pages — across
        # many container crashes / redeploys it can accumulate to GB+, which
        # SQLite then maps into shared memory on every connection open. A
        # full TRUNCATE checkpoint on startup brings it back to zero size.
        try:
            cur = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            row = cur.fetchone()
            if row:
                # row = (busy, log_pages, checkpointed_pages)
                logger.info(
                    "Startup WAL checkpoint: busy=%s log_pages=%s checkpointed=%s",
                    row[0], row[1], row[2],
                )
        except sqlite3.Error as exc:
            logger.warning("Startup WAL checkpoint failed (non-fatal): %s", exc)

        # Report DATA_DIR contents so persistent-state bloat is visible in logs.
        # Also auto-prune the HuggingFace cache if it has grown past the safe
        # threshold — every redeploy that downloads a model adds new blob files
        # without garbage-collecting old ones, so a 250 MB model can grow to
        # 1+ GB across many redeploys and OOM the container on next startup.
        try:
            import shutil
            from src.edu_cti.core.config import DATA_DIR
            data_dir = Path(DATA_DIR)
            sizes = []
            hf_cache_dir = data_dir / "hf_cache"
            hf_cache_size = 0
            if data_dir.exists():
                for p in sorted(data_dir.iterdir()):
                    try:
                        if p.is_file():
                            sizes.append((p.name, p.stat().st_size))
                        elif p.is_dir():
                            total = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
                            sizes.append((f"{p.name}/", total))
                            if p == hf_cache_dir:
                                hf_cache_size = total
                    except OSError:
                        pass
            sizes.sort(key=lambda x: -x[1])
            logger.info(
                "DATA_DIR contents: %s",
                ", ".join(f"{n}={s/(1024*1024):.1f}MB" for n, s in sizes[:10]),
            )

            # Auto-prune oversized HF cache (threshold: 500 MB).
            # Two models (GLiNER ~150 MB + sentence-transformer ~90 MB) should
            # total ~250 MB. Anything past 500 MB is accumulated junk.
            HF_CACHE_THRESHOLD_BYTES = 500 * 1024 * 1024
            if hf_cache_size > HF_CACHE_THRESHOLD_BYTES and hf_cache_dir.exists():
                logger.warning(
                    "HF cache is %.1f MB (threshold %.1f MB) — auto-pruning to release memory pressure",
                    hf_cache_size / (1024 * 1024),
                    HF_CACHE_THRESHOLD_BYTES / (1024 * 1024),
                )
                try:
                    shutil.rmtree(hf_cache_dir)
                    hf_cache_dir.mkdir(parents=True, exist_ok=True)
                    logger.info("HF cache auto-pruned. Models will re-download on next pipeline run.")
                except Exception as exc:
                    logger.error("HF cache auto-prune failed: %s", exc)
        except Exception as exc:
            logger.debug("DATA_DIR audit skipped: %s", exc)

        conn.close()

        logger.info("Database initialized successfully")

        # Start persistent metrics — load cumulative counters/histograms from DB
        # so they survive container restarts and redeploys.
        from src.edu_cti.core import metrics as _metrics_module
        _metrics_module.get_metrics().configure(METRICS_DB_PATH)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        # Don't fail startup - let it try again on first request

    # Start self-ping to prevent Railway from sleeping the container
    # during long-running background tasks (pipeline, enrichment)
    import asyncio
    import httpx
    import os

    async def _keep_alive():
        port = os.getenv("PORT", "8000")
        url = f"http://localhost:{port}/api/health"
        while True:
            await asyncio.sleep(300)  # Ping every 5 minutes
            try:
                async with httpx.AsyncClient() as client:
                    await client.get(url, timeout=5)
            except Exception:
                pass  # Best-effort; container is alive if this code runs

    keep_alive_task = asyncio.create_task(_keep_alive())

    yield

    keep_alive_task.cancel()
    logger.info("Shutting down EduThreat-CTI API...")


OPENAPI_TAGS = [
    {
        "name": "Health",
        "description": "Service health and readiness checks.",
    },
    {
        "name": "Dashboard",
        "description": "Aggregated statistics, charts, and recent activity for the CTI dashboard.",
    },
    {
        "name": "Incidents",
        "description": "CRUD and search operations on cyber incidents affecting education institutions.",
    },
    {
        "name": "Analytics",
        "description": "Breakdown and time-series analytics by country, attack type, ransomware family, and threat actor.",
    },
    {
        "name": "Filters",
        "description": "Available filter option values for building dynamic UI dropdowns.",
    },
    {
        "name": "Admin",
        "description": "Administrative endpoints for pipeline management, enrichment triggers, and database stats.",
    },
    {
        "name": "Schema",
        "description": "Database schema reference and pipeline documentation — all tables, columns, and analytics endpoint mappings.",
    },
]


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="EduThreat-CTI API",
        description=(
            "## Education Sector Cyber Threat Intelligence\n\n"
            "Real-time threat intelligence platform tracking cyber incidents affecting "
            "universities, schools, and research institutions worldwide since 2019.\n\n"
            "### Features\n"
            "- **800+ incidents** across 50+ countries\n"
            "- LLM-enriched CTI extraction (MITRE ATT&CK, timelines, impact metrics)\n"
            "- Multi-source ingestion (curated, news, RSS, API)\n"
            "- Cross-source deduplication and fuzzy institution matching\n\n"
            "### Data Sources\n"
            "KonBriefing, Ransomware.live, DataBreaches.net, CISA KEV, "
            "BleepingComputer, Krebs on Security, The Record, and more."
        ),
        version="2.1.0",
        lifespan=lifespan,
        openapi_tags=OPENAPI_TAGS,
        docs_url="/docs",
        redoc_url="/redoc",
    )
    
    # CORS middleware for frontend access
    allowed_origins = os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )
    
    return app


app = create_app()


def _pipeline_is_running() -> bool:
    """Return True when a background pipeline run is actively mutating the dataset."""
    try:
        from src.edu_cti.pipeline.manager import get_pipeline_manager

        return get_pipeline_manager().is_running
    except Exception:
        return False

# Add exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle FastAPI validation errors with detailed logging."""
    logger.error(f"Validation error on {request.method} {request.url}: {exc.errors()}")
    logger.error(f"Validation error {request.method} {request.url}: {str(exc.errors())[:200]}")
    return JSONResponse(
        status_code=400,
        content={
            "detail": exc.errors(),
            "body": exc.body,
        }
    )

# Include admin router
from .admin import router as admin_router
app.include_router(admin_router, prefix="/api")


# ============================================================
# Health Check
# ============================================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Basic liveness probe. Reports a stalled watchdog without killing the process."""
    try:
        from src.edu_cti.pipeline.phase2.__main__ import _get_watchdog
        watchdog = _get_watchdog()
        if watchdog and watchdog.is_stalled():
            logger.error("[HEALTH] Enrichment watchdog stall detected")
            return JSONResponse(
                status_code=503,
                content={
                    "status": "degraded",
                    "watchdog": "stalled",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )
    except Exception:
        pass
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/health", tags=["Health"])
async def api_health_check():
    """Health check with database connectivity test and incident count."""
    try:
        conn = get_api_connection()
        cur = conn.execute("SELECT COUNT(*) as count FROM incidents")
        count = cur.fetchone()["count"]
        conn.close()
        return {
            "status": "healthy",
            "database": "connected",
            "incident_count": count,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        }


@app.get("/metrics", tags=["Health"])
async def prometheus_metrics():
    """Prometheus-compatible metrics endpoint."""
    from src.edu_cti.core.metrics import get_metrics
    metrics = get_metrics()
    return Response(
        content=metrics.format_prometheus(),
        media_type="text/plain; version=0.0.4",
    )


@app.get("/api/metrics/fetch-stats", tags=["Health"])
async def fetch_stats():
    """
    Per-tier article fetch statistics — success rates, latency percentiles,
    content length distributions, SERP and rate-limiting counts.
    Suitable for pipeline methodology documentation in research papers.
    """
    from src.edu_cti.core.metrics import get_metrics
    return get_metrics().fetch_stats_by_tier()


@app.get("/api/metrics/research-summary", tags=["Health"])
async def research_summary():
    """
    Paper-ready metrics summary — LLM extraction quality, field completeness,
    source novelty rates, deduplication quality, and pipeline performance.
    Suitable for citation in the methodology section of research papers.
    """
    from src.edu_cti.core.metrics import get_metrics
    return get_metrics().research_summary()


# ============================================================
# Dashboard Endpoints
# ============================================================

@app.get("/api/dashboard", response_model=DashboardResponse, tags=["Dashboard"])
async def get_dashboard():
    """
    Get complete dashboard data including stats, charts, and recent incidents.
    
    Returns aggregated statistics, incident distributions, and recent activity.
    """
    cache_key = "dashboard"
    bypass_cache = _pipeline_is_running()
    if not bypass_cache:
        cached = cache_get(cache_key, ttl_seconds=300)
        if cached is not None:
            return cached

    try:
        conn = get_api_connection()

        stats_data = get_dashboard_stats(conn)
        stats = DashboardStats(**stats_data)

        incidents_by_country = [
            CountByCategory(**c) for c in get_incidents_by_country(conn, limit=200)
        ]

        incidents_by_attack_type = [
            CountByCategory(**c) for c in get_incidents_by_attack_type(conn, limit=12)
        ]

        incidents_by_ransomware = [
            CountByCategory(**c) for c in get_incidents_by_ransomware_family(conn, limit=12)
        ]

        incidents_over_time = [
            TimeSeriesPoint(**t) for t in get_incidents_over_time(conn, months=24)
            if t.get("date") is not None
        ]

        recent_incidents = [
            RecentIncident(**i) for i in get_recent_incidents(conn, limit=10)
        ]

        conn.close()

        result = DashboardResponse(
            stats=stats,
            incidents_by_country=incidents_by_country,
            incidents_by_attack_type=incidents_by_attack_type,
            incidents_by_ransomware=incidents_by_ransomware,
            incidents_over_time=incidents_over_time,
            recent_incidents=recent_incidents,
        )
        if not bypass_cache:
            cache_set(cache_key, result)
        return result

    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats", response_model=DashboardStats, tags=["Dashboard"])
async def get_stats():
    """Get overall dashboard statistics."""
    cache_key = "stats"
    bypass_cache = _pipeline_is_running()
    if not bypass_cache:
        cached = cache_get(cache_key, ttl_seconds=300)
        if cached is not None:
            return cached

    try:
        conn = get_api_connection()
        stats_data = get_dashboard_stats(conn)
        conn.close()
        result = DashboardStats(**stats_data)
        if not bypass_cache:
            cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Incidents Endpoints
# ============================================================

@app.get("/api/incidents", response_model=IncidentListResponse, tags=["Incidents"])
async def list_incidents(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    country: Optional[str] = Query(None, description="Filter by country code"),
    attack_category: Optional[str] = Query(None, description="Filter by attack category"),
    ransomware_family: Optional[str] = Query(None, description="Filter by ransomware family"),
    threat_actor: Optional[str] = Query(None, description="Filter by threat actor"),
    institution_type: Optional[str] = Query(None, description="Filter by institution type"),
    year: Optional[int] = Query(None, description="Filter by year"),
    enriched_only: bool = Query(False, description="Only show enriched incidents"),
    data_breached: bool = Query(False, description="Only show incidents with confirmed data breach"),
    search: Optional[str] = Query(None, description="Search query"),
    sort_by: str = Query("incident_date", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order (asc/desc)"),
):
    """
    List incidents with pagination and filtering.
    
    Supports filtering by country, attack type, ransomware family, threat actor,
    institution type, and year. Full-text search is also available.
    """
    try:
        conn = get_api_connection()
        
        incidents_data, total = get_incidents_paginated(
            conn,
            page=page,
            per_page=per_page,
            country=country,
            attack_category=attack_category,
            ransomware_family=ransomware_family,
            threat_actor=threat_actor,
            institution_type=institution_type,
            year=year,
            enriched_only=enriched_only,
            data_breached=data_breached,
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        
        conn.close()
        
        # Convert to response models
        incidents = [
            IncidentSummary(
                incident_id=i["incident_id"],
                institution_name=i["institution_name"] or "Unknown",
                institution_type=i.get("institution_type"),
                country=i.get("country"),
                country_code=i.get("country_code"),
                region=i.get("region"),
                city=i.get("city"),
                incident_date=i.get("incident_date"),
                date_precision=i.get("date_precision"),
                title=i.get("title"),
                subtitle=i.get("subtitle"),
                enriched_summary=i.get("enriched_summary"),
                attack_type_hint=i.get("attack_type_hint"),
                attack_category=i.get("attack_category"),
                ransomware_family=i.get("ransomware_family"),
                threat_actor_name=i.get("threat_actor_name"),
                status=i.get("status", "suspected"),
                source_confidence=i.get("source_confidence", "medium"),
                llm_enriched=bool(i.get("llm_enriched")),
                llm_enriched_at=i.get("llm_enriched_at"),
                ingested_at=i.get("ingested_at"),
                sources=i.get("sources", []),
            )
            for i in incidents_data
        ]
        
        total_pages = (total + per_page - 1) // per_page
        
        pagination = PaginationMeta(
            page=page,
            per_page=per_page,
            total=total,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        )
        
        return IncidentListResponse(
            incidents=incidents,
            pagination=pagination,
        )
        
    except Exception as e:
        logger.error(f"Error listing incidents: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/incidents/{incident_id}/report", tags=["Incidents"])
async def get_incident_report(incident_id: str):
    """
    Generate and download a CTI report for an incident.
    
    Returns a markdown-formatted report following industry-standard frameworks.
    """
    try:
        from src.edu_cti.api.reports import generate_cti_report
        from src.edu_cti.api.database import get_api_connection, get_incident_by_id
        
        conn = get_api_connection()
        incident_data = get_incident_by_id(conn, incident_id)
        conn.close()
        
        if not incident_data:
            raise HTTPException(status_code=404, detail="Incident not found")
        
        report = generate_cti_report(incident_data)
        
        from fastapi.responses import Response
        return Response(
            content=report,
            media_type="text/markdown",
            headers={
                "Content-Disposition": f'attachment; filename="cti-report-{incident_id}.md"'
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating report for incident {incident_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/incidents/{incident_id}", response_model=IncidentDetail, tags=["Incidents"])
async def get_incident(incident_id: str):
    """
    Get full incident details including all enrichment data.
    
    Returns comprehensive CTI data including timeline, MITRE ATT&CK techniques,
    attack dynamics, and impact metrics.
    """
    try:
        conn = get_api_connection()
        incident_data = get_incident_by_id(conn, incident_id)
        conn.close()
        
        if not incident_data:
            raise HTTPException(status_code=404, detail="Incident not found")
        
        # Build response model
        timeline = None
        if incident_data.get("timeline"):
            timeline = [TimelineEvent(**e) for e in incident_data["timeline"] if isinstance(e, dict)]
        
        mitre_techniques = None
        if incident_data.get("mitre_attack_techniques"):
            mitre_techniques = [MITRETechnique(**t) for t in incident_data["mitre_attack_techniques"] if isinstance(t, dict)]
        
        attack_dynamics = None
        if incident_data.get("attack_dynamics"):
            attack_dynamics = AttackDynamics(**incident_data["attack_dynamics"])
        
        sources = [
            IncidentSource(**s) for s in incident_data.get("sources", [])
        ]
        
        # Build impact objects from flat data
        data_impact = DataImpact(
            data_breached=incident_data.get("data_breached"),
            data_exfiltrated=incident_data.get("data_exfiltrated"),
            data_categories=incident_data.get("data_categories") if isinstance(incident_data.get("data_categories"), list) else None,
            records_affected_exact=incident_data.get("records_affected_exact"),
            records_affected_min=incident_data.get("records_affected_min"),
            records_affected_max=incident_data.get("records_affected_max"),
            pii_records_leaked=incident_data.get("pii_records_leaked"),
        )
        
        system_impact = SystemImpact(
            systems_affected=incident_data.get("systems_affected"),
            critical_systems_affected=incident_data.get("critical_systems_affected"),
            network_compromised=incident_data.get("network_compromised"),
            email_system_affected=incident_data.get("email_system_affected"),
            student_portal_affected=incident_data.get("student_portal_affected"),
            research_systems_affected=incident_data.get("research_systems_affected"),
            hospital_systems_affected=incident_data.get("hospital_systems_affected"),
            cloud_services_affected=incident_data.get("cloud_services_affected"),
            third_party_vendor_impact=incident_data.get("third_party_vendor_impact"),
            vendor_name=incident_data.get("vendor_name"),
        )
        
        user_impact = UserImpact(
            students_affected=incident_data.get("students_affected"),
            staff_affected=incident_data.get("staff_affected"),
            faculty_affected=incident_data.get("faculty_affected"),
            alumni_affected=incident_data.get("alumni_affected"),
            parents_affected=incident_data.get("parents_affected"),
            applicants_affected=incident_data.get("applicants_affected"),
            patients_affected=incident_data.get("patients_affected"),
            users_affected_min=incident_data.get("users_affected_min"),
            users_affected_max=incident_data.get("users_affected_max"),
            users_affected_exact=incident_data.get("users_affected_exact"),
            total_individuals_affected=incident_data.get("users_affected_exact"),
        )
        
        financial_impact = FinancialImpact(
            estimated_total_cost_usd=incident_data.get("total_cost_estimate"),
            ransom_cost_usd=incident_data.get("ransom_amount"),
            recovery_cost_usd=incident_data.get("recovery_costs_max"),
            legal_cost_usd=incident_data.get("legal_costs"),
            notification_cost_usd=incident_data.get("notification_costs"),
            insurance_claim=incident_data.get("insurance_claim"),
            insurance_payout_usd=incident_data.get("insurance_claim_amount"),
            business_impact=incident_data.get("business_impact"),
        )
        
        regulatory_impact = RegulatoryImpact(
            applicable_regulations=incident_data.get("regulatory_context") if isinstance(incident_data.get("regulatory_context"), list) else None,
            gdpr_breach=incident_data.get("gdpr_breach"),
            hipaa_breach=incident_data.get("hipaa_breach"),
            ferpa_breach=incident_data.get("ferpa_breach"),
            breach_notification_required=incident_data.get("breach_notification_required"),
            notification_sent=incident_data.get("notifications_sent"),
            notification_sent_date=incident_data.get("notifications_sent_date"),
            notification_delay_days=incident_data.get("notification_delay_days"),
            dpa_notified=incident_data.get("dpa_notified"),
            investigation_opened=incident_data.get("investigation_opened"),
            fine_imposed=incident_data.get("fine_imposed"),
            fine_amount_usd=incident_data.get("fine_amount"),
            lawsuits_filed=incident_data.get("lawsuits_filed"),
            class_action_filed=incident_data.get("class_action"),
        )

        research_impact = ResearchImpact(
            research_projects_affected=incident_data.get("research_projects_affected"),
            research_data_compromised=incident_data.get("research_data_compromised"),
            publications_delayed=incident_data.get("publications_delayed"),
            grants_affected=incident_data.get("grants_affected"),
            research_area=incident_data.get("research_area"),
        )

        recovery_metrics = RecoveryMetrics(
            recovery_duration_days=incident_data.get("recovery_timeframe_days"),
            from_backup=incident_data.get("from_backup"),
            backup_status=incident_data.get("backup_status"),
            backup_age_days=incident_data.get("backup_age_days"),
            mfa_implemented=incident_data.get("mfa_implemented"),
            law_enforcement_involved=incident_data.get("law_enforcement_involved"),
            law_enforcement_agency=incident_data.get("law_enforcement_agency"),
            ir_firm_engaged=incident_data.get("incident_response_firm"),
            forensics_firm=incident_data.get("forensics_firm"),
        )
        
        transparency_metrics = TransparencyMetrics(
            public_disclosure=incident_data.get("public_disclosure"),
            public_disclosure_date=incident_data.get("public_disclosure_date"),
            disclosure_delay_days=incident_data.get("disclosure_delay_days"),
            transparency_level=incident_data.get("transparency_level"),
        )
        
        return IncidentDetail(
            incident_id=incident_data["incident_id"],
            institution_name=incident_data.get("institution_name") or "Unknown",
            institution_type=incident_data.get("institution_type"),
            institution_size=incident_data.get("institution_size"),
            country=incident_data.get("country"),
            country_code=incident_data.get("country_code"),
            region=incident_data.get("region"),
            city=incident_data.get("city"),
            incident_date=incident_data.get("incident_date"),
            date_precision=incident_data.get("date_precision"),
            source_published_date=incident_data.get("source_published_date"),
            ingested_at=incident_data.get("ingested_at"),
            title=incident_data.get("title"),
            subtitle=incident_data.get("subtitle"),
            enriched_summary=incident_data.get("enriched_summary") or incident_data.get("llm_summary"),
            initial_access_description=incident_data.get("initial_access_description"),
            primary_url=incident_data.get("primary_url"),
            all_urls=incident_data.get("all_urls", []),
            leak_site_url=incident_data.get("leak_site_url"),
            source_detail_url=incident_data.get("source_detail_url"),
            screenshot_url=incident_data.get("screenshot_url"),
            attack_type_hint=incident_data.get("attack_type_hint"),
            attack_category=incident_data.get("attack_category"),
            incident_severity=incident_data.get("incident_severity"),
            status=incident_data.get("status", "suspected"),
            source_confidence=incident_data.get("source_confidence", "medium"),
            academic_period_affected=incident_data.get("academic_period_affected"),
            dark_web_posting_confirmed=incident_data.get("dark_web_posting_confirmed"),
            prior_breach_same_institution=incident_data.get("prior_breach_same_institution"),
            threat_actor=incident_data.get("threat_actor"),
            # COALESCE: prefer LLM-extracted name, fall back to raw ingestion (ransomware.live group)
            threat_actor_name=incident_data.get("threat_actor_name") or incident_data.get("threat_actor"),
            threat_actor_category=incident_data.get("threat_actor_category"),
            threat_actor_motivation=incident_data.get("threat_actor_motivation"),
            threat_actor_origin_country=incident_data.get("threat_actor_origin_country"),
            timeline=timeline,
            mitre_attack_techniques=mitre_techniques,
            attack_dynamics=attack_dynamics,
            data_impact=data_impact,
            system_impact=system_impact,
            user_impact=user_impact,
            financial_impact=financial_impact,
            regulatory_impact=regulatory_impact,
            research_impact=research_impact,
            recovery_metrics=recovery_metrics,
            transparency_metrics=transparency_metrics,
            llm_enriched=bool(incident_data.get("llm_enriched")),
            llm_enriched_at=incident_data.get("llm_enriched_at"),
            sources=sources,
            notes=incident_data.get("notes"),
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting incident {incident_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Analytics Endpoints
# ============================================================

@app.get("/api/analytics/countries", tags=["Analytics"])
async def get_country_analytics(limit: int = Query(20, ge=1, le=500)):
    """Get incident counts by country."""
    cache_key = f"analytics:countries:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    try:
        conn = get_api_connection()
        data = get_incidents_by_country(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting country analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/attack-types", tags=["Analytics"])
async def get_attack_type_analytics(limit: int = Query(15, ge=1, le=50)):
    """Get incident counts by attack type."""
    cache_key = f"analytics:attack-types:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    try:
        conn = get_api_connection()
        data = get_incidents_by_attack_type(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting attack type analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware", tags=["Analytics"])
async def get_ransomware_analytics(limit: int = Query(15, ge=1, le=50)):
    """Get incident counts by ransomware family."""
    cache_key = f"analytics:ransomware:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    try:
        conn = get_api_connection()
        data = get_incidents_by_ransomware_family(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting ransomware analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/timeline", tags=["Analytics"])
async def get_timeline_analytics(months: int = Query(24, ge=1, le=120)):
    """Get incident counts over time."""
    cache_key = f"analytics:timeline:{months}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    try:
        conn = get_api_connection()
        data = get_incidents_over_time(conn, months=months)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting timeline analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/threat-actors", response_model=ThreatActorsResponse, tags=["Analytics"])
async def get_threat_actor_analytics(limit: int = Query(20, ge=1, le=500)):
    """Get threat actor activity summary."""
    cache_key = f"analytics:threat-actors:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    try:
        conn = get_api_connection()
        actors_data = get_threat_actors(conn, limit=limit)
        conn.close()

        actors = [ThreatActorSummary(**a) for a in actors_data["threat_actors"]]

        result = ThreatActorsResponse(
            threat_actors=actors,
            total=actors_data["total"],
            returned=actors_data["returned"],
            total_incidents=actors_data["total_incidents"],
            countries_targeted_total=actors_data["countries_targeted_total"],
        )
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting threat actor analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Advanced Analytics Endpoints
# ============================================================

@app.get("/api/analytics/attack-trends", tags=["Analytics"])
async def get_attack_trends_endpoint(months: int = Query(36, ge=1, le=120)):
    """Get attack trends over time by category (stacked area chart)."""
    cache_key = f"analytics:attack-trends:{months}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_attack_trends(conn, months=months)
        conn.close()
        result = {"data": data, "total": len(data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting attack trends: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/attack-vectors", tags=["Analytics"])
async def get_attack_vectors_endpoint(limit: int = Query(10, ge=1, le=50)):
    """Get attack vector distribution."""
    cache_key = f"analytics:attack-vectors:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_attack_vectors(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting attack vectors: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/mitre-tactics", tags=["Analytics"])
async def get_mitre_tactics_endpoint():
    """Get MITRE ATT&CK tactic distribution."""
    cache_key = "analytics:mitre-tactics"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_mitre_tactics(conn)
        conn.close()
        result = {"data": data, "total": sum(d["count"] for d in data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting MITRE tactics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/initial-access", tags=["Analytics"])
async def get_initial_access_endpoint(limit: int = Query(12, ge=1, le=50)):
    """Get initial access method distribution."""
    cache_key = f"analytics:initial-access:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_initial_access_methods(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting initial access methods: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/system-impact", tags=["Analytics"])
async def get_system_impact_endpoint():
    """Get system impact statistics."""
    cache_key = "analytics:system-impact"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_system_impact_stats(conn)
        conn.close()
        result = {"data": data, "total": sum(d["count"] for d in data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting system impact: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware-timeline", tags=["Analytics"])
async def get_ransomware_timeline_endpoint(limit: int = Query(15, ge=1, le=50)):
    """Get ransomware family activity periods."""
    cache_key = f"analytics:ransomware-timeline:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransomware_timeline(conn, limit=limit)
        conn.close()
        result = {"data": data, "total": len(data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting ransomware timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware-families-detail", tags=["Analytics"])
async def get_ransomware_families_detail_endpoint(limit: int = Query(15, ge=1, le=50)):
    """Get enhanced ransomware family stats."""
    cache_key = f"analytics:ransomware-families-detail:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransomware_families_detail(conn, limit=limit)
        conn.close()
        result = {"data": data, "total": len(data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting ransomware families detail: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransom-economics", tags=["Analytics"])
async def get_ransom_economics_endpoint():
    """Get ransom demand/payment economics."""
    cache_key = "analytics:ransom-economics"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransom_economics(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting ransom economics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware-recovery", tags=["Analytics"])
async def get_ransomware_recovery_endpoint():
    """Compare recovery metrics: ransomware vs non-ransomware."""
    cache_key = "analytics:ransomware-recovery"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransomware_recovery_comparison(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting ransomware recovery: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware-geo", tags=["Analytics"])
async def get_ransomware_geo_endpoint():
    """Get per-family geographic targeting."""
    cache_key = "analytics:ransomware-geo"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransomware_geo(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting ransomware geo: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/threat-actor-categories", tags=["Analytics"])
async def get_threat_actor_categories_endpoint():
    """Get threat actor category distribution."""
    cache_key = "analytics:threat-actor-categories"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_threat_actor_categories(conn)
        conn.close()
        result = {"data": data, "total": sum(d["count"] for d in data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting threat actor categories: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/threat-actor-motivations", tags=["Analytics"])
async def get_threat_actor_motivations_endpoint():
    """Get threat actor motivation distribution."""
    cache_key = "analytics:threat-actor-motivations"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_threat_actor_motivations(conn)
        conn.close()
        result = {"data": data, "total": sum(d["count"] for d in data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting threat actor motivations: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/threat-actor-timeline", tags=["Analytics"])
async def get_threat_actor_timeline_endpoint(limit: int = Query(10, ge=1, le=50)):
    """Get monthly activity per threat actor."""
    cache_key = f"analytics:threat-actor-timeline:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_threat_actor_timeline(conn, limit=limit)
        conn.close()
        result = {"data": data, "total": len(data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting threat actor timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/actor-ransomware-matrix", tags=["Analytics"])
async def get_actor_ransomware_matrix_endpoint():
    """Get actor-to-ransomware-family cross-tabulation."""
    cache_key = "analytics:actor-ransomware-matrix"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_actor_ransomware_matrix(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting actor ransomware matrix: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/actor-targeting", tags=["Analytics"])
async def get_actor_targeting_endpoint(limit: int = Query(10, ge=1, le=50)):
    """Get per-actor country targeting."""
    cache_key = f"analytics:actor-targeting:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_actor_targeting(conn, limit=limit)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting actor targeting: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/institution-types", tags=["Analytics"])
async def get_institution_types_endpoint():
    """Get institution type distribution."""
    cache_key = "analytics:institution-types"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_institution_types(conn)
        total = sum(d["count"] for d in data)
        conn.close()
        result = {"data": data, "total": total}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting institution types: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/operational-impact", tags=["Analytics"])
async def get_operational_impact_endpoint():
    """Get operational impact metrics."""
    cache_key = "analytics:operational-impact"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_operational_impact(conn)
        conn.close()
        result = {"data": data, "total": sum(d["count"] for d in data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting operational impact: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/financial-impact", tags=["Analytics"])
async def get_financial_impact_endpoint():
    """Get financial impact by year."""
    cache_key = "analytics:financial-impact"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_financial_impact_by_year(conn)
        conn.close()
        result = {"data": data, "total": len(data)}
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting financial impact: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/data-impact", tags=["Analytics"])
async def get_data_impact_endpoint():
    """Get data breach impact statistics."""
    cache_key = "analytics:data-impact"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_data_impact_stats(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting data impact: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/regulatory-impact", tags=["Analytics"])
async def get_regulatory_impact_endpoint():
    """Get regulatory compliance statistics."""
    cache_key = "analytics:regulatory-impact"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_regulatory_impact_stats(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting regulatory impact: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/recovery-metrics", tags=["Analytics"])
async def get_recovery_metrics_endpoint():
    """Get recovery effectiveness metrics."""
    cache_key = "analytics:recovery-metrics"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_recovery_effectiveness(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting recovery metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/transparency-metrics", tags=["Analytics"])
async def get_transparency_metrics_endpoint():
    """Get transparency and disclosure metrics."""
    cache_key = "analytics:transparency-metrics"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_transparency_metrics_db(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting transparency metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/user-impact", tags=["Analytics"])
async def get_user_impact_endpoint():
    """Get user category impact totals."""
    cache_key = "analytics:user-impact"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_user_impact_totals(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting user impact: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Filter Options Endpoint
# ============================================================

@app.get("/api/filters", response_model=FilterOptions, tags=["Filters"])
async def get_filters():
    """Get available filter options for the incidents list."""
    cache_key = "filters"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached

    try:
        conn = get_api_connection()
        options = get_filter_options(conn)
        conn.close()
        result = FilterOptions(**options)
        cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Error getting filter options: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Extended Cross-Dimensional Analytics
# ============================================================

@app.get("/api/analytics/institution-risk-matrix", response_model=List[InstitutionRiskItem], tags=["Analytics"])
async def api_institution_risk_matrix():
    """Institution type × attack category cross-tabulation."""
    cache_key = "analytics:institution-risk-matrix"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_institution_risk_matrix(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting institution risk matrix: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/recovery-by-attack-type", response_model=List[RecoveryByAttackTypeItem], tags=["Analytics"])
async def api_recovery_by_attack_type():
    """Recovery and downtime by attack category."""
    cache_key = "analytics:recovery-by-attack-type"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_recovery_by_attack_type(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting recovery by attack type: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/attack-vector-by-institution", response_model=AttackVectorByInstitutionResponse, tags=["Analytics"])
async def api_attack_vector_by_institution(limit: int = Query(8, ge=1, le=20)):
    """Attack vector distribution per institution type."""
    cache_key = f"analytics:attack-vector-by-institution:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_attack_vector_by_institution(conn, limit=limit)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting attack vector by institution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/breach-severity-timeline", response_model=List[BreachSeverityPoint], tags=["Analytics"])
async def api_breach_severity_timeline(months: int = Query(60, ge=12, le=120)):
    """Monthly incident count + avg records breached over time."""
    cache_key = f"analytics:breach-severity-timeline:{months}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_breach_severity_timeline(conn, months=months)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting breach severity timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransom-payment-by-year", response_model=List[RansomPaymentByYearItem], tags=["Analytics"])
async def api_ransom_payment_by_year():
    """Ransom demanded vs paid by year."""
    cache_key = "analytics:ransom-payment-by-year"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransom_payment_by_year(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting ransom payment by year: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware-family-trend", response_model=RansomwareFamilyTrendResponse, tags=["Analytics"])
async def api_ransomware_family_trend(limit: int = Query(8, ge=1, le=20)):
    """Top ransomware families over time."""
    cache_key = f"analytics:ransomware-family-trend:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransomware_family_trend(conn, limit=limit)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting ransomware family trend: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/actor-institution-targeting", response_model=ActorInstitutionResponse, tags=["Analytics"])
async def api_actor_institution_targeting(limit: int = Query(12, ge=1, le=20)):
    """Actor × institution type targeting matrix."""
    cache_key = f"analytics:actor-institution-targeting:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_actor_institution_targeting(conn, limit=limit)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting actor institution targeting: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/actor-ttp-profile", response_model=ActorTTPResponse, tags=["Analytics"])
async def api_actor_ttp_profile(limit: int = Query(8, ge=1, le=20)):
    """Actor MITRE ATT&CK tactic profiles."""
    cache_key = f"analytics:actor-ttp-profile:{limit}"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_actor_ttp_profile(conn, limit=limit)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting actor TTP profile: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/disclosure-timeline", response_model=List[DisclosureTimelinePoint], tags=["Analytics"])
async def api_disclosure_timeline():
    """Disclosure delay over time by country."""
    cache_key = "analytics:disclosure-timeline"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_disclosure_timeline(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting disclosure timeline: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/breach-by-institution-type", response_model=List[BreachByInstitutionItem], tags=["Analytics"])
async def api_breach_by_institution_type():
    """Breach rate and records per institution type."""
    cache_key = "analytics:breach-by-institution-type"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_breach_by_institution_type(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting breach by institution type: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Interactive Nivo Visualization Endpoints
# ============================================================

@app.get("/api/analytics/attack-flow", response_model=AttackFlowResponse, tags=["Analytics"])
async def api_attack_flow():
    """Sankey: Attack Vector → Category → Impact Outcome."""
    cache_key = "analytics:attack-flow"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_attack_flow(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting attack flow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/mitre-sunburst", response_model=MitreSunburstResponse, tags=["Analytics"])
async def api_mitre_sunburst():
    """Hierarchical MITRE ATT&CK sunburst: Tactic → Technique."""
    cache_key = "analytics:mitre-sunburst"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_mitre_sunburst(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting MITRE sunburst: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/actor-network", response_model=ActorNetworkResponse, tags=["Analytics"])
async def api_actor_network():
    """Force-directed network: actors linked by shared ransomware families."""
    cache_key = "analytics:actor-network"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_actor_network(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting actor network: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransom-flow", response_model=RansomFlowResponse, tags=["Analytics"])
async def api_ransom_flow():
    """Sankey: Institution Type → Ransomware Family → Payment Outcome."""
    cache_key = "analytics:ransom-flow"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_ransom_flow(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting ransom flow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/country-attack-matrix", response_model=CountryAttackMatrixResponse, tags=["Analytics"])
async def api_country_attack_matrix():
    """Country × Attack Category chord diagram data."""
    cache_key = "analytics:country-attack-matrix"
    cached = cache_get(cache_key, ttl_seconds=300)
    if cached is not None:
        return cached
    try:
        conn = get_api_connection()
        data = get_country_attack_matrix(conn)
        conn.close()
        cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Error getting country attack matrix: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Debug / Raw Data Viewer Endpoint (Admin)
# ============================================================

@app.get("/api/admin/raw-incidents", tags=["Admin"])
async def get_raw_incidents(
    incident_id: Optional[str] = Query(None, description="Filter by incident ID (partial match)"),
    has_mitre: Optional[bool] = Query(None, description="Filter by MITRE data presence"),
    attack_category: Optional[str] = Query(None, description="Filter by attack category"),
    country: Optional[str] = Query(None, description="Filter by country"),
    has_enrichment: Optional[bool] = Query(None, description="Filter by canonical final enrichment JSON presence"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Get raw incident data from all tables for debugging/inspection."""
    try:
        conn = get_api_connection()
        data = get_raw_incident_data(
            conn,
            incident_id=incident_id,
            has_mitre=has_mitre,
            attack_category=attack_category,
            country=country,
            has_enrichment=has_enrichment,
            limit=limit,
            offset=offset,
        )
        conn.close()
        return data
    except Exception as e:
        logger.error(f"Error getting raw incidents: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Schema Documentation
# ============================================================

@app.get(
    "/api/schema",
    response_model=SchemaResponse,
    tags=["Schema"],
    summary="Full database schema and pipeline reference",
)
async def get_schema():
    """Returns all tables, columns (with descriptions, types, and pipeline mappings),
    pipeline layers, and analytics endpoint documentation for the EduThreat-CTI database."""
    return SCHEMA_DOC


# ============================================================
# Run Server (for development)
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
