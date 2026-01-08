"""
EduThreat-CTI REST API

FastAPI application providing REST endpoints for the CTI dashboard.
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional
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
    RecoveryMetrics,
    TransparencyMetrics,
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
)

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
        from src.edu_cti.core.config import DB_PATH
        from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
        
        logger.info(f"Initializing database at: {DB_PATH}")
        print(f"[API] Initializing database at: {DB_PATH}", flush=True)
        
        # Ensure data directory exists
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database (creates tables if they don't exist)
        conn = get_connection()
        init_db(conn)
        
        # Initialize enrichment tables
        init_incident_enrichments_table(conn)
        
        conn.commit()
        conn.close()
        
        logger.info("Database initialized successfully")
        print("[API] ✓ Database initialized successfully", flush=True)
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        print(f"[API] ✗ Database initialization failed: {e}", flush=True)
        # Don't fail startup - let it try again on first request
    
    yield
    logger.info("Shutting down EduThreat-CTI API...")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="EduThreat-CTI API",
        description="""
        REST API for the EduThreat-CTI cyber threat intelligence platform.
        
        Provides access to education sector cyber incident data including:
        - Incident details with LLM-enriched CTI data
        - Timeline and MITRE ATT&CK mappings
        - Attack dynamics and impact metrics
        - Dashboard statistics and analytics
        """,
        version="1.0.0",
        lifespan=lifespan,
    )
    
    # CORS middleware for frontend access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    return app


app = create_app()

# Add exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle FastAPI validation errors with detailed logging."""
    logger.error(f"Validation error on {request.method} {request.url}: {exc.errors()}")
    print(f"[VALIDATION ERROR] {request.method} {request.url}: {exc.errors()}", flush=True)
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

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/health")
async def api_health_check():
    """API health check with database connectivity test."""
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


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint."""
    from src.edu_cti.core.metrics import get_metrics
    
    metrics = get_metrics()
    return Response(
        content=metrics.format_prometheus(),
        media_type="text/plain; version=0.0.4",
    )


# ============================================================
# Dashboard Endpoints
# ============================================================

@app.get("/api/dashboard", response_model=DashboardResponse)
async def get_dashboard():
    """
    Get complete dashboard data including stats, charts, and recent incidents.
    
    Returns aggregated statistics, incident distributions, and recent activity.
    """
    try:
        conn = get_api_connection()
        
        stats_data = get_dashboard_stats(conn)
        stats = DashboardStats(**stats_data)
        
        incidents_by_country = [
            CountByCategory(**c) for c in get_incidents_by_country(conn, limit=15)
        ]
        
        incidents_by_attack_type = [
            CountByCategory(**c) for c in get_incidents_by_attack_type(conn, limit=12)
        ]
        
        incidents_by_ransomware = [
            CountByCategory(**c) for c in get_incidents_by_ransomware_family(conn, limit=12)
        ]
        
        incidents_over_time = [
            TimeSeriesPoint(**t) for t in get_incidents_over_time(conn, months=24)
        ]
        
        recent_incidents = [
            RecentIncident(**i) for i in get_recent_incidents(conn, limit=10)
        ]
        
        conn.close()
        
        return DashboardResponse(
            stats=stats,
            incidents_by_country=incidents_by_country,
            incidents_by_attack_type=incidents_by_attack_type,
            incidents_by_ransomware=incidents_by_ransomware,
            incidents_over_time=incidents_over_time,
            recent_incidents=recent_incidents,
        )
        
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats", response_model=DashboardStats)
async def get_stats():
    """Get overall dashboard statistics."""
    try:
        conn = get_api_connection()
        stats_data = get_dashboard_stats(conn)
        conn.close()
        return DashboardStats(**stats_data)
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Incidents Endpoints
# ============================================================

@app.get("/api/incidents", response_model=IncidentListResponse)
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
            search=search,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        
        conn.close()
        
        # Convert to response models
        incidents = [
            IncidentSummary(
                incident_id=i["incident_id"],
                university_name=i["university_name"] or "Unknown",
                victim_raw_name=i.get("victim_raw_name"),
                institution_type=i.get("institution_type"),
                country=i.get("country"),
                region=i.get("region"),
                city=i.get("city"),
                incident_date=i.get("incident_date"),
                date_precision=i.get("date_precision"),
                title=i.get("title"),
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


@app.get("/api/incidents/{incident_id}", response_model=IncidentDetail)
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
            timeline = [TimelineEvent(**e) for e in incident_data["timeline"]]
        
        mitre_techniques = None
        if incident_data.get("mitre_attack_techniques"):
            mitre_techniques = [MITRETechnique(**t) for t in incident_data["mitre_attack_techniques"]]
        
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
        )
        
        user_impact = UserImpact(
            students_affected=incident_data.get("students_affected"),
            staff_affected=incident_data.get("staff_affected"),
            faculty_affected=incident_data.get("faculty_affected"),
            total_individuals_affected=incident_data.get("users_affected_exact"),
        )
        
        financial_impact = FinancialImpact(
            ransom_cost_usd=incident_data.get("ransom_amount"),
            recovery_cost_usd=incident_data.get("recovery_costs_max"),
            legal_cost_usd=incident_data.get("legal_costs"),
            insurance_claim=incident_data.get("insurance_claim"),
            insurance_payout_usd=incident_data.get("insurance_claim_amount"),
        )
        
        regulatory_impact = RegulatoryImpact(
            breach_notification_required=incident_data.get("breach_notification_required"),
            notification_sent=incident_data.get("notifications_sent"),
            fine_imposed=incident_data.get("fine_imposed"),
            fine_amount_usd=incident_data.get("fine_amount"),
            lawsuits_filed=incident_data.get("lawsuits_filed"),
            class_action_filed=incident_data.get("class_action"),
        )
        
        recovery_metrics = RecoveryMetrics(
            recovery_duration_days=incident_data.get("recovery_timeframe_days"),
            ir_firm_engaged=incident_data.get("incident_response_firm"),
        )
        
        transparency_metrics = TransparencyMetrics(
            public_disclosure=incident_data.get("public_disclosure"),
            public_disclosure_date=incident_data.get("public_disclosure_date"),
            disclosure_delay_days=incident_data.get("disclosure_delay_days"),
            transparency_level=incident_data.get("transparency_level"),
        )
        
        return IncidentDetail(
            incident_id=incident_data["incident_id"],
            university_name=incident_data.get("university_name") or "Unknown",
            victim_raw_name=incident_data.get("victim_raw_name"),
            institution_type=incident_data.get("institution_type"),
            country=incident_data.get("country"),
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
            attack_type_hint=incident_data.get("attack_type_hint"),
            attack_category=incident_data.get("attack_category"),
            status=incident_data.get("status", "suspected"),
            source_confidence=incident_data.get("source_confidence", "medium"),
            threat_actor_name=incident_data.get("threat_actor_name"),
            timeline=timeline,
            mitre_attack_techniques=mitre_techniques,
            attack_dynamics=attack_dynamics,
            data_impact=data_impact,
            system_impact=system_impact,
            user_impact=user_impact,
            financial_impact=financial_impact,
            regulatory_impact=regulatory_impact,
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

@app.get("/api/analytics/countries")
async def get_country_analytics(limit: int = Query(20, ge=1, le=100)):
    """Get incident counts by country."""
    try:
        conn = get_api_connection()
        data = get_incidents_by_country(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        return {"data": data, "total": total}
    except Exception as e:
        logger.error(f"Error getting country analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/attack-types")
async def get_attack_type_analytics(limit: int = Query(15, ge=1, le=50)):
    """Get incident counts by attack type."""
    try:
        conn = get_api_connection()
        data = get_incidents_by_attack_type(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        return {"data": data, "total": total}
    except Exception as e:
        logger.error(f"Error getting attack type analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/ransomware")
async def get_ransomware_analytics(limit: int = Query(15, ge=1, le=50)):
    """Get incident counts by ransomware family."""
    try:
        conn = get_api_connection()
        data = get_incidents_by_ransomware_family(conn, limit=limit)
        total = sum(d["count"] for d in data)
        conn.close()
        return {"data": data, "total": total}
    except Exception as e:
        logger.error(f"Error getting ransomware analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/timeline")
async def get_timeline_analytics(months: int = Query(24, ge=1, le=120)):
    """Get incident counts over time."""
    try:
        conn = get_api_connection()
        data = get_incidents_over_time(conn, months=months)
        total = sum(d["count"] for d in data)
        conn.close()
        return {"data": data, "total": total}
    except Exception as e:
        logger.error(f"Error getting timeline analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/analytics/threat-actors", response_model=ThreatActorsResponse)
async def get_threat_actor_analytics(limit: int = Query(20, ge=1, le=100)):
    """Get threat actor activity summary."""
    try:
        conn = get_api_connection()
        actors_data = get_threat_actors(conn, limit=limit)
        conn.close()
        
        actors = [ThreatActorSummary(**a) for a in actors_data]
        
        return ThreatActorsResponse(
            threat_actors=actors,
            total=len(actors),
        )
    except Exception as e:
        logger.error(f"Error getting threat actor analytics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Filter Options Endpoint
# ============================================================

@app.get("/api/filters", response_model=FilterOptions)
async def get_filters():
    """Get available filter options for the incidents list."""
    try:
        conn = get_api_connection()
        options = get_filter_options(conn)
        conn.close()
        return FilterOptions(**options)
    except Exception as e:
        logger.error(f"Error getting filter options: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Run Server (for development)
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

