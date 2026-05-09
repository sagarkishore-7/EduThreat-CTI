"""Postgres-backed v2 API surface."""

from __future__ import annotations

from functools import lru_cache
from datetime import date
from typing import Iterator, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session, sessionmaker

from src.edu_cti.api.reports import generate_cti_report
from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.services import V2CanonicalReadService

router = APIRouter(prefix="/api/v2", tags=["V2"])


@lru_cache
def get_v2_session_factory() -> sessionmaker[Session]:
    return create_session_factory()


def get_v2_session() -> Iterator[Session]:
    session_factory = get_v2_session_factory()
    with session_factory() as session:
        yield session


def get_v2_read_service() -> V2CanonicalReadService:
    return V2CanonicalReadService()


@router.get("/health")
async def v2_health() -> dict[str, str]:
    """Lightweight health check for the v2 Postgres read path."""
    return {"status": "healthy", "layer": "v2"}


@router.get("/dashboard")
async def get_v2_dashboard(
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return the cached or live v2 dashboard summary."""
    return read_service.get_dashboard_summary(session)


@router.get("/stats")
async def get_v2_stats(
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return the dashboard stats subset for compatibility with the old stats route."""
    return read_service.get_dashboard_stats(session)


@router.get("/incidents")
async def list_v2_incidents(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0, le=100000),
    status: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None, min_length=1, max_length=200),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2),
    attack_category: Optional[str] = Query(None),
    institution_type: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    is_education_related: Optional[bool] = Query(None),
    has_vendor: Optional[bool] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    sort_by: Literal["last_seen_at", "incident_date", "created_at", "institution_name", "country", "severity"] = Query("last_seen_at"),
    sort_order: Literal["asc", "desc"] = Query("desc"),
    format: Literal["default", "legacy"] = Query("default"),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """List recent canonical incidents from the v2 Postgres layer."""
    statuses = tuple(status) if status else ("open",)
    if format == "legacy":
        return read_service.list_legacy_incidents(
            session,
            limit=limit,
            offset=offset,
            statuses=statuses,
            search=search,
            country_code=country_code.upper() if country_code else None,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
    result = read_service.list_incidents(
        session,
        limit=limit,
        offset=offset,
        statuses=statuses,
        search=search,
        country_code=country_code.upper() if country_code else None,
        attack_category=attack_category,
        institution_type=institution_type,
        severity=severity,
        is_education_related=is_education_related,
        has_vendor=has_vendor,
        date_from=date_from,
        date_to=date_to,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return {
        "items": result["items"],
        "meta": {
            "limit": limit,
            "offset": offset,
            "returned": len(result["items"]),
            "total": result["total"],
            "statuses": list(statuses),
            "search": search,
            "country_code": country_code.upper() if country_code else None,
            "attack_category": attack_category,
            "institution_type": institution_type,
            "severity": severity,
            "is_education_related": is_education_related,
            "has_vendor": has_vendor,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "sort_by": sort_by,
            "sort_order": sort_order,
        },
    }


@router.get("/incidents/facets")
async def get_v2_incident_facets(
    status: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None, min_length=1, max_length=200),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2),
    attack_category: Optional[str] = Query(None),
    institution_type: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    is_education_related: Optional[bool] = Query(None),
    has_vendor: Optional[bool] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    facet_limit: int = Query(20, ge=1, le=100),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return filtered facet counts for canonical incidents from the v2 Postgres layer."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_incident_facets(
        session,
        statuses=statuses,
        search=search,
        country_code=country_code.upper() if country_code else None,
        attack_category=attack_category,
        institution_type=institution_type,
        severity=severity,
        is_education_related=is_education_related,
        has_vendor=has_vendor,
        date_from=date_from,
        date_to=date_to,
        facet_limit=facet_limit,
    )


@router.get("/analytics/breakdowns")
async def get_v2_analytics_breakdowns(
    status: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None, min_length=1, max_length=200),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2),
    attack_category: Optional[str] = Query(None),
    institution_type: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    is_education_related: Optional[bool] = Query(None),
    has_vendor: Optional[bool] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    breakdown_limit: int = Query(20, ge=1, le=100),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return filtered canonical breakdowns for frontend analytics charts."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_analytics_breakdowns(
        session,
        statuses=statuses,
        search=search,
        country_code=country_code.upper() if country_code else None,
        attack_category=attack_category,
        institution_type=institution_type,
        severity=severity,
        is_education_related=is_education_related,
        has_vendor=has_vendor,
        date_from=date_from,
        date_to=date_to,
        breakdown_limit=breakdown_limit,
    )


@router.get("/analytics/countries")
async def get_v2_country_analytics(
    limit: int = Query(20, ge=1, le=500),
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Compatibility endpoint for old country analytics shape."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_country_analytics(
        session,
        statuses=statuses,
        limit=limit,
    )


@router.get("/analytics/attack-types")
async def get_v2_attack_type_analytics(
    limit: int = Query(15, ge=1, le=50),
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Compatibility endpoint for old attack-type analytics shape."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_attack_type_analytics(
        session,
        statuses=statuses,
        limit=limit,
    )


@router.get("/analytics/ransomware")
async def get_v2_ransomware_analytics(
    limit: int = Query(15, ge=1, le=50),
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Compatibility endpoint for old ransomware analytics shape."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_ransomware_analytics(
        session,
        statuses=statuses,
        limit=limit,
    )


@router.get("/analytics/trend")
async def get_v2_analytics_trend(
    status: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None, min_length=1, max_length=200),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2),
    attack_category: Optional[str] = Query(None),
    institution_type: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    is_education_related: Optional[bool] = Query(None),
    has_vendor: Optional[bool] = Query(None),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    bucket: Literal["month", "week", "year"] = Query("month"),
    limit: int = Query(24, ge=1, le=120),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return a filtered incident trend series for frontend analytics charts."""
    statuses = tuple(status) if status else ("open",)
    return {
        "bucket": bucket,
        "items": read_service.get_incident_trend(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code.upper() if country_code else None,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            bucket=bucket,
            limit=limit,
        ),
    }


@router.get("/analytics/timeline")
async def get_v2_timeline_analytics(
    months: int = Query(24, ge=1, le=120),
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Compatibility endpoint for old timeline analytics shape."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_timeline_analytics(
        session,
        statuses=statuses,
        months=months,
    )


@router.get("/analytics/threat-actors")
async def get_v2_threat_actor_analytics(
    limit: int = Query(20, ge=1, le=500),
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Compatibility endpoint for old threat-actor analytics shape."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_threat_actor_analytics(
        session,
        statuses=statuses,
        limit=limit,
    )


@router.get("/analytics/intelligence")
async def get_v2_intelligence_analytics(
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return an analyst-focused intelligence summary from canonical incidents."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_intelligence_summary(
        session,
        statuses=statuses,
    )


@router.get("/filters")
async def get_v2_filter_options(
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Compatibility endpoint for incident list filter options."""
    statuses = tuple(status) if status else ("open",)
    return read_service.get_filter_options(
        session,
        statuses=statuses,
    )


@router.get("/incidents/{canonical_incident_id}")
async def get_v2_incident_detail(
    canonical_incident_id: str,
    format: Literal["default", "legacy"] = Query("default"),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return one canonical incident detail payload from the v2 Postgres layer."""
    detail = (
        read_service.get_legacy_incident_detail(session, canonical_incident_id)
        if format == "legacy"
        else read_service.get_incident_detail(session, canonical_incident_id)
    )
    if detail is None:
        raise HTTPException(status_code=404, detail="Canonical incident not found")
    return detail


@router.get("/incidents/{canonical_incident_id}/report")
async def get_v2_incident_report(
    canonical_incident_id: str,
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Generate a markdown CTI report from the v2 canonical incident detail."""
    detail = read_service.get_legacy_incident_detail(session, canonical_incident_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Canonical incident not found")

    report = generate_cti_report(detail)
    return Response(
        content=report,
        media_type="text/markdown",
        headers={
            "Content-Disposition": f'attachment; filename="cti-report-{canonical_incident_id}.md"'
        },
    )
