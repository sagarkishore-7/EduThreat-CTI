"""Postgres-backed v2 API surface."""

from __future__ import annotations

from functools import lru_cache
from datetime import date
from typing import Iterator, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, sessionmaker

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
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """List recent canonical incidents from the v2 Postgres layer."""
    statuses = tuple(status) if status else ("open",)
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


@router.get("/incidents/{canonical_incident_id}")
async def get_v2_incident_detail(
    canonical_incident_id: str,
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """Return one canonical incident detail payload from the v2 Postgres layer."""
    detail = read_service.get_incident_detail(session, canonical_incident_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="Canonical incident not found")
    return detail
