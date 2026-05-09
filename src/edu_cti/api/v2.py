"""Postgres-backed v2 API surface."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterator, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session, sessionmaker

from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.services import V2CanonicalReadService

router = APIRouter(prefix="/api/v2", tags=["V2"])


@lru_cache
def _get_v2_session_factory() -> sessionmaker[Session]:
    return create_session_factory()


def get_v2_session() -> Iterator[Session]:
    session_factory = _get_v2_session_factory()
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
    status: Optional[List[str]] = Query(None),
    session: Session = Depends(get_v2_session),
    read_service: V2CanonicalReadService = Depends(get_v2_read_service),
):
    """List recent canonical incidents from the v2 Postgres layer."""
    statuses = tuple(status) if status else ("open",)
    items = read_service.list_recent_incidents(
        session,
        limit=limit,
        statuses=statuses,
    )
    return {
        "items": items,
        "meta": {
            "limit": limit,
            "returned": len(items),
            "statuses": list(statuses),
        },
    }


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

