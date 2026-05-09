"""Analytics refresh services for the Postgres-backed v2 runtime."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session

from src.edu_cti_v2.repositories import AnalyticsRefreshRepository, CanonicalIncidentRepository
from src.edu_cti_v2.services.read_models import V2CanonicalReadService


class V2AnalyticsRefreshService:
    """Refresh lightweight persisted analytics snapshots from canonical incidents."""

    def __init__(
        self,
        *,
        canonical_repository: Optional[CanonicalIncidentRepository] = None,
        analytics_refresh_repository: Optional[AnalyticsRefreshRepository] = None,
        read_service: Optional[V2CanonicalReadService] = None,
    ) -> None:
        self.canonical_repository = canonical_repository or CanonicalIncidentRepository()
        self.analytics_refresh_repository = analytics_refresh_repository or AnalyticsRefreshRepository()
        self.read_service = read_service or V2CanonicalReadService(
            canonical_repository=self.canonical_repository,
            analytics_refresh_repository=self.analytics_refresh_repository,
        )

    def refresh_canonical_incident_snapshot(
        self,
        session: Session,
        canonical_incident_id,
    ) -> dict[str, Any]:
        detail = self.read_service.get_incident_detail(session, str(canonical_incident_id))
        if detail is None:
            return {
                "refreshed": False,
                "reason": "missing_canonical_incident",
                "canonical_incident_id": str(canonical_incident_id),
            }

        now = datetime.now(timezone.utc)
        canonical_snapshot = {
            "canonical_incident_id": detail["canonical_incident_id"],
            "display_name": detail["display_name"],
            "country_code": detail["country_code"],
            "incident_date": detail["incident_date"],
            "attack_category": detail["attack_category"],
            "membership_count": detail["membership_count"],
            "selected_source_enrichment_id": detail["selected_source_enrichment_id"],
            "timeline_count": len(detail["timeline"]),
            "last_seen_at": detail["last_seen_at"],
            "refreshed_at": now.isoformat(),
        }
        self.analytics_refresh_repository.upsert_snapshot(
            session,
            refresh_key=f"canonical:{detail['canonical_incident_id']}",
            refresh_scope="canonical_incident",
            state_payload=canonical_snapshot,
            needs_refresh=False,
            last_refreshed_at=now,
        )

        return {
            "refreshed": True,
            "canonical_incident_id": detail["canonical_incident_id"],
            "snapshot_scope": "canonical_incident",
            "snapshots_updated": 1,
        }

    def refresh_dashboard_snapshot(
        self,
        session: Session,
        *,
        last_trigger_canonical_incident_id: str | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)

        rollup = self.canonical_repository.get_dashboard_rollup(session)
        dashboard_snapshot = {
            "totals": rollup,
            "top_countries": self.canonical_repository.get_country_breakdown(session),
            "top_attack_categories": self.canonical_repository.get_attack_breakdown(session),
            "refreshed_at": now.isoformat(),
            "last_trigger_canonical_incident_id": last_trigger_canonical_incident_id,
        }
        self.analytics_refresh_repository.upsert_snapshot(
            session,
            refresh_key="dashboard:global",
            refresh_scope="global",
            state_payload=dashboard_snapshot,
            needs_refresh=False,
            last_refreshed_at=now,
        )

        return {
            "refreshed": True,
            "canonical_incident_id": last_trigger_canonical_incident_id,
            "snapshot_scope": "global",
            "dashboard_totals": rollup,
            "snapshots_updated": 1,
        }

    def refresh_for_canonical_incident(
        self,
        session: Session,
        canonical_incident_id,
    ) -> dict[str, Any]:
        canonical_result = self.refresh_canonical_incident_snapshot(
            session,
            canonical_incident_id,
        )
        if not canonical_result.get("refreshed"):
            return canonical_result

        dashboard_result = self.refresh_dashboard_snapshot(
            session,
            last_trigger_canonical_incident_id=str(canonical_incident_id),
        )
        return {
            "refreshed": True,
            "canonical_incident_id": str(canonical_incident_id),
            "dashboard_totals": dashboard_result.get("dashboard_totals"),
            "snapshots_updated": int(canonical_result.get("snapshots_updated", 0))
            + int(dashboard_result.get("snapshots_updated", 0)),
        }
