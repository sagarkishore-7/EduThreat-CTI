"""Repository helpers for production campaign correlation tables."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional, Sequence
from uuid import UUID

from sqlalchemy import Select, delete, func, or_, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import (
    Campaign,
    CampaignEvidenceItem,
    CampaignMembership,
    CampaignSignature,
)


REVIEWED_MEMBERSHIP_STATUSES = {"true_positive", "false_positive", "uncertain"}


def _as_uuid(value: Any) -> UUID | None:
    if value is None or value == "":
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _as_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _json_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _serialize_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _serialize_decimal(value: Decimal | float | None) -> float | None:
    return float(value) if value is not None else None


class CampaignRepository:
    """Repository boundary for campaign hypotheses and their evidence."""

    @staticmethod
    def build_list_campaigns_stmt(
        *,
        statuses: Sequence[str] = ("analyst_reviewed",),
        campaign_type: Optional[str] = None,
        vendor: Optional[str] = None,
        platform: Optional[str] = None,
        actor: Optional[str] = None,
        cve: Optional[str] = None,
        min_confidence: Optional[float] = None,
        q: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Select:
        stmt = select(Campaign).where(Campaign.status.in_(list(statuses)))
        if campaign_type:
            stmt = stmt.where(Campaign.campaign_type == campaign_type)
        if vendor:
            stmt = stmt.where(Campaign.vendors.contains([vendor]))
        if platform:
            stmt = stmt.where(Campaign.platforms.contains([platform]))
        if actor:
            stmt = stmt.where(Campaign.actors.contains([actor]))
        if cve:
            stmt = stmt.where(Campaign.cves.contains([cve]))
        if min_confidence is not None:
            stmt = stmt.where(Campaign.confidence >= min_confidence)
        if q:
            pattern = f"%{q.strip()}%"
            stmt = stmt.where(
                or_(
                    Campaign.campaign_name.ilike(pattern),
                    Campaign.analyst_summary.ilike(pattern),
                )
            )
        return (
            stmt.order_by(
                Campaign.member_count.desc(),
                Campaign.confidence.desc().nullslast(),
                Campaign.last_correlated_at.desc().nullslast(),
            )
            .limit(limit)
            .offset(offset)
        )

    @staticmethod
    def build_count_campaigns_stmt(
        *,
        statuses: Sequence[str] = ("analyst_reviewed",),
        campaign_type: Optional[str] = None,
        vendor: Optional[str] = None,
        platform: Optional[str] = None,
        actor: Optional[str] = None,
        cve: Optional[str] = None,
        min_confidence: Optional[float] = None,
        q: Optional[str] = None,
    ) -> Select:
        stmt = select(func.count(Campaign.id)).where(Campaign.status.in_(list(statuses)))
        if campaign_type:
            stmt = stmt.where(Campaign.campaign_type == campaign_type)
        if vendor:
            stmt = stmt.where(Campaign.vendors.contains([vendor]))
        if platform:
            stmt = stmt.where(Campaign.platforms.contains([platform]))
        if actor:
            stmt = stmt.where(Campaign.actors.contains([actor]))
        if cve:
            stmt = stmt.where(Campaign.cves.contains([cve]))
        if min_confidence is not None:
            stmt = stmt.where(Campaign.confidence >= min_confidence)
        if q:
            pattern = f"%{q.strip()}%"
            stmt = stmt.where(
                or_(
                    Campaign.campaign_name.ilike(pattern),
                    Campaign.analyst_summary.ilike(pattern),
                )
            )
        return stmt

    @staticmethod
    def build_get_campaign_stmt(campaign_id: str, *, statuses: Optional[Sequence[str]] = None) -> Select:
        stmt = select(Campaign).where(Campaign.id == campaign_id).limit(1)
        if statuses:
            stmt = stmt.where(Campaign.status.in_(list(statuses)))
        return stmt

    @staticmethod
    def build_list_memberships_stmt(campaign_id: str, *, limit: int = 500) -> Select:
        return (
            select(CampaignMembership)
            .where(CampaignMembership.campaign_id == campaign_id)
            .order_by(CampaignMembership.confidence.desc().nullslast(), CampaignMembership.victim_name.asc().nullslast())
            .limit(limit)
        )

    @staticmethod
    def build_list_evidence_stmt(
        campaign_id: str,
        *,
        canonical_incident_id: str | None = None,
        limit: int = 1000,
    ) -> Select:
        stmt = (
            select(CampaignEvidenceItem)
            .where(CampaignEvidenceItem.campaign_id == campaign_id)
            .order_by(CampaignEvidenceItem.created_at.asc())
            .limit(limit)
        )
        if canonical_incident_id:
            stmt = stmt.where(CampaignEvidenceItem.canonical_incident_id == _as_uuid(canonical_incident_id))
        return stmt

    def get_campaign(
        self,
        session: Session,
        campaign_id: str,
        *,
        statuses: Optional[Sequence[str]] = None,
    ) -> Campaign | None:
        return session.execute(self.build_get_campaign_stmt(campaign_id, statuses=statuses)).scalar_one_or_none()

    def list_campaigns(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("analyst_reviewed",),
        campaign_type: Optional[str] = None,
        vendor: Optional[str] = None,
        platform: Optional[str] = None,
        actor: Optional[str] = None,
        cve: Optional[str] = None,
        min_confidence: Optional[float] = None,
        q: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Campaign], int]:
        items = list(
            session.execute(
                self.build_list_campaigns_stmt(
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
            )
            .scalars()
            .all()
        )
        total = int(
            session.execute(
                self.build_count_campaigns_stmt(
                    statuses=statuses,
                    campaign_type=campaign_type,
                    vendor=vendor,
                    platform=platform,
                    actor=actor,
                    cve=cve,
                    min_confidence=min_confidence,
                    q=q,
                )
            ).scalar_one()
            or 0
        )
        return items, total

    def list_memberships(self, session: Session, campaign_id: str, *, limit: int = 500) -> list[CampaignMembership]:
        return list(session.execute(self.build_list_memberships_stmt(campaign_id, limit=limit)).scalars().all())

    def list_evidence(
        self,
        session: Session,
        campaign_id: str,
        *,
        canonical_incident_id: str | None = None,
        limit: int = 1000,
    ) -> list[CampaignEvidenceItem]:
        return list(
            session.execute(
                self.build_list_evidence_stmt(
                    campaign_id,
                    canonical_incident_id=canonical_incident_id,
                    limit=limit,
                )
            )
            .scalars()
            .all()
        )

    def upsert_campaign(self, session: Session, payload: dict[str, Any], *, correlated_at: datetime) -> Campaign:
        campaign_id = str(payload["campaign_id"])
        existing = self.get_campaign(session, campaign_id)
        if existing is None:
            existing = Campaign(
                id=campaign_id,
                campaign_name=payload["campaign_name"],
                campaign_type=payload["campaign_type"],
                status="candidate",
                correlation_version=payload["correlation_version"],
            )
        if not existing.is_name_pinned:
            existing.campaign_name = payload["campaign_name"]
        existing.campaign_type = payload["campaign_type"]
        if existing.status not in {"analyst_reviewed", "suppressed"}:
            existing.status = payload.get("status") or "candidate"
        existing.first_seen_date = _as_date(payload.get("first_seen_date"))
        existing.last_seen_date = _as_date(payload.get("last_seen_date"))
        existing.actors = _json_list(payload.get("actors"))
        existing.vendors = _json_list(payload.get("vendors"))
        existing.platforms = _json_list(payload.get("platforms"))
        existing.cves = _json_list(payload.get("cves"))
        existing.campaign_names = _json_list(payload.get("campaign_names"))
        existing.attack_categories = _json_list(payload.get("attack_categories"))
        existing.member_count = int(payload.get("member_count") or 0)
        existing.confirmed_member_count = int(payload.get("confirmed_member_count") or 0)
        existing.evidence_only_member_count = int(payload.get("evidence_only_member_count") or 0)
        existing.confidence = _as_decimal(payload.get("confidence"))
        if not existing.analyst_summary:
            existing.analyst_summary = payload.get("analyst_summary")
        elif existing.status == "candidate":
            existing.analyst_summary = payload.get("analyst_summary")
        existing.correlation_version = payload["correlation_version"]
        existing.last_correlated_at = correlated_at
        existing.campaign_metadata = payload.get("metadata") or {}
        session.add(existing)
        return existing

    def upsert_membership(self, session: Session, payload: dict[str, Any]) -> CampaignMembership:
        campaign_id = str(payload["campaign_id"])
        canonical_uuid = _as_uuid(payload["canonical_incident_id"])
        stmt = (
            select(CampaignMembership)
            .where(CampaignMembership.campaign_id == campaign_id)
            .where(CampaignMembership.canonical_incident_id == canonical_uuid)
            .limit(1)
        )
        existing = session.execute(stmt).scalar_one_or_none()
        if existing is None:
            existing = CampaignMembership(
                campaign_id=campaign_id,
                canonical_incident_id=canonical_uuid,
                role=payload["role"],
            )
        existing.role = payload["role"]
        existing.confidence = _as_decimal(payload.get("confidence"))
        existing.evidence_article_ids = _json_list(payload.get("evidence_article_ids"))
        existing.evidence_source_incident_ids = _json_list(payload.get("evidence_source_incident_ids"))
        existing.evidence_quotes = _json_list(payload.get("evidence_quotes"))
        if existing.review_status not in REVIEWED_MEMBERSHIP_STATUSES:
            existing.review_status = payload.get("review_status") or "candidate_unreviewed"
        existing.victim_name = payload.get("victim_name")
        existing.canonical_status = payload.get("canonical_status")
        existing.reasons = _json_list(payload.get("reasons"))
        existing.membership_metadata = payload.get("metadata") or {}
        session.add(existing)
        return existing

    def replace_evidence_items(self, session: Session, campaign_ids: Sequence[str], rows: Sequence[dict[str, Any]]) -> int:
        if campaign_ids:
            session.execute(
                delete(CampaignEvidenceItem).where(CampaignEvidenceItem.campaign_id.in_(list(campaign_ids)))
            )
        count = 0
        for row in rows:
            item = CampaignEvidenceItem(
                id=row["id"],
                campaign_id=row["campaign_id"],
                canonical_incident_id=_as_uuid(row["canonical_incident_id"]),
                source_incident_id=_as_uuid(row.get("source_incident_id")),
                article_document_id=_as_uuid(row.get("article_document_id")),
                source_url=row.get("source_url"),
                source_title=row.get("source_title"),
                article_title=row.get("article_title"),
                evidence_quotes=_json_list(row.get("evidence_quotes")),
                vendors=_json_list(row.get("vendors")),
                platforms=_json_list(row.get("platforms")),
                actors=_json_list(row.get("actors")),
                cves=_json_list(row.get("cves")),
                evidence_payload=row.get("evidence_payload") or {},
            )
            session.add(item)
            count += 1
        return count

    def upsert_signature(self, session: Session, payload: dict[str, Any]) -> CampaignSignature:
        signature_id = str(payload["id"])
        existing = session.get(CampaignSignature, signature_id)
        if existing is None:
            existing = CampaignSignature(
                id=signature_id,
                campaign_id=payload["campaign_id"],
                correlation_version=payload["correlation_version"],
            )
        existing.status = payload.get("status") or "candidate"
        existing.signature_payload = payload.get("signature_payload") or {}
        existing.correlation_version = payload["correlation_version"]
        session.add(existing)
        return existing

    def update_campaign_review(
        self,
        session: Session,
        campaign_id: str,
        *,
        status: str | None = None,
        campaign_name: str | None = None,
        analyst_summary: str | None = None,
        analyst_notes: str | None = None,
    ) -> Campaign | None:
        campaign = self.get_campaign(session, campaign_id)
        if campaign is None:
            return None
        if status:
            campaign.status = status
        if campaign_name:
            campaign.campaign_name = campaign_name
            campaign.is_name_pinned = True
        if analyst_summary is not None:
            campaign.analyst_summary = analyst_summary
        if analyst_notes is not None:
            campaign.analyst_notes = analyst_notes
        campaign.updated_at = datetime.now(timezone.utc)
        session.add(campaign)
        return campaign

    def update_membership_review(
        self,
        session: Session,
        campaign_id: str,
        canonical_incident_id: str,
        *,
        review_status: str,
        role: str | None = None,
    ) -> CampaignMembership | None:
        stmt = (
            select(CampaignMembership)
            .where(CampaignMembership.campaign_id == campaign_id)
            .where(CampaignMembership.canonical_incident_id == _as_uuid(canonical_incident_id))
            .limit(1)
        )
        membership = session.execute(stmt).scalar_one_or_none()
        if membership is None:
            return None
        membership.review_status = review_status
        if role:
            membership.role = role
        membership.updated_at = datetime.now(timezone.utc)
        session.add(membership)
        return membership


def serialize_campaign(campaign: Campaign) -> dict[str, Any]:
    return {
        "campaign_id": campaign.id,
        "campaign_name": campaign.campaign_name,
        "campaign_type": campaign.campaign_type,
        "status": campaign.status,
        "first_seen_date": _serialize_date(campaign.first_seen_date),
        "last_seen_date": _serialize_date(campaign.last_seen_date),
        "actors": campaign.actors or [],
        "vendors": campaign.vendors or [],
        "platforms": campaign.platforms or [],
        "cves": campaign.cves or [],
        "campaign_names": campaign.campaign_names or [],
        "attack_categories": campaign.attack_categories or [],
        "member_count": campaign.member_count,
        "confirmed_member_count": campaign.confirmed_member_count,
        "evidence_only_member_count": campaign.evidence_only_member_count,
        "confidence": _serialize_decimal(campaign.confidence),
        "analyst_summary": campaign.analyst_summary,
        "analyst_notes": campaign.analyst_notes,
        "is_name_pinned": campaign.is_name_pinned,
        "correlation_version": campaign.correlation_version,
        "last_correlated_at": _serialize_datetime(campaign.last_correlated_at),
        "metadata": campaign.campaign_metadata or {},
        "created_at": _serialize_datetime(campaign.created_at),
        "updated_at": _serialize_datetime(campaign.updated_at),
    }


def serialize_membership(membership: CampaignMembership) -> dict[str, Any]:
    return {
        "membership_id": str(membership.id),
        "campaign_id": membership.campaign_id,
        "canonical_incident_id": str(membership.canonical_incident_id),
        "role": membership.role,
        "confidence": _serialize_decimal(membership.confidence),
        "evidence_article_ids": membership.evidence_article_ids or [],
        "evidence_source_incident_ids": membership.evidence_source_incident_ids or [],
        "evidence_quotes": membership.evidence_quotes or [],
        "review_status": membership.review_status,
        "victim_name": membership.victim_name,
        "canonical_status": membership.canonical_status,
        "reasons": membership.reasons or [],
        "metadata": membership.membership_metadata or {},
        "created_at": _serialize_datetime(membership.created_at),
        "updated_at": _serialize_datetime(membership.updated_at),
    }


def serialize_evidence_item(item: CampaignEvidenceItem) -> dict[str, Any]:
    return {
        "evidence_item_id": item.id,
        "campaign_id": item.campaign_id,
        "canonical_incident_id": str(item.canonical_incident_id),
        "source_incident_id": str(item.source_incident_id) if item.source_incident_id else None,
        "article_document_id": str(item.article_document_id) if item.article_document_id else None,
        "source_url": item.source_url,
        "source_title": item.source_title,
        "article_title": item.article_title,
        "evidence_quotes": item.evidence_quotes or [],
        "vendors": item.vendors or [],
        "platforms": item.platforms or [],
        "actors": item.actors or [],
        "cves": item.cves or [],
        "evidence_payload": item.evidence_payload or {},
        "created_at": _serialize_datetime(item.created_at),
        "updated_at": _serialize_datetime(item.updated_at),
    }
