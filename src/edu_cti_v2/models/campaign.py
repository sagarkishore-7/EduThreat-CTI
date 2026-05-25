"""Campaign correlation models for EduThreat-CTI v2."""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Integer, Numeric, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Campaign(TimestampMixin, Base):
    __tablename__ = "campaigns"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    campaign_name: Mapped[str] = mapped_column(Text, nullable=False)
    campaign_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="candidate")
    first_seen_date: Mapped[Optional[date]] = mapped_column(Date)
    last_seen_date: Mapped[Optional[date]] = mapped_column(Date)
    actors: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    vendors: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    platforms: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    cves: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    campaign_names: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    attack_categories: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    member_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confirmed_member_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    evidence_only_member_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 3))
    analyst_summary: Mapped[Optional[str]] = mapped_column(Text)
    analyst_notes: Mapped[Optional[str]] = mapped_column(Text)
    is_name_pinned: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    correlation_version: Mapped[str] = mapped_column(Text, nullable=False)
    last_correlated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    campaign_metadata: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    __table_args__ = (
        CheckConstraint(
            "campaign_type IN ("
            "'same_campaign', 'shared_vendor_incident', 'mass_exploitation', "
            "'actor_activity_wave', 'roundup_not_campaign', 'unrelated')",
            name="campaigns_campaign_type",
        ),
        CheckConstraint(
            "status IN ('candidate', 'analyst_reviewed', 'suppressed')",
            name="campaigns_status",
        ),
    )


class CampaignMembership(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "campaign_memberships"

    campaign_id: Mapped[str] = mapped_column(
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
    )
    canonical_incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 3))
    evidence_article_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    evidence_source_incident_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    evidence_quotes: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    review_status: Mapped[str] = mapped_column(Text, nullable=False, default="candidate_unreviewed")
    victim_name: Mapped[Optional[str]] = mapped_column(Text)
    canonical_status: Mapped[Optional[str]] = mapped_column(Text)
    reasons: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    membership_metadata: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("campaign_id", "canonical_incident_id", name="uq_campaign_membership_incident"),
        CheckConstraint(
            "role IN ('direct_victim', 'affected_via_vendor', 'vendor_operator', 'mentioned_only', 'needs_review')",
            name="campaign_memberships_role",
        ),
        CheckConstraint(
            "review_status IN ("
            "'candidate_unreviewed', 'true_positive', 'false_positive', 'uncertain', "
            "'excluded_evidence_only', 'manual_review_required')",
            name="campaign_memberships_review_status",
        ),
    )


class CampaignEvidenceItem(TimestampMixin, Base):
    __tablename__ = "campaign_evidence_items"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    campaign_id: Mapped[str] = mapped_column(
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
    )
    canonical_incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_incident_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    article_document_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    source_url: Mapped[Optional[str]] = mapped_column(Text)
    source_title: Mapped[Optional[str]] = mapped_column(Text)
    article_title: Mapped[Optional[str]] = mapped_column(Text)
    evidence_quotes: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    vendors: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    platforms: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    actors: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    cves: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    evidence_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)


class CampaignSignature(TimestampMixin, Base):
    __tablename__ = "campaign_signatures"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    campaign_id: Mapped[str] = mapped_column(
        ForeignKey("campaigns.id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(Text, nullable=False, default="candidate")
    signature_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    correlation_version: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "status IN ('candidate', 'analyst_reviewed', 'suppressed')",
            name="campaign_signatures_status",
        ),
    )
