"""Canonical incident models for EduThreat-CTI v2."""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, Numeric, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class CanonicalIncident(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "canonical_incidents"

    canonical_key: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="open")
    institution_name: Mapped[Optional[str]] = mapped_column(Text)
    institution_type: Mapped[Optional[str]] = mapped_column(Text)
    vendor_name: Mapped[Optional[str]] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(Text)
    country_code: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(Text)
    incident_date: Mapped[Optional[date]] = mapped_column(Date)
    date_precision: Mapped[Optional[str]] = mapped_column(Text)
    source_published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    attack_category: Mapped[Optional[str]] = mapped_column(Text)
    attack_vector: Mapped[Optional[str]] = mapped_column(Text)
    threat_actor_name: Mapped[Optional[str]] = mapped_column(Text)
    ransomware_family: Mapped[Optional[str]] = mapped_column(Text)
    is_education_related: Mapped[Optional[bool]] = mapped_column(Boolean)
    severity: Mapped[Optional[str]] = mapped_column(Text)
    canonical_summary: Mapped[Optional[str]] = mapped_column(Text)
    primary_source_incident_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source_incidents.id", ondelete="SET NULL"),
    )
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    resolution_version: Mapped[str] = mapped_column(Text, nullable=False, default="v2")
    resolution_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    memberships: Mapped[List["CanonicalMembership"]] = relationship(
        back_populates="canonical_incident",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        CheckConstraint("status IN ('open', 'excluded', 'merged', 'superseded')", name="canonical_incidents_status"),
        CheckConstraint(
            "date_precision IS NULL OR date_precision IN ('day', 'week', 'month', 'year', 'approximate', 'unknown')",
            name="canonical_incidents_date_precision",
        ),
    )


class CanonicalMembership(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "canonical_memberships"

    canonical_incident_id: Mapped[str] = mapped_column(
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_incident_id: Mapped[str] = mapped_column(
        ForeignKey("source_incidents.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    match_type: Mapped[str] = mapped_column(Text, nullable=False)
    match_score: Mapped[Optional[float]] = mapped_column(Numeric(7, 2))
    survivor_score: Mapped[Optional[float]] = mapped_column(Numeric(7, 2))
    is_primary_member: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    field_contribution: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    matcher_version: Mapped[str] = mapped_column(Text, nullable=False)
    matched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    canonical_incident: Mapped[CanonicalIncident] = relationship(back_populates="memberships")

    __table_args__ = (
        CheckConstraint(
            "match_type IN ('url_exact', 'url_resolved', 'name_date', 'vendor_platform', 'vendor_date', 'vendor_followup', 'exact_identity_same_event', 'manual', 'seed')",
            name="canonical_memberships_match_type",
        ),
        UniqueConstraint("source_incident_id", name="uq_canonical_memberships_source_incident_id"),
    )
