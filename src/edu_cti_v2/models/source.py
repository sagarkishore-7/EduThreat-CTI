"""Source observation models for EduThreat-CTI v2."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class SourceIncident(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "source_incidents"

    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_group: Mapped[str] = mapped_column(Text, nullable=False)
    source_event_key: Mapped[str] = mapped_column(Text, nullable=False)
    collector_version: Mapped[Optional[str]] = mapped_column(Text)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    raw_title: Mapped[Optional[str]] = mapped_column(Text)
    raw_subtitle: Mapped[Optional[str]] = mapped_column(Text)
    raw_victim_name: Mapped[Optional[str]] = mapped_column(Text)
    raw_institution_name: Mapped[Optional[str]] = mapped_column(Text)
    raw_institution_type: Mapped[Optional[str]] = mapped_column(Text)
    raw_country: Mapped[Optional[str]] = mapped_column(Text)
    raw_region: Mapped[Optional[str]] = mapped_column(Text)
    raw_city: Mapped[Optional[str]] = mapped_column(Text)
    raw_incident_date: Mapped[Optional[str]] = mapped_column(Text)
    raw_date_precision: Mapped[Optional[str]] = mapped_column(Text)
    raw_status: Mapped[Optional[str]] = mapped_column(Text)
    raw_attack_hint: Mapped[Optional[str]] = mapped_column(Text)
    raw_threat_actor: Mapped[Optional[str]] = mapped_column(Text)
    raw_notes: Mapped[Optional[str]] = mapped_column(Text)
    source_confidence: Mapped[Optional[str]] = mapped_column(Text)
    ingest_hash: Mapped[str] = mapped_column(Text, nullable=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # LLM title-relevance gate (replaces the keyword pre-filter for news/rss).
    # 'pending'    — awaiting bulk title classification (default for new rows)
    # 'relevant'   — keep: enqueue resolve/fetch (curated/api are relevant by construction)
    # 'irrelevant' — confident-negative title; never fetched, kept for audit/recall analysis
    relevance_status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="pending", default="pending"
    )
    title_relevance_score: Mapped[Optional[float]] = mapped_column(Float)
    title_relevance_reason: Mapped[Optional[str]] = mapped_column(Text)
    title_classified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    urls: Mapped[List["SourceIncidentUrl"]] = relationship(
        back_populates="source_incident",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("source_name", "source_event_key", name="uq_source_incidents_source_name"),
        CheckConstraint("source_group IN ('curated', 'news', 'rss', 'api')", name="source_group"),
        CheckConstraint(
            "relevance_status IN ('pending', 'relevant', 'irrelevant')",
            name="source_incidents_relevance_status",
        ),
        # Drives the classifier's batch sweep: pending news/rss rows by group.
        Index(
            "ix_source_incidents_relevance",
            "source_group",
            "relevance_status",
        ),
    )


class SourceIncidentUrl(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "source_incident_urls"

    source_incident_id: Mapped[str] = mapped_column(
        ForeignKey("source_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_url: Mapped[str] = mapped_column(Text, nullable=False)
    resolved_url: Mapped[Optional[str]] = mapped_column(Text)
    url_kind: Mapped[str] = mapped_column(Text, nullable=False, default="article")
    is_wrapper: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_primary_from_source: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_resolved_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    source_incident: Mapped[SourceIncident] = relationship(back_populates="urls")

    __table_args__ = (
        UniqueConstraint(
            "source_incident_id",
            "normalized_url",
            name="uq_source_incident_urls_source_incident_id",
        ),
        CheckConstraint(
            "url_kind IN ('article', 'detail', 'leak_site', 'screenshot', 'rss_wrapper', 'search_result', 'other')",
            name="source_incident_urls_url_kind",
        ),
    )


class SourceState(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "source_state"

    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    state_scope: Mapped[str] = mapped_column(Text, nullable=False, default="default")
    cursor_key: Mapped[str] = mapped_column(Text, nullable=False, default="default")
    state_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    last_seen_published_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("source_name", "state_scope", "cursor_key", name="uq_source_state_scope"),
    )
