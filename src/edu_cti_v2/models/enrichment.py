"""Enrichment models for EduThreat-CTI v2."""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class SourceEnrichment(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "source_enrichments"

    source_incident_id: Mapped[str] = mapped_column(
        ForeignKey("source_incidents.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    article_document_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("article_documents.id", ondelete="SET NULL"),
    )
    llm_provider: Mapped[Optional[str]] = mapped_column(Text)
    llm_model: Mapped[Optional[str]] = mapped_column(Text)
    prompt_version: Mapped[Optional[str]] = mapped_column(Text)
    schema_version: Mapped[Optional[str]] = mapped_column(Text)
    mapper_version: Mapped[Optional[str]] = mapped_column(Text)
    post_processing_version: Mapped[Optional[str]] = mapped_column(Text)
    raw_response: Mapped[Optional[dict]] = mapped_column(JSONB)
    raw_extraction: Mapped[Optional[dict]] = mapped_column(JSONB)
    typed_enrichment: Mapped[Optional[dict]] = mapped_column(JSONB)
    enrichment_confidence: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    is_education_related: Mapped[Optional[bool]] = mapped_column(Boolean)
    failed_reason: Mapped[Optional[str]] = mapped_column(Text)


class CanonicalEnrichment(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "canonical_enrichments"

    canonical_incident_id: Mapped[str] = mapped_column(
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    selected_source_enrichment_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source_enrichments.id", ondelete="SET NULL"),
    )
    merged_from_source_enrichment_ids: Mapped[List[str]] = mapped_column(
        ARRAY(UUID(as_uuid=True)),
        nullable=False,
        default=list,
    )
    canonical_projection: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    analytics_projection: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    field_provenance: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    completeness_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))


class CanonicalTimelineEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "canonical_timeline_events"

    canonical_incident_id: Mapped[str] = mapped_column(
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    seq_order: Mapped[int] = mapped_column(Integer, nullable=False)
    event_date: Mapped[Optional[date]] = mapped_column(Date)
    date_precision: Mapped[Optional[str]] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_description: Mapped[Optional[str]] = mapped_column(Text)
    actor_attribution: Mapped[Optional[str]] = mapped_column(Text)
    source_enrichment_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source_enrichments.id", ondelete="SET NULL"),
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
