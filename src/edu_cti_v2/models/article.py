"""Article fetch models for EduThreat-CTI v2."""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class ArticleDocument(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "article_documents"

    source_incident_id: Mapped[str] = mapped_column(
        ForeignKey("source_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_incident_url_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source_incident_urls.id", ondelete="SET NULL"),
    )
    title: Mapped[Optional[str]] = mapped_column(Text)
    author: Mapped[Optional[str]] = mapped_column(Text)
    publish_date: Mapped[Optional[date]] = mapped_column(Date)
    content_text: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    content_language: Mapped[Optional[str]] = mapped_column(Text)
    document_metadata: Mapped[dict] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        default=dict,
    )
    is_selected_for_enrichment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ArticleFetchAttempt(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "article_fetch_attempts"

    source_incident_id: Mapped[str] = mapped_column(
        ForeignKey("source_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    source_incident_url_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source_incident_urls.id", ondelete="SET NULL"),
    )
    fetch_tier: Mapped[str] = mapped_column(Text, nullable=False)
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    worker_id: Mapped[Optional[str]] = mapped_column(Text)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    http_status: Mapped[Optional[int]] = mapped_column(Integer)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer)
    content_length: Mapped[Optional[int]] = mapped_column(Integer)
    error_code: Mapped[Optional[str]] = mapped_column(Text)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    response_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
