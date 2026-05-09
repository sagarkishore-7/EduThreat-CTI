"""Pipeline runtime models for EduThreat-CTI v2."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class PipelineRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "pipeline_runs"

    run_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    service_name: Mapped[str] = mapped_column(Text, nullable=False)
    params: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    error: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "run_type IN ('collect', 'fetch', 'enrich', 'canonicalize', 'analytics_refresh', 'reenrich', 'maintenance')",
            name="pipeline_runs_run_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'paused')",
            name="pipeline_runs_status",
        ),
    )


class PipelineTask(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "pipeline_tasks"

    run_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("pipeline_runs.id", ondelete="SET NULL"),
    )
    task_type: Mapped[str] = mapped_column(Text, nullable=False)
    target_table: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[Optional[str]] = mapped_column(UUID(as_uuid=True))
    status: Mapped[str] = mapped_column(Text, nullable=False, default="queued")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    result: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    error: Mapped[Optional[str]] = mapped_column(Text)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_owner: Mapped[Optional[str]] = mapped_column(Text)
    lease_token: Mapped[Optional[str]] = mapped_column(UUID(as_uuid=True))
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)

    __table_args__ = (
        CheckConstraint(
            "task_type IN ('collect', 'resolve_url', 'fetch_article', 'enrich_source', 'canonicalize', 'refresh_analytics', 'reenrich')",
            name="pipeline_tasks_task_type",
        ),
        CheckConstraint(
            "status IN ('queued', 'leased', 'completed', 'failed', 'dead_letter', 'cancelled')",
            name="pipeline_tasks_status",
        ),
    )


class AnalyticsRefreshState(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "analytics_refresh_state"

    refresh_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    refresh_scope: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    needs_refresh: Mapped[bool] = mapped_column(nullable=False, default=True)
    last_refreshed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    state_payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
