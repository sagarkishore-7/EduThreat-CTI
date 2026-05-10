"""Persistent research-metrics snapshots for v2."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class ResearchMetricSnapshot(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "research_metric_snapshots"

    snapshot_key: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_scope: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    run_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("pipeline_runs.id", ondelete="SET NULL"),
    )
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
