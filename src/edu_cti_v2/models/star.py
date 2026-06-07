"""Star-schema analytical layer for EduThreat-CTI v2.

This is an *additive* layer built from the operational tables (``canonical_incidents``,
``canonical_enrichments``) by ``services/star_projection.py``. It holds clean,
controlled-vocabulary keys so the corpus can be exported to CSV/JSON/STIX without
preprocessing and analyzed with plain SQL ``GROUP BY`` and joins.

Grain of ``fact_incident`` is one open canonical incident; its primary key is the
canonical incident id, so backfill and incremental refresh are idempotent upserts.
Single-valued categorical attributes reference surrogate-key dimension tables;
multi-valued CTI (data categories, MITRE techniques, CVEs, CWEs, actors, system
impact, IOCs) is modelled as bridge tables keyed on the canonical incident id.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Optional

from sqlalchemy import (
    Boolean,
    Date,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.edu_cti_v2.db.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


# ── Dimensions: surrogate-key controlled vocabularies ─────────────────────────

class _SlugDimension(UUIDPrimaryKeyMixin, TimestampMixin):
    """Mixin for surrogate-key dimensions keyed by a unique canonical slug."""

    slug: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    in_vocabulary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class DimInstitutionType(_SlugDimension, Base):
    __tablename__ = "dim_institution_type"


class DimAttackCategory(_SlugDimension, Base):
    __tablename__ = "dim_attack_category"
    family: Mapped[Optional[str]] = mapped_column(Text)  # ransomware/phishing/data_breach/...


class DimAttackVector(_SlugDimension, Base):
    __tablename__ = "dim_attack_vector"


class DimSeverity(_SlugDimension, Base):
    __tablename__ = "dim_severity"
    rank: Mapped[Optional[int]] = mapped_column(Integer)


class DimThreatActor(_SlugDimension, Base):
    __tablename__ = "dim_threat_actor"
    aliases: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)


class DimRansomwareFamily(_SlugDimension, Base):
    __tablename__ = "dim_ransomware_family"
    aliases: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)


class DimDataCategory(_SlugDimension, Base):
    __tablename__ = "dim_data_category"
    is_pii: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class DimSystemImpact(_SlugDimension, Base):
    __tablename__ = "dim_system_impact"


# ── Dimensions: natural-key reference data ────────────────────────────────────

class DimCountry(TimestampMixin, Base):
    __tablename__ = "dim_country"
    country_code: Mapped[str] = mapped_column(Text, primary_key=True)  # ISO 3166-1 alpha-2
    name: Mapped[str] = mapped_column(Text, nullable=False)
    region: Mapped[Optional[str]] = mapped_column(Text)
    subregion: Mapped[Optional[str]] = mapped_column(Text)


class DimMitreTactic(TimestampMixin, Base):
    __tablename__ = "dim_mitre_tactic"
    slug: Mapped[str] = mapped_column(Text, primary_key=True)
    tactic_id: Mapped[Optional[str]] = mapped_column(Text)  # TA00xx
    name: Mapped[str] = mapped_column(Text, nullable=False)
    ordinal: Mapped[Optional[int]] = mapped_column(Integer)  # kill-chain order


class DimMitreTechnique(TimestampMixin, Base):
    __tablename__ = "dim_mitre_technique"
    technique_id: Mapped[str] = mapped_column(Text, primary_key=True)  # T1078 / T1078.004
    name: Mapped[Optional[str]] = mapped_column(Text)
    tactic_slug: Mapped[Optional[str]] = mapped_column(Text)
    is_sub_technique: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parent_technique_id: Mapped[Optional[str]] = mapped_column(Text)


class DimCve(TimestampMixin, Base):
    __tablename__ = "dim_cve"
    cve_id: Mapped[str] = mapped_column(Text, primary_key=True)  # CVE-2023-34362
    year: Mapped[Optional[int]] = mapped_column(Integer)


class DimCwe(TimestampMixin, Base):
    __tablename__ = "dim_cwe"
    cwe_id: Mapped[str] = mapped_column(Text, primary_key=True)  # CWE-79
    name: Mapped[Optional[str]] = mapped_column(Text)


class DimSource(TimestampMixin, Base):
    __tablename__ = "dim_source"
    source_name: Mapped[str] = mapped_column(Text, primary_key=True)
    source_group: Mapped[Optional[str]] = mapped_column(Text)


# ── Fact table: one open canonical incident per row ───────────────────────────

class FactIncident(TimestampMixin, Base):
    __tablename__ = "fact_incident"

    canonical_incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # Descriptive (denormalized for export convenience)
    institution_name: Mapped[Optional[str]] = mapped_column(Text)
    vendor_name: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(Text)

    # Single-valued dimension foreign keys
    institution_type_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_institution_type.id", ondelete="SET NULL"))
    attack_category_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_attack_category.id", ondelete="SET NULL"))
    attack_vector_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_attack_vector.id", ondelete="SET NULL"))
    severity_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_severity.id", ondelete="SET NULL"))
    primary_actor_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_threat_actor.id", ondelete="SET NULL"))
    ransomware_family_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_ransomware_family.id", ondelete="SET NULL"))
    country_code: Mapped[Optional[str]] = mapped_column(
        Text, ForeignKey("dim_country.country_code", ondelete="SET NULL"))

    # Dates
    incident_date: Mapped[Optional[date]] = mapped_column(Date)
    detection_date: Mapped[Optional[date]] = mapped_column(Date)
    disclosure_date: Mapped[Optional[date]] = mapped_column(Date)
    incident_year: Mapped[Optional[int]] = mapped_column(Integer)
    incident_quarter: Mapped[Optional[int]] = mapped_column(Integer)

    # Timing measures (days)
    dwell_time_days: Mapped[Optional[int]] = mapped_column(Integer)
    disclosure_lag_days: Mapped[Optional[int]] = mapped_column(Integer)
    recovery_days: Mapped[Optional[int]] = mapped_column(Integer)
    downtime_days: Mapped[Optional[int]] = mapped_column(Integer)
    mttd_hours: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))
    mttr_hours: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))

    # Impact measures
    records_affected_exact: Mapped[Optional[int]] = mapped_column(Numeric(18, 0))
    records_affected_min: Mapped[Optional[int]] = mapped_column(Numeric(18, 0))
    records_affected_max: Mapped[Optional[int]] = mapped_column(Numeric(18, 0))
    ransom_demanded_usd: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))
    ransom_paid_usd: Mapped[Optional[float]] = mapped_column(Numeric(18, 2))

    # Boolean impact flags
    data_exfiltrated: Mapped[Optional[bool]] = mapped_column(Boolean)
    data_encrypted: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_vendor_breach: Mapped[Optional[bool]] = mapped_column(Boolean)
    teaching_disrupted: Mapped[Optional[bool]] = mapped_column(Boolean)
    research_disrupted: Mapped[Optional[bool]] = mapped_column(Boolean)

    # Confidence / quality (the three previously-ambiguous scores, named clearly)
    attribution_confidence: Mapped[Optional[str]] = mapped_column(Text)
    source_reliability: Mapped[Optional[str]] = mapped_column(Text)  # admiralty A-F
    enrichment_confidence: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    completeness_score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))
    source_count: Mapped[Optional[int]] = mapped_column(Integer)

    projection_version: Mapped[str] = mapped_column(Text, nullable=False, default="star-v1")

    __table_args__ = (
        Index("ix_fact_incident_attack_category", "attack_category_id"),
        Index("ix_fact_incident_institution_type", "institution_type_id"),
        Index("ix_fact_incident_country", "country_code"),
        Index("ix_fact_incident_year", "incident_year"),
    )


# ── Bridge tables: multi-valued CTI keyed on the canonical incident ───────────

class _IncidentBridge(UUIDPrimaryKeyMixin):
    """Mixin for incident bridges that cascade with the canonical incident."""

    canonical_incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("canonical_incidents.id", ondelete="CASCADE"),
        nullable=False,
    )


class BridgeIncidentDataCategory(_IncidentBridge, Base):
    __tablename__ = "bridge_incident_data_category"
    data_category_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_data_category.id", ondelete="CASCADE"), nullable=False)
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "data_category_id",
                         name="uq_bridge_incident_data_category"),
    )


class BridgeIncidentSystemImpact(_IncidentBridge, Base):
    __tablename__ = "bridge_incident_system_impact"
    system_impact_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_system_impact.id", ondelete="CASCADE"), nullable=False)
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "system_impact_id",
                         name="uq_bridge_incident_system_impact"),
    )


class BridgeIncidentMitreTechnique(_IncidentBridge, Base):
    __tablename__ = "bridge_incident_mitre_technique"
    technique_id: Mapped[str] = mapped_column(
        Text, ForeignKey("dim_mitre_technique.technique_id", ondelete="CASCADE"), nullable=False)
    tactic_slug: Mapped[Optional[str]] = mapped_column(Text)
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "technique_id",
                         name="uq_bridge_incident_mitre_technique"),
    )


class BridgeIncidentCve(_IncidentBridge, Base):
    __tablename__ = "bridge_incident_cve"
    cve_id: Mapped[str] = mapped_column(
        Text, ForeignKey("dim_cve.cve_id", ondelete="CASCADE"), nullable=False)
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "cve_id", name="uq_bridge_incident_cve"),
    )


class BridgeIncidentCwe(_IncidentBridge, Base):
    __tablename__ = "bridge_incident_cwe"
    cwe_id: Mapped[str] = mapped_column(
        Text, ForeignKey("dim_cwe.cwe_id", ondelete="CASCADE"), nullable=False)
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "cwe_id", name="uq_bridge_incident_cwe"),
    )


class BridgeIncidentActor(_IncidentBridge, Base):
    __tablename__ = "bridge_incident_actor"
    actor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_threat_actor.id", ondelete="CASCADE"), nullable=False)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "actor_id", name="uq_bridge_incident_actor"),
    )


class IncidentIoc(_IncidentBridge, Base):
    __tablename__ = "incident_ioc"
    ioc_type: Mapped[str] = mapped_column(Text, nullable=False)  # ipv4/ipv6/domain/url/email/md5/sha1/sha256/sha512
    value: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[Optional[float]] = mapped_column(Numeric(5, 3))
    source_enrichment_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("source_enrichments.id", ondelete="SET NULL"))
    __table_args__ = (
        UniqueConstraint("canonical_incident_id", "ioc_type", "value", name="uq_incident_ioc"),
        Index("ix_incident_ioc_value", "value"),
        Index("ix_incident_ioc_type", "ioc_type"),
    )
