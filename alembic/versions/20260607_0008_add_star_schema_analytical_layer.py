"""add star-schema analytical layer (dimensions, fact, bridges, IOC)

Additive only: creates the controlled-vocabulary dimension tables, the
``fact_incident`` table, the multi-valued CTI bridge tables, and ``incident_ioc``.
Nothing existing is dropped or altered, so this is safe to apply on a live
database while the pipeline continues to run.

The tables are created from the registered ORM metadata
(``src/edu_cti_v2/models/star.py``) in dependency order with ``checkfirst`` so the
migration is idempotent and always matches the model definitions.
"""

from __future__ import annotations

from alembic import op

import src.edu_cti_v2.models  # noqa: F401  (register all ORM tables)
from src.edu_cti_v2.db.base import Base

revision = "20260607_0008"
down_revision = "20260523_0007"
branch_labels = None
depends_on = None

_STAR_TABLES = (
    "dim_institution_type",
    "dim_attack_category",
    "dim_attack_vector",
    "dim_severity",
    "dim_threat_actor",
    "dim_ransomware_family",
    "dim_data_category",
    "dim_system_impact",
    "dim_country",
    "dim_mitre_tactic",
    "dim_mitre_technique",
    "dim_cve",
    "dim_cwe",
    "dim_source",
    "fact_incident",
    "bridge_incident_data_category",
    "bridge_incident_system_impact",
    "bridge_incident_mitre_technique",
    "bridge_incident_cve",
    "bridge_incident_cwe",
    "bridge_incident_actor",
    "incident_ioc",
)


def upgrade() -> None:
    bind = op.get_bind()
    # sorted_tables gives FK-dependency order so targets are created first.
    ordered = [t for t in Base.metadata.sorted_tables if t.name in _STAR_TABLES]
    for table in ordered:
        table.create(bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    ordered = [t for t in Base.metadata.sorted_tables if t.name in _STAR_TABLES]
    for table in reversed(ordered):
        table.drop(bind, checkfirst=True)
