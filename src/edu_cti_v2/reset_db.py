"""Reset the Postgres-backed v2 schema and rebuild it from Alembic head."""

from __future__ import annotations

import argparse
import importlib

from src.edu_cti_v2.db import V2DatabaseSettings, create_engine_from_settings
from src.edu_cti_v2.migrate import build_alembic_config

_DROP_STATEMENTS = [
    "DROP TABLE IF EXISTS analytics_refresh_state CASCADE",
    "DROP TABLE IF EXISTS pipeline_tasks CASCADE",
    "DROP TABLE IF EXISTS pipeline_runs CASCADE",
    "DROP TABLE IF EXISTS canonical_timeline_events CASCADE",
    "DROP TABLE IF EXISTS canonical_enrichments CASCADE",
    "DROP TABLE IF EXISTS canonical_memberships CASCADE",
    "DROP TABLE IF EXISTS canonical_incidents CASCADE",
    "DROP TABLE IF EXISTS source_enrichments CASCADE",
    "DROP TABLE IF EXISTS article_fetch_attempts CASCADE",
    "DROP TABLE IF EXISTS article_documents CASCADE",
    "DROP TABLE IF EXISTS source_state CASCADE",
    "DROP TABLE IF EXISTS source_incident_urls CASCADE",
    "DROP TABLE IF EXISTS source_incidents CASCADE",
    "DROP TABLE IF EXISTS alembic_version CASCADE",
]


def reset_database(*, upgrade_revision: str = "head") -> None:
    settings = V2DatabaseSettings.from_env()
    engine = create_engine_from_settings(settings)
    try:
        with engine.begin() as connection:
            for statement in _DROP_STATEMENTS:
                connection.exec_driver_sql(statement)
        config = build_alembic_config()
        alembic_command = importlib.import_module("alembic.command")
        alembic_command.upgrade(config, upgrade_revision)
    finally:
        engine.dispose()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset the EduThreat-CTI v2 Postgres schema")
    parser.add_argument(
        "--revision",
        default="head",
        help="Alembic revision to rebuild after the reset",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    reset_database(upgrade_revision=args.revision)


if __name__ == "__main__":
    main()
