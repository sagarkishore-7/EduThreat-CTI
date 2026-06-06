"""Reset the Postgres-backed v2 schema and rebuild it from Alembic head."""

from __future__ import annotations

import argparse
import importlib

from src.edu_cti_v2.db import V2DatabaseSettings, create_engine_from_settings
from src.edu_cti_v2.migrate import build_alembic_config

_DROP_STATEMENTS = [
    "DROP TABLE IF EXISTS research_metric_snapshots CASCADE",
    "DROP TABLE IF EXISTS analytics_refresh_state CASCADE",
    "DROP TABLE IF EXISTS pipeline_tasks CASCADE",
    "DROP TABLE IF EXISTS pipeline_runs CASCADE",
    "DROP TABLE IF EXISTS campaign_signatures CASCADE",
    "DROP TABLE IF EXISTS campaign_evidence_items CASCADE",
    "DROP TABLE IF EXISTS campaign_memberships CASCADE",
    "DROP TABLE IF EXISTS campaigns CASCADE",
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
            connection.exec_driver_sql("SET statement_timeout = 0")
            for statement in _DROP_STATEMENTS:
                connection.exec_driver_sql(statement)
        config = build_alembic_config()
        alembic_command = importlib.import_module("alembic.command")
        alembic_command.upgrade(config, upgrade_revision)
    finally:
        engine.dispose()


# Tables wiped by the selective reprocess reset (children → parents). The raw
# collection layer (source_incidents, source_incident_urls, article_documents,
# article_fetch_attempts, source_state) is intentionally preserved.
_REPROCESS_TRUNCATE = [
    "campaign_signatures",
    "campaign_evidence_items",
    "campaign_memberships",
    "campaigns",
    "canonical_timeline_events",
    "canonical_enrichments",
    "canonical_memberships",
    "canonical_incidents",
    "source_enrichments",
]
# Pipeline task types that drive enrichment/canonicalize/correlate — cleared so
# the reprocess starts from a clean queue. Collection tasks are left untouched.
_REPROCESS_TASK_TYPES = (
    "enrich_source",
    "reenrich",
    "canonicalize",
    "canonicalize_consistency",
    "campaign_correlate",
    "refresh_analytics",
)
# Derived source-incident stubs created during enrichment (roundup + vendor
# victim fan-out). Regenerated on reprocess, so they are deleted here.
_DERIVED_STUB_KINDS = ("roundup_secondary_stub", "vendor_victim_stub")


def reset_for_reprocess(*, confirm: bool = False) -> dict[str, int]:
    """Wipe enrichment/canonical/campaign data but KEEP the raw collection.

    Truncates derived tables, deletes derived source stubs, and clears
    reprocess pipeline tasks — so enrichment → canonicalize → correlate can be
    re-run with fixed logic over the retained `source_incidents` +
    `article_documents`. Raw collection counts are asserted unchanged (modulo the
    intentionally-deleted derived stubs).

    Returns a before/after integrity report. Raises if the raw collection would
    be altered beyond the expected stub deletions.
    """
    settings = V2DatabaseSettings.from_env()
    engine = create_engine_from_settings(settings)

    def _count(conn, table: str) -> int:
        return int(conn.exec_driver_sql(f"SELECT COUNT(*) FROM {table}").scalar() or 0)

    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("SET statement_timeout = 0")

            before = {
                t: _count(connection, t)
                for t in ("source_incidents", "source_incident_urls",
                          "article_documents", "article_fetch_attempts", "source_state")
            }
            stub_kinds = "(" + ",".join("'%s'" % k for k in _DERIVED_STUB_KINDS) + ")"
            stub_count = int(
                connection.exec_driver_sql(
                    f"SELECT COUNT(*) FROM source_incidents "
                    f"WHERE raw_payload->>'kind' IN {stub_kinds}"
                ).scalar()
                or 0
            )

            if not confirm:
                raise RuntimeError(
                    "Refusing to reset without confirm=True. This will wipe "
                    f"{', '.join(_REPROCESS_TRUNCATE)} and {stub_count} derived "
                    "source stub(s), keeping the raw collection."
                )

            # 1) Truncate derived tables (CASCADE handles internal FKs only).
            connection.exec_driver_sql(
                "TRUNCATE TABLE " + ", ".join(_REPROCESS_TRUNCATE) + " CASCADE"
            )
            # 2) Delete derived source stubs (their urls cascade via FK).
            connection.exec_driver_sql(
                f"DELETE FROM source_incidents WHERE raw_payload->>'kind' IN {stub_kinds}"
            )
            # 3) Clear reprocess pipeline tasks (keep collection tasks).
            task_in = "(" + ",".join("'%s'" % t for t in _REPROCESS_TASK_TYPES) + ")"
            deleted_tasks = int(
                connection.exec_driver_sql(
                    f"DELETE FROM pipeline_tasks WHERE task_type IN {task_in}"
                ).rowcount
                or 0
            )

            after = {
                t: _count(connection, t)
                for t in ("source_incidents", "source_incident_urls",
                          "article_documents", "article_fetch_attempts", "source_state")
            }

            # Integrity gate: raw collection unchanged except the deleted stubs.
            expected_incidents = before["source_incidents"] - stub_count
            if after["source_incidents"] != expected_incidents:
                raise RuntimeError(
                    f"Integrity check failed: source_incidents {before['source_incidents']} "
                    f"→ {after['source_incidents']} (expected {expected_incidents} after "
                    f"removing {stub_count} stubs). Rolling back."
                )
            if after["article_documents"] != before["article_documents"]:
                raise RuntimeError(
                    f"Integrity check failed: article_documents changed "
                    f"{before['article_documents']} → {after['article_documents']}. Rolling back."
                )

            report = {
                "stubs_deleted": stub_count,
                "tasks_deleted": deleted_tasks,
                **{f"{k}_before": v for k, v in before.items()},
                **{f"{k}_after": v for k, v in after.items()},
            }
            return report
    finally:
        engine.dispose()


def reseed_enrichment_tasks(*, batch_size: int = 1000) -> dict[str, int]:
    """Re-enqueue enrichment for every retained source incident.

    Run after ``reset_for_reprocess`` so the worker re-derives enrichments →
    canonicals → campaigns over the kept raw collection. For sources whose
    article is already fetched this enqueues ``enrich_source`` directly;
    otherwise it falls back to fetch/resolve. Idempotent — sources that already
    have an enrichment or an active task are skipped.
    """
    from sqlalchemy import select

    from src.edu_cti_v2.db import create_session_factory
    from src.edu_cti_v2.models import SourceIncident
    from src.edu_cti_v2.services.intake import V2IntakeService

    settings = V2DatabaseSettings.from_env()
    session_factory = create_session_factory(settings)
    intake = V2IntakeService()

    seeded = 0
    scanned = 0
    last_id = None
    while True:
        with session_factory() as session:
            stmt = (
                select(SourceIncident)
                .where(SourceIncident.is_deleted.is_(False))
                .order_by(SourceIncident.id)
                .limit(batch_size)
            )
            if last_id is not None:
                stmt = stmt.where(SourceIncident.id > last_id)
            rows = list(session.execute(stmt).scalars())
            if not rows:
                break
            for si in rows:
                last_id = si.id
                scanned += 1
                if intake.ensure_initial_processing_task(session, si) is not None:
                    seeded += 1
            session.commit()
        if len(rows) < batch_size:
            break
    return {"scanned": scanned, "tasks_seeded": seeded}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset the EduThreat-CTI v2 Postgres schema")
    parser.add_argument(
        "--revision",
        default="head",
        help="Alembic revision to rebuild after a full reset",
    )
    parser.add_argument(
        "--keep-collection",
        action="store_true",
        help=(
            "Selective reprocess reset: wipe enrichments/canonicals/campaigns + "
            "derived source stubs and clear reprocess tasks, but KEEP the raw "
            "collection (source_incidents, urls, article_documents, source_state)."
        ),
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required with --keep-collection to actually perform the deletion.",
    )
    parser.add_argument(
        "--reseed",
        action="store_true",
        help="After --keep-collection, re-enqueue enrichment tasks for all retained sources.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.keep_collection:
        report = reset_for_reprocess(confirm=args.confirm)
        print("Selective reprocess reset complete. Integrity report:")
        for key in sorted(report):
            print(f"  {key}: {report[key]}")
        if args.reseed:
            print("Re-seeding enrichment tasks for retained sources...")
            seed_report = reseed_enrichment_tasks()
            print(f"  scanned: {seed_report['scanned']}, tasks_seeded: {seed_report['tasks_seeded']}")
        return
    reset_database(upgrade_revision=args.revision)


if __name__ == "__main__":
    main()
