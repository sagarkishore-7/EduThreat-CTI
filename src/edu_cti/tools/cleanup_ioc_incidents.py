"""
One-time cleanup script: delete IOC source incidents from the database.

IOC sources (threatfox, urlhaus, otx_alienvault, cisa_kev) have been moved to
sources/future_work/ and removed from the pipeline. Their incidents in the DB
are not useful for education CTI research — they link to abuse.ch, NVD, Censys
rather than news articles about attacks on educational institutions.

Usage:
    python -m src.edu_cti.tools.cleanup_ioc_incidents [--dry-run]

On Railway:
    railway run python -m src.edu_cti.tools.cleanup_ioc_incidents
"""

import argparse
import logging
import sys

from src.edu_cti.core.db import get_connection
from src.edu_cti.core.config import DB_PATH

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

IOC_PREFIXES = ["threatfox", "urlhaus", "otx_alienvault", "cisa_kev"]


def count_ioc_incidents(conn) -> dict:
    """Count incidents per IOC source prefix."""
    counts = {}
    for prefix in IOC_PREFIXES:
        row = conn.execute(
            "SELECT COUNT(*) FROM incidents WHERE incident_id LIKE ?",
            (f"{prefix}_%",),
        ).fetchone()
        counts[prefix] = row[0]
    return counts


def delete_ioc_incidents(conn, dry_run: bool = False) -> dict:
    """Delete all IOC source incidents (cascades to related tables)."""
    deleted = {}
    for prefix in IOC_PREFIXES:
        count_row = conn.execute(
            "SELECT COUNT(*) FROM incidents WHERE incident_id LIKE ?",
            (f"{prefix}_%",),
        ).fetchone()
        count = count_row[0]

        if not dry_run and count > 0:
            conn.execute(
                "DELETE FROM incidents WHERE incident_id LIKE ?",
                (f"{prefix}_%",),
            )
        deleted[prefix] = count

    if not dry_run:
        conn.commit()

    return deleted


def main():
    parser = argparse.ArgumentParser(description="Delete IOC source incidents from the DB")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    args = parser.parse_args()

    logger.info(f"Database: {DB_PATH}")

    conn = get_connection()

    logger.info("Counting IOC incidents before cleanup...")
    counts = count_ioc_incidents(conn)
    total = sum(counts.values())

    for prefix, count in counts.items():
        logger.info(f"  {prefix}: {count} incidents")
    logger.info(f"  TOTAL: {total} incidents")

    if total == 0:
        logger.info("No IOC incidents found — database is already clean.")
        conn.close()
        sys.exit(0)

    if args.dry_run:
        logger.info("[DRY RUN] Would delete all of the above incidents (and cascaded rows).")
        conn.close()
        sys.exit(0)

    logger.info("Deleting IOC incidents (DELETE CASCADE will clean up related tables)...")
    deleted = delete_ioc_incidents(conn, dry_run=False)

    for prefix, count in deleted.items():
        if count > 0:
            logger.info(f"  Deleted {count} incidents from {prefix}")

    logger.info(f"Cleanup complete: {sum(deleted.values())} incidents removed.")
    conn.close()


if __name__ == "__main__":
    main()
