"""
Comprehensive deduplication of existing incidents in the database.

Runs three passes in order:

  Pass 1 — Exact URL match
    If two incidents share the same primary_url (regardless of source or date),
    they describe the same event. Keep the one with better enrichment.

  Pass 2 — Exact URL + same date (subset of Pass 1, already covered)

  Pass 3 — Normalised institution name within a date window
    If two enriched incidents have the same normalised institution name and their
    dates are ≤ window_days apart, keep the better-enriched one.

Survivor selection (who to keep when merging):
  1. Prefer llm_enriched=1 over llm_enriched=0
  2. Among enriched, prefer longer llm_summary (more detail)
  3. Among equal quality, prefer source priority:
     curated (ransomwarelive, konbriefing, comparitech, databreach) >
     news (securityweek, therecord, darkreading, krebsonsecurity, thehackernews) >
     rss / other

Usage:
    # Dry run — shows what would be deleted
    railway ssh python3 -m src.edu_cti.tools.dedup_existing_incidents --dry-run

    # Run for real
    railway ssh python3 -m src.edu_cti.tools.dedup_existing_incidents

    # Adjust date window (default 14 days)
    railway ssh python3 -m src.edu_cti.tools.dedup_existing_incidents --window 7
"""

import argparse
import logging
import sqlite3
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.deduplication import normalize_url
from src.edu_cti.pipeline.phase2.utils.deduplication import normalize_institution_name as normalize_name

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Source priority (higher = preferred survivor)
SOURCE_PRIORITY: Dict[str, int] = {
    "ransomwarelive": 10,
    "konbriefing": 10,
    "comparitech": 10,
    "databreach": 9,
    "databreaches": 9,
    "securityweek": 7,
    "therecord": 7,
    "darkreading": 6,
    "krebsonsecurity": 6,
    "thehackernews": 6,
    "bleepingcomputer": 5,
    "databreaches_rss": 5,
    "cisa_rss": 4,
    "googlenews_rss": 3,
    "oxylabs_news": 3,
    "ransomlook": 2,
}



def source_prefix(incident_id: str) -> str:
    return incident_id.split("_")[0]


def survivor_score(row: sqlite3.Row) -> Tuple:
    """
    Return a tuple used to pick the best incident to keep.
    Higher = better. Compare with max().
    """
    enriched = int(row["llm_enriched"] or 0)
    summary_len = len(row["llm_summary"] or "")
    src = source_prefix(row["incident_id"])
    prio = SOURCE_PRIORITY.get(src, 1)
    return (enriched, summary_len, prio)


def delete_incident(conn: sqlite3.Connection, incident_id: str, dry_run: bool) -> None:
    if dry_run:
        logger.info(f"  [DRY] Would delete: {incident_id}")
    else:
        conn.execute("DELETE FROM incidents WHERE incident_id = ?", (incident_id,))
        logger.info(f"  Deleted: {incident_id}")


# ---------------------------------------------------------------------------
# Pass 1: same normalised primary_url
# ---------------------------------------------------------------------------

def pass1_url_dedup(
    conn: sqlite3.Connection, dry_run: bool
) -> Tuple[int, int]:
    """
    Group incidents by normalised primary_url.
    Within each group, keep the best incident and delete the rest.
    """
    rows = conn.execute(
        """
        SELECT incident_id, primary_url, incident_date, llm_enriched,
               llm_summary, university_name
        FROM incidents
        WHERE primary_url IS NOT NULL AND primary_url != ''
        """
    ).fetchall()

    url_groups: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        key = normalize_url(row["primary_url"])
        if key:
            url_groups[key].append(row)

    checked = 0
    deleted = 0
    for norm_url, group in url_groups.items():
        if len(group) <= 1:
            continue
        checked += 1
        group.sort(key=survivor_score, reverse=True)
        keeper = group[0]
        dupes = group[1:]
        logger.info(
            f"URL match ({len(group)} incidents) → keep {keeper['incident_id']} "
            f"({keeper['university_name']})"
        )
        for dupe in dupes:
            delete_incident(conn, dupe["incident_id"], dry_run)
            deleted += 1

    if not dry_run and deleted:
        conn.commit()

    return checked, deleted


# ---------------------------------------------------------------------------
# Pass 2: same normalised name + dates within window
# ---------------------------------------------------------------------------

def pass2_name_dedup(
    conn: sqlite3.Connection, window_days: int, dry_run: bool
) -> Tuple[int, int]:
    """
    Among enriched incidents: group by normalised institution name,
    then within each group merge pairs whose dates are ≤ window_days apart.
    """
    rows = conn.execute(
        """
        SELECT incident_id, university_name, victim_raw_name,
               incident_date, llm_enriched, llm_summary
        FROM incidents
        WHERE university_name IS NOT NULL
        ORDER BY incident_date
        """
    ).fetchall()

    # Build normalised-name → list of rows
    name_groups: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        name = row["university_name"] or row["victim_raw_name"] or ""
        key = normalize_name(name)
        if len(key) >= 4:  # skip very short keys
            name_groups[key].append(row)

    checked = 0
    deleted = 0
    deleted_ids: set = set()

    for norm_name, group in name_groups.items():
        if len(group) <= 1:
            continue

        # Within the group, find pairs within the date window
        # Use a greedy approach: sort by date, merge consecutive pairs in window
        group_sorted = sorted(
            group,
            key=lambda r: (r["incident_date"] or "0000-00-00"),
        )

        i = 0
        while i < len(group_sorted):
            if group_sorted[i]["incident_id"] in deleted_ids:
                i += 1
                continue

            anchor = group_sorted[i]
            anchor_date = anchor["incident_date"] or ""

            cluster = [anchor]
            for j in range(i + 1, len(group_sorted)):
                if group_sorted[j]["incident_id"] in deleted_ids:
                    continue
                other_date = group_sorted[j]["incident_date"] or ""
                if anchor_date and other_date:
                    try:
                        from datetime import datetime
                        d1 = datetime.strptime(anchor_date[:10], "%Y-%m-%d")
                        d2 = datetime.strptime(other_date[:10], "%Y-%m-%d")
                        if abs((d2 - d1).days) <= window_days:
                            cluster.append(group_sorted[j])
                        else:
                            break  # sorted by date, no point looking further
                    except ValueError:
                        pass

            if len(cluster) > 1:
                checked += 1
                cluster.sort(key=survivor_score, reverse=True)
                keeper = cluster[0]
                dupes = cluster[1:]
                logger.info(
                    f"Name match '{norm_name}' ({len(cluster)} incidents, "
                    f"window {window_days}d) → keep {keeper['incident_id']}"
                )
                for dupe in dupes:
                    if dupe["incident_id"] not in deleted_ids:
                        delete_incident(conn, dupe["incident_id"], dry_run)
                        deleted_ids.add(dupe["incident_id"])
                        deleted += 1

            i += 1

    if not dry_run and deleted:
        conn.commit()

    return checked, deleted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deduplicate existing incidents in the EduThreat-CTI database"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be deleted without deleting")
    parser.add_argument("--window", type=int, default=14,
                        help="Date window in days for name-based dedup (default: 14)")
    parser.add_argument("--skip-name", action="store_true",
                        help="Skip name-based dedup (only do URL dedup)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    total_before = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Incidents before: {total_before}")
    if args.dry_run:
        logger.info("DRY RUN — no changes will be made")

    # Pass 1: URL dedup
    logger.info("\n=== Pass 1: URL-based dedup ===")
    p1_checked, p1_deleted = pass1_url_dedup(conn, args.dry_run)
    logger.info(f"Pass 1 complete: {p1_checked} duplicate URL groups, {p1_deleted} deleted")

    # Pass 2: Name dedup
    if not args.skip_name:
        logger.info(f"\n=== Pass 2: Name-based dedup (window={args.window}d) ===")
        p2_checked, p2_deleted = pass2_name_dedup(conn, args.window, args.dry_run)
        logger.info(f"Pass 2 complete: {p2_checked} duplicate name groups, {p2_deleted} deleted")
    else:
        p2_deleted = 0

    total_after = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
    total_deleted = p1_deleted + p2_deleted
    logger.info(f"\n=== Summary ===")
    logger.info(f"Before: {total_before} incidents")
    logger.info(f"Deleted: {total_deleted}")
    logger.info(f"After:  {total_after if not args.dry_run else total_before - total_deleted} incidents")

    conn.close()


if __name__ == "__main__":
    main()
