"""
Repair incidents where the LLM incorrectly overrode incident_date with a date
significantly later than source_published_date (e.g. from a repost/mirror URL).

Also fixes primary_url when it was set to a SERP-discovered URL that is not in all_urls.

Run locally against a downloaded copy of the DB or against Railway via the admin API.

Usage:
    # Dry-run (shows what would be fixed, no writes):
    python3 scripts/repair_corrupted_incidents.py --db data/eduthreat.db --dry-run

    # Apply fixes:
    python3 scripts/repair_corrupted_incidents.py --db data/eduthreat.db

    # Optionally mark fixed incidents for re-enrichment:
    python3 scripts/repair_corrupted_incidents.py --db data/eduthreat.db --re-enrich
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, timedelta


FORWARD_THRESHOLD_DAYS = 90  # same guard used in db.py


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def find_corrupted(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return incidents where incident_date is >90 days after source_published_date."""
    return conn.execute(
        """
        SELECT incident_id, incident_date, source_published_date, primary_url, all_urls
        FROM incidents
        WHERE incident_date IS NOT NULL
          AND source_published_date IS NOT NULL
          AND julianday(substr(incident_date, 1, 10))
              - julianday(substr(source_published_date, 1, 10)) > ?
        ORDER BY (
            julianday(substr(incident_date, 1, 10))
            - julianday(substr(source_published_date, 1, 10))
        ) DESC
        """,
        (FORWARD_THRESHOLD_DAYS,),
    ).fetchall()


def find_wrong_primary_url(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return incidents where primary_url is not in all_urls (and all_urls is non-empty)."""
    rows = conn.execute(
        """
        SELECT incident_id, primary_url, all_urls
        FROM incidents
        WHERE primary_url IS NOT NULL
          AND all_urls IS NOT NULL
          AND all_urls != '[]'
          AND all_urls != ''
        """
    ).fetchall()
    bad = []
    for row in rows:
        try:
            urls = json.loads(row["all_urls"])
        except (json.JSONDecodeError, TypeError):
            continue
        if urls and row["primary_url"] not in urls:
            bad.append(row)
    return bad


def repair(conn: sqlite3.Connection, dry_run: bool, re_enrich: bool) -> None:
    corrupted_dates = find_corrupted(conn)
    print(f"\n=== Date corruption: {len(corrupted_dates)} incidents ===")
    for row in corrupted_dates:
        iid = row["incident_id"]
        old_date = row["incident_date"]
        new_date = row["source_published_date"]
        gap = (
            date.fromisoformat(str(old_date)[:10])
            - date.fromisoformat(str(new_date)[:10])
        ).days
        print(f"  {iid}: {old_date} → {new_date}  (gap={gap}d)")
        if not dry_run:
            fields = "incident_date = ?, date_precision = 'approximate'"
            params: list = [new_date]
            if re_enrich:
                fields += ", llm_enriched = 0, llm_enriched_at = NULL"
            conn.execute(
                f"UPDATE incidents SET {fields} WHERE incident_id = ?",
                (*params, iid),
            )

    wrong_urls = find_wrong_primary_url(conn)
    print(f"\n=== Wrong primary_url: {len(wrong_urls)} incidents ===")
    for row in wrong_urls:
        iid = row["incident_id"]
        try:
            urls = json.loads(row["all_urls"])
        except (json.JSONDecodeError, TypeError):
            continue
        correct_url = urls[0]
        print(f"  {iid}:")
        print(f"    was:    {row['primary_url']}")
        print(f"    fixing: {correct_url}")
        if not dry_run:
            conn.execute(
                "UPDATE incidents SET primary_url = ? WHERE incident_id = ?",
                (correct_url, iid),
            )

    if not dry_run:
        conn.commit()
        print(f"\nFixed {len(corrupted_dates)} date(s) and {len(wrong_urls)} primary_url(s).")
        if re_enrich:
            print("Marked date-corrupted incidents for re-enrichment (llm_enriched=0).")
    else:
        print("\n[DRY RUN] No changes written.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair corrupted incident dates and primary URLs")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be changed without writing")
    parser.add_argument("--re-enrich", action="store_true", help="Mark date-fixed incidents for re-enrichment")
    args = parser.parse_args()

    conn = connect(args.db)
    try:
        repair(conn, dry_run=args.dry_run, re_enrich=args.re_enrich)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
