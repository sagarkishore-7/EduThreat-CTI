"""
One-time tool: populate all_urls for existing SecurityWeek incidents via title-based SERP.

SecurityWeek is behind a hard paywall — all 4 fetch tiers return login-gate HTML.
This tool uses the article title as a SERP query to find the same story on an open
news source, then saves the discovered URL to the incident's all_urls field so that
Phase 2 enrichment can fetch and process it on the next run.

Usage:
    railway ssh python3 -m src.edu_cti.tools.rescue_securityweek
    railway ssh python3 -m src.edu_cti.tools.rescue_securityweek --dry-run
    railway ssh python3 -m src.edu_cti.tools.rescue_securityweek --limit 50
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from typing import List, Optional

from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.oxylabs import OxylabsClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _get_securityweek_incidents(
    conn: sqlite3.Connection,
    limit: Optional[int],
) -> List[sqlite3.Row]:
    """Return unenriched SecurityWeek incidents that have no useful URLs."""
    query = """
        SELECT incident_id, title, incident_date, all_urls
        FROM incidents
        WHERE llm_enriched = 0
          AND incident_id LIKE 'securityweek_%'
          AND title IS NOT NULL
          AND title != ''
        ORDER BY incident_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return conn.execute(query).fetchall()


def _update_all_urls(
    conn: sqlite3.Connection,
    incident_id: str,
    new_url: str,
    dry_run: bool,
) -> None:
    if dry_run:
        logger.info(f"  [DRY] Would add URL for {incident_id}: {new_url[:80]}")
        return
    conn.execute(
        "UPDATE incidents SET all_urls = ? WHERE incident_id = ?",
        (new_url, incident_id),
    )
    conn.commit()
    logger.info(f"  Saved: {incident_id} -> {new_url[:80]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rescue SecurityWeek incidents via title-based SERP"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change without writing")
    parser.add_argument("--limit", type=int, default=None,
                        help="Maximum number of incidents to process")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Seconds between SERP calls (default: 2.0)")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    client = OxylabsClient()
    if not client._is_configured():
        logger.error("Oxylabs credentials not configured — set OXYLABS_USERNAME and OXYLABS_PASSWORD")
        conn.close()
        return

    rows = _get_securityweek_incidents(conn, args.limit)
    logger.info(f"Found {len(rows)} SecurityWeek incidents to rescue")
    if args.dry_run:
        logger.info("DRY RUN — no changes will be written")

    rescued = 0
    skipped = 0

    for i, row in enumerate(rows, 1):
        incident_id = row["incident_id"]
        title = row["title"]
        existing_urls = (row["all_urls"] or "").strip()

        logger.info(f"[{i}/{len(rows)}] {incident_id}")
        logger.info(f"  Title: {title[:100]}")

        if existing_urls and "securityweek.com" not in existing_urls:
            logger.info(f"  Already has non-SecurityWeek URL — skipping")
            skipped += 1
            continue

        # SERP query using the article title
        query = f'"{title}"'
        try:
            results = client.search_news(query, max_results=5)
        except Exception as e:
            logger.warning(f"  SERP error: {e}")
            skipped += 1
            continue

        # Pick first result that is NOT securityweek.com
        found_url = None
        for r in results:
            url = r.get("url", "")
            if url and "securityweek.com" not in url:
                found_url = url
                break

        if found_url:
            _update_all_urls(conn, incident_id, found_url, args.dry_run)
            rescued += 1
        else:
            logger.info(f"  No open-source URL found for '{title[:60]}'")
            skipped += 1

        if i < len(rows):
            time.sleep(args.delay)

    conn.close()
    logger.info(f"\n=== Summary ===")
    logger.info(f"Rescued: {rescued}")
    logger.info(f"Skipped: {skipped}")
    logger.info(f"Total:   {len(rows)}")


if __name__ == "__main__":
    main()
