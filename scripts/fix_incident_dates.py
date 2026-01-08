#!/usr/bin/env python3
"""
Fix incident dates from timeline data.

This script updates incident_date for all enriched incidents by extracting
the earliest date from their timeline events, if the current incident_date
appears to be a source published date rather than the actual incident date.
"""

import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.edu_cti.core.config import DB_PATH
from src.edu_cti.core.db import get_connection


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except:
        return None


def fix_incident_dates(dry_run: bool = True):
    """Fix incident dates from timeline data."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    
    try:
        # Get all enriched incidents with timeline data
        cur = conn.execute("""
            SELECT 
                i.incident_id,
                i.incident_date,
                i.date_precision,
                i.source_published_date,
                e.enrichment_data
            FROM incidents i
            JOIN incident_enrichments e ON i.incident_id = e.incident_id
            WHERE i.llm_enriched = 1
            AND e.enrichment_data IS NOT NULL
        """)
        
        incidents = cur.fetchall()
        print(f"Found {len(incidents)} enriched incidents to check")
        
        fixed_count = 0
        skipped_count = 0
        
        for row in incidents:
            incident_id = row["incident_id"]
            current_date = row["incident_date"]
            source_pub_date = row["source_published_date"]
            enrichment_data = json.loads(row["enrichment_data"])
            
            # Get timeline from enrichment
            timeline = enrichment_data.get("timeline", [])
            if not timeline:
                skipped_count += 1
                continue
            
            # Find earliest date in timeline
            dated_events = [e for e in timeline if e.get("date")]
            if not dated_events:
                skipped_count += 1
                continue
            
            earliest_event = min(dated_events, key=lambda e: e["date"])
            timeline_date = earliest_event["date"]
            timeline_precision = earliest_event.get("date_precision") or "approximate"
            
            # Check if current date is likely a published date (same as source_published_date)
            # or if timeline date is earlier (more likely to be the actual incident date)
            current_dt = parse_date(current_date) if current_date else None
            timeline_dt = parse_date(timeline_date) if timeline_date else None
            source_dt = parse_date(source_pub_date) if source_pub_date else None
            
            should_update = False
            
            if current_dt and timeline_dt:
                # Update if timeline date is earlier than current date
                # OR if current date matches source published date (likely wrong)
                if timeline_dt < current_dt:
                    should_update = True
                    reason = f"Timeline date ({timeline_date}) is earlier than current date ({current_date})"
                elif current_date == source_pub_date:
                    should_update = True
                    reason = f"Current date ({current_date}) matches source published date, using timeline date ({timeline_date})"
            
            if should_update:
                print(f"\n[{incident_id}]")
                print(f"  Current: {current_date} (precision: {row['date_precision']})")
                print(f"  Timeline earliest: {timeline_date} (precision: {timeline_precision})")
                print(f"  Source published: {source_pub_date}")
                print(f"  Reason: {reason}")
                
                if not dry_run:
                    conn.execute("""
                        UPDATE incidents
                        SET incident_date = ?,
                            date_precision = ?
                        WHERE incident_id = ?
                    """, (timeline_date, timeline_precision, incident_id))
                    conn.commit()
                    print(f"  ✓ Updated to {timeline_date}")
                else:
                    print(f"  [DRY RUN] Would update to {timeline_date}")
                
                fixed_count += 1
        
        print(f"\n{'='*60}")
        print(f"Summary:")
        print(f"  Fixed: {fixed_count}")
        print(f"  Skipped: {skipped_count} (no timeline or no dated events)")
        print(f"  Total checked: {len(incidents)}")
        if dry_run:
            print(f"\n  [DRY RUN] No changes made. Run with --apply to make changes.")
        print(f"{'='*60}")
        
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix incident dates from timeline data")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the changes (default is dry-run)"
    )
    
    args = parser.parse_args()
    
    fix_incident_dates(dry_run=not args.apply)
