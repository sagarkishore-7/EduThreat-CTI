#!/usr/bin/env python3
"""
Script to revert all enriched incidents in the database.

This removes all LLM enrichment data and marks all incidents as unenriched,
allowing them to be processed again with the updated prompts.
"""

import logging
import sys
from pathlib import Path

# Add project root to path
# From src/edu_cti/pipeline/phase2/revert_enrichments.py
# Go up: phase2 -> pipeline -> edu_cti -> src -> project_root
script_path = Path(__file__).resolve()
project_root = script_path.parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Also try adding current working directory (in case script is run from project root)
cwd = Path.cwd()
if (cwd / "src" / "edu_cti").exists():
    sys.path.insert(0, str(cwd))

from src.edu_cti.core.db import get_connection
from src.edu_cti.pipeline.phase2.storage.db import revert_all_enriched_incidents, get_enrichment_stats
from src.edu_cti.core.logging_utils import configure_logging

logger = logging.getLogger(__name__)


def main():
    """Revert all enriched incidents in the database."""
    configure_logging()
    
    logger.info("=" * 80)
    logger.info("REVERTING ALL ENRICHED INCIDENTS")
    logger.info("=" * 80)
    
    # Get current stats
    conn = get_connection()
    stats_before = get_enrichment_stats(conn)
    logger.info(f"Before revert:")
    logger.info(f"  Enriched incidents: {stats_before.get('enriched_incidents', 0)}")
    logger.info(f"  Unenriched incidents: {stats_before.get('unenriched_incidents', 0)}")
    
    # Confirm with user
    enriched_count = stats_before.get('enriched_incidents', 0)
    if enriched_count == 0:
        logger.info("No enriched incidents to revert. Exiting.")
        return
    
    logger.warning(f"WARNING: This will revert {enriched_count} enriched incidents.")
    logger.warning("All enrichment data (summaries, timelines, MITRE ATT&CK, attack dynamics) will be deleted.")
    logger.warning("All articles will be deleted (they can be re-fetched).")
    
    response = input("\nAre you sure you want to continue? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        logger.info("Revert cancelled by user.")
        return
    
    # Revert all enriched incidents
    logger.info("\nReverting all enriched incidents...")
    reverted_count = revert_all_enriched_incidents(conn)
    
    # Get stats after revert
    stats_after = get_enrichment_stats(conn)
    logger.info(f"\nAfter revert:")
    logger.info(f"  Enriched incidents: {stats_after.get('enriched_incidents', 0)}")
    logger.info(f"  Unenriched incidents: {stats_after.get('unenriched_incidents', 0)}")
    
    logger.info(f"\n✓ Successfully reverted {reverted_count} enriched incidents")
    logger.info("All incidents are now marked as unenriched and can be processed again.")


if __name__ == "__main__":
    main()

