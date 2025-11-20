"""
RSS feed pipeline for EduThreat-CTI.

This module handles RSS feed sources that provide real-time incident data.
RSS feeds are filtered by category (e.g., "Education Sector") or keywords.
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Sequence

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.sources import (
    RSS_SOURCE_REGISTRY,
    get_rss_builder,
    validate_sources as validate_source_names,
)
from .base_io import RAW_RSS_DIR, write_base_csv

logger = logging.getLogger(__name__)


def collect_rss_incidents(
    *,
    sources: Optional[Sequence[str]] = None,
    max_age_days: int = 1,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> Dict[str, List[BaseIncident]]:
    """
    Run RSS feed ingestors and return a mapping of source -> incidents list.
    Supports incremental saving via save_callback.
    
    Args:
        sources: List of source names to run. If None, runs all RSS sources.
        max_age_days: Maximum age of items to include (default: 1 day)
        save_callback: Optional callback function to save incidents incrementally.
                      Called with batches of incidents as they are collected.
    """
    # Determine which sources to run
    if sources is None:
        sources_to_run = list(RSS_SOURCE_REGISTRY.keys())
    else:
        sources_to_run = validate_source_names("rss", sources)
    
    results = {}
    for source_name in sources_to_run:
        builder_func = get_rss_builder(source_name)
        if builder_func is None:
            logger.error(f"Builder function not found for RSS source: {source_name}")
            results[source_name] = []
            continue
        try:
            logger.info(f"Collecting incidents from RSS feed: {source_name}...")
            
            # Check if builder supports save_callback
            import inspect
            sig = inspect.signature(builder_func)
            builder_kwargs = {"max_age_days": max_age_days}
            
            if "save_callback" in sig.parameters and save_callback is not None:
                builder_kwargs["save_callback"] = save_callback
                incidents = builder_func(**builder_kwargs)
            else:
                # Builder doesn't support incremental saving yet
                # Collect all incidents, then save in batches
                incidents = builder_func(**{k: v for k, v in builder_kwargs.items() if k != "save_callback"})
                
                # Save in batches if callback provided
                if save_callback is not None and incidents:
                    batch_size = 50
                    for i in range(0, len(incidents), batch_size):
                        batch = incidents[i:i + batch_size]
                        try:
                            save_callback(batch)
                            logger.debug(f"{source_name}: Saved batch of {len(batch)} incidents ({i+1}-{min(i+batch_size, len(incidents))} of {len(incidents)})")
                        except Exception as e:
                            logger.error(f"{source_name}: Error saving batch: {e}", exc_info=True)
            
            results[source_name] = incidents
            logger.info(f"{source_name}: collected {len(incidents)} incidents")
        except Exception as e:
            logger.error(f"Error collecting incidents from RSS source {source_name}: {e}", exc_info=True)
            results[source_name] = []  # Return empty list on error
    
    return results


def run_rss_pipeline(
    *,
    sources: Optional[Sequence[str]] = None,
    max_age_days: int = 1,
    write_raw: bool = False,
) -> List[BaseIncident]:
    """
    Execute the RSS feed pipeline, optionally writing per-source CSV snapshots and returning incidents.
    
    Args:
        sources: List of source names to run. If None, runs all RSS sources.
        max_age_days: Maximum age of items to include (default: 1 day)
        write_raw: If True, write per-source CSV snapshots to raw/ directory (for debugging).
                   Default False for production efficiency.
    """
    all_incidents: List[BaseIncident] = []
    logger.info(
        "Collecting RSS feed incidents (sources=%s, max_age_days=%s, write_raw=%s)",
        sources if sources else "all",
        max_age_days,
        write_raw,
    )
    results = collect_rss_incidents(sources=sources, max_age_days=max_age_days)

    for source, incidents in results.items():
        if write_raw:
            logger.info("Writing %s incidents snapshot (%s rows)", source, len(incidents))
            write_base_csv(RAW_RSS_DIR / f"{source}_base.csv", incidents)
        all_incidents.extend(incidents)
        print(f"    {source}: {len(incidents)} incidents")

    return all_incidents

