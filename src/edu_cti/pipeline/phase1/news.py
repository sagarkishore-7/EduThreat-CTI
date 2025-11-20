from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Sequence

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.sources import (
    NEWS_SOURCE_REGISTRY,
    get_news_builder,
    validate_sources as validate_source_names,
)
from .base_io import RAW_NEWS_DIR, write_base_csv

logger = logging.getLogger(__name__)

# Alias for backward compatibility
NEWS_SOURCE_BUILDERS = NEWS_SOURCE_REGISTRY


def collect_news_incidents(
    *,
    max_pages: Optional[int] = None,
    sources: Optional[Sequence[str]] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
) -> Dict[str, List[BaseIncident]]:
    """
    Run news/RSS-style ingestors and return a mapping of source -> incidents list.
    Supports incremental saving via save_callback.
    
    Args:
        max_pages: Maximum number of pages to fetch per source (None = all pages)
        sources: List of source names to run. If None, runs all sources.
                 Valid sources: krebsonsecurity, thehackernews, therecord, 
                                databreach, securityweek, darkreading
        save_callback: Optional callback function to save incidents incrementally.
                      Called with batches of incidents as they are collected.
    """
    builder_args = (
        {"max_pages": max_pages} if max_pages is not None else {}
    )
    
    # If save_callback provided, add it to builder args
    if save_callback is not None:
        builder_args["save_callback"] = save_callback
    
    # Determine which sources to run
    if sources is None:
        sources_to_run = list(NEWS_SOURCE_REGISTRY.keys())
    else:
        sources_to_run = validate_source_names("news", sources)
    
    results = {}
    for source_name in sources_to_run:
        builder_func = get_news_builder(source_name)
        if builder_func is None:
            logger.error(f"Builder function not found for news source: {source_name}")
            results[source_name] = []
            continue
        try:
            logger.info(f"Collecting incidents from {source_name}...")
            # Check if builder supports save_callback
            import inspect
            sig = inspect.signature(builder_func)
            if "save_callback" in sig.parameters and save_callback is not None:
                # Pass save_callback to builder for incremental saving
                incidents = builder_func(**builder_args)
            else:
                # Builder doesn't support incremental saving yet
                # Collect all incidents, then save in batches to prevent data loss
                incidents = builder_func(**{k: v for k, v in builder_args.items() if k != "save_callback"})
                
                # Save in batches if callback provided (provides protection against mid-collection errors)
                if save_callback is not None and incidents:
                    batch_size = 50
                    for i in range(0, len(incidents), batch_size):
                        batch = incidents[i:i + batch_size]
                        try:
                            save_callback(batch)
                            logger.debug(f"{source_name}: Saved batch of {len(batch)} incidents ({i+1}-{min(i+batch_size, len(incidents))} of {len(incidents)})")
                        except Exception as e:
                            logger.error(f"{source_name}: Error saving batch: {e}", exc_info=True)
                            # Continue with next batch even if one fails
            
            results[source_name] = incidents
            logger.info(f"{source_name}: collected {len(incidents)} incidents")
        except Exception as e:
            logger.error(f"Error collecting incidents from {source_name}: {e}", exc_info=True)
            results[source_name] = []  # Return empty list on error
    
    return results


def run_news_pipeline(
    *,
    max_pages: Optional[int] = None,
    sources: Optional[Sequence[str]] = None,
    write_raw: bool = False,
) -> List[BaseIncident]:
    """
    Execute the news pipeline, optionally writing per-source CSV snapshots and returning incidents.
    
    Args:
        max_pages: Maximum number of pages to fetch per source (None = all pages)
        sources: List of source names to run. If None, runs all sources.
        write_raw: If True, write per-source CSV snapshots to raw/ directory (for debugging).
                   Default False for production efficiency.
    """
    all_incidents: List[BaseIncident] = []
    logger.info(
        "Collecting news/search incidents (max_pages=%s, sources=%s, write_raw=%s)",
        max_pages if max_pages is not None else "unbounded",
        sources if sources else "all",
        write_raw,
    )
    results = collect_news_incidents(max_pages=max_pages, sources=sources)

    for source, incidents in results.items():
        if write_raw:
            logger.info("Writing %s incidents snapshot (%s rows)", source, len(incidents))
            write_base_csv(RAW_NEWS_DIR / f"{source}_base.csv", incidents)
        all_incidents.extend(incidents)
        print(f"    {source}: {len(incidents)} incidents")

    return all_incidents

