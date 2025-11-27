"""
Phase 1: Curated Sources Pipeline

Collects incidents from curated education-sector specific sources:
- KonBriefing: University cyber attacks listing
- RansomwareLive: Ransomware victim tracker (education sector)
- DataBreaches.net: Education sector archive

Supports incremental ingestion via last_pubdate tracking.
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional, Sequence

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.sources import (
    CURATED_SOURCE_REGISTRY,
    get_curated_builder,
    validate_sources as validate_source_names,
)
from .base_io import RAW_CURATED_DIR, write_base_csv

logger = logging.getLogger(__name__)

# Alias for backward compatibility
CURATED_SOURCE_BUILDERS = CURATED_SOURCE_REGISTRY


def collect_curated_incidents(
    *,
    sources: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> Dict[str, List[BaseIncident]]:
    """
    Run curated ingestors and return a mapping of source -> incidents.
    
    Supports incremental ingestion:
    - incremental=True (default): Only fetch new incidents since last_pubdate
    - incremental=False: Full historical scrape (all pages/incidents)
    
    Curated sources are those with dedicated education sector sections/endpoints
    that contain only education-related incidents.
    
    Args:
        sources: List of source names to run. If None, runs all sources.
                 Valid sources: konbriefing, ransomwarelive, databreach
        max_pages: Maximum number of pages to fetch per source (only applies to databreach).
                   If None and incremental=False, fetches all pages.
        save_callback: Optional callback function to save incidents incrementally.
        incremental: If True, use incremental ingestion (stop at already-ingested articles)
    """
    # Determine which sources to run
    if sources is None:
        sources_to_run = list(CURATED_SOURCE_REGISTRY.keys())
    else:
        sources_to_run = validate_source_names("curated", sources)
    
    results = {}
    for source_name in sources_to_run:
        builder_func = get_curated_builder(source_name)
        if builder_func is None:
            logger.error(f"Builder function not found for curated source: {source_name}")
            results[source_name] = []
            continue
        try:
            logger.info(f"Collecting incidents from {source_name}...")
            
            # Check which parameters the builder supports
            import inspect
            sig = inspect.signature(builder_func)
            builder_kwargs = {}
            
            # Pass max_pages if supported (databreach)
            if "max_pages" in sig.parameters and max_pages is not None:
                builder_kwargs["max_pages"] = max_pages
            
            # Pass incremental flag if supported
            if "incremental" in sig.parameters:
                builder_kwargs["incremental"] = incremental
            
            # Pass save_callback if supported
            if "save_callback" in sig.parameters and save_callback is not None:
                builder_kwargs["save_callback"] = save_callback
                incidents = builder_func(**builder_kwargs)
            else:
                # Builder doesn't support incremental saving yet
                incidents = builder_func(**builder_kwargs)
                
                # Save in batches if callback provided
                if save_callback is not None and incidents:
                    batch_size = 50
                    for i in range(0, len(incidents), batch_size):
                        batch = incidents[i:i + batch_size]
                        try:
                            save_callback(batch)
                            logger.debug(f"{source_name}: Saved batch of {len(batch)} incidents")
                        except Exception as e:
                            logger.error(f"{source_name}: Error saving batch: {e}", exc_info=True)
            
            results[source_name] = incidents
            logger.info(f"{source_name}: collected {len(incidents)} incidents")
        except Exception as e:
            logger.error(f"Error collecting incidents from {source_name}: {e}", exc_info=True)
            results[source_name] = []
    
    return results


def run_curated_pipeline(
    *,
    sources: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    write_raw: bool = False,
    incremental: bool = True,
) -> List[BaseIncident]:
    """
    Execute the curated pipeline, optionally writing per-source CSVs.
    
    Args:
        sources: List of source names to run. If None, runs all sources.
        max_pages: Maximum number of pages to fetch per source.
        write_raw: If True, write per-source CSV snapshots.
        incremental: If True, use incremental ingestion.
    """
    all_incidents: List[BaseIncident] = []
    logger.info(
        "Collecting curated incidents (sources=%s, max_pages=%s, incremental=%s)",
        sources if sources else "all",
        max_pages if max_pages else "all",
        incremental,
    )
    results = collect_curated_incidents(
        sources=sources,
        max_pages=max_pages,
        incremental=incremental,
    )

    for source, incidents in results.items():
        if write_raw:
            write_base_csv(RAW_CURATED_DIR / f"{source}_base.csv", incidents)
        all_incidents.extend(incidents)
        print(f"    {source}: {len(incidents)} incidents")

    return all_incidents
