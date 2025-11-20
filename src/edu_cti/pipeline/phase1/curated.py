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
) -> Dict[str, List[BaseIncident]]:
    """
    Run curated ingestors and return a mapping of source -> incidents.
    Supports incremental saving via save_callback.
    
    Curated sources are those with dedicated education sector sections/endpoints
    that contain only education-related incidents.
    
    Args:
        sources: List of source names to run. If None, runs all sources.
                 Valid sources: konbriefing, ransomwarelive, databreach
        max_pages: Maximum number of pages to fetch per source (only applies to databreach).
                   If None, fetches all pages.
        save_callback: Optional callback function to save incidents incrementally.
                      Called with batches of incidents as they are collected.
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
            
            # Check if builder supports save_callback
            import inspect
            sig = inspect.signature(builder_func)
            builder_kwargs = {}
            
            # Only databreach supports max_pages parameter
            if source_name == "databreach" and max_pages is not None:
                builder_kwargs["max_pages"] = max_pages
            
            if "save_callback" in sig.parameters and save_callback is not None:
                builder_kwargs["save_callback"] = save_callback
                incidents = builder_func(**builder_kwargs)
            else:
                # Builder doesn't support incremental saving yet
                # Collect all incidents, then save in batches
                incidents = builder_func(**builder_kwargs)
                
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
            logger.error(f"Error collecting incidents from {source_name}: {e}", exc_info=True)
            results[source_name] = []  # Return empty list on error
    
    return results


def run_curated_pipeline(
    *,
    sources: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    write_raw: bool = False,
) -> List[BaseIncident]:
    """
    Execute the curated pipeline, optionally writing per-source CSVs and returning all incidents.
    
    Args:
        sources: List of source names to run. If None, runs all sources.
        max_pages: Maximum number of pages to fetch per source (only applies to databreach).
                   If None, fetches all pages.
        write_raw: If True, write per-source CSV snapshots to raw/ directory (for debugging).
                   Default False for production efficiency.
    """
    all_incidents: List[BaseIncident] = []
    logger.info(
        "Collecting curated incidents (sources=%s, max_pages=%s, write_raw=%s)",
        sources if sources else "all",
        max_pages if max_pages else "all",
        write_raw,
    )
    results = collect_curated_incidents(sources=sources, max_pages=max_pages)

    for source, incidents in results.items():
        if write_raw:
            write_base_csv(RAW_CURATED_DIR / f"{source}_base.csv", incidents)
        all_incidents.extend(incidents)
        print(f"    {source}: {len(incidents)} incidents")

    return all_incidents
