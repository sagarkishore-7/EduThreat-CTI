"""
Phase 1: API Sources Pipeline

Collects incidents from free API-based sources:
- RansomWatch: Ransomware victim tracker (education filter)
- CISA KEV: Known Exploited Vulnerabilities catalog
- AlienVault OTX: Open Threat Exchange pulses

These sources are cost-free and don't require web scraping.
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Dict, List, Optional, Sequence

from src.edu_cti.core.logging_utils import bind_log_context, unbind_log_context
from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.sources import (
    API_SOURCE_REGISTRY,
    get_api_builder,
    validate_sources as validate_source_names,
)
from src.edu_cti.core.timeouts import guard_source_timeout

logger = logging.getLogger(__name__)


def collect_api_incidents(
    *,
    sources: Optional[Sequence[str]] = None,
    max_pages: Optional[int] = None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
) -> Dict[str, List[BaseIncident]]:
    """
    Run API-based source collectors and return a mapping of source -> incidents.

    Args:
        sources: List of source names to run. If None, runs all API sources.
        max_pages: Max pages (for OTX pagination)
        save_callback: Callback for incremental saving
        incremental: If True, only fetch recent data
    """
    if sources is None:
        sources_to_run = list(API_SOURCE_REGISTRY.keys())
    else:
        sources_to_run = validate_source_names("api", sources)

    results = {}
    for source_name in sources_to_run:
        builder_func = get_api_builder(source_name)
        if builder_func is None:
            logger.error(f"Builder function not found for API source: {source_name}")
            results[source_name] = []
            continue
        builder_func = guard_source_timeout(builder_func, label=f"api:{source_name}")
        bind_log_context(source=source_name, source_group="api")
        started = time.monotonic()
        try:
            logger.info("source_started")

            import inspect
            sig = inspect.signature(builder_func)
            builder_kwargs = {}

            if "max_pages" in sig.parameters and max_pages is not None:
                builder_kwargs["max_pages"] = max_pages
            if "incremental" in sig.parameters:
                builder_kwargs["incremental"] = incremental
            if "save_callback" in sig.parameters and save_callback is not None:
                builder_kwargs["save_callback"] = save_callback

            incidents = builder_func(**builder_kwargs)

            # Save in batches if callback provided and builder didn't use it
            if save_callback is not None and incidents and "save_callback" not in builder_kwargs:
                batch_size = 50
                for i in range(0, len(incidents), batch_size):
                    batch = incidents[i:i + batch_size]
                    try:
                        save_callback(batch)
                    except Exception as e:
                        logger.error(f"{source_name}: Error saving batch: {e}")

            results[source_name] = incidents
            logger.info(
                "source_completed",
                extra={
                    "incidents": len(incidents),
                    "elapsed_ms": round((time.monotonic() - started) * 1000),
                },
            )
        except Exception as e:
            logger.error(
                "source_failed",
                extra={
                    "error": str(e),
                    "elapsed_ms": round((time.monotonic() - started) * 1000),
                },
                exc_info=True,
            )
            results[source_name] = []
        finally:
            unbind_log_context("source", "source_group")

    return results
