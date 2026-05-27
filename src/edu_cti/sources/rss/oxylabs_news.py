"""
Oxylabs News Search source for EduThreat-CTI.

Uses Oxylabs Google News SERP to discover education-sector cyber incidents
via keyword search. More reliable than Google News RSS (no redirect decoding,
no rate-limiting, supports historical date-range filtering).

Two modes:
- Incremental (daily): Fetches last 30 days of news for all queries
- Historical: Walks yearly date windows from HISTORICAL_START_YEAR to present

Cost: ~$1.00/1k Google SERP results. A full 22-query sweep returns ~220 results
= ~$0.22 per run. Historical sweep across 7 years × 22 queries = ~$1.54 total.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Callable, List, Optional
from urllib.parse import urlparse

from src.edu_cti.core.config import HISTORICAL_START_YEAR, NEWS_SEARCH_QUERIES_ALL
from src.edu_cti.core.discovery_policy import (
    QUERY_SCOPED_HIGH_RECALL,
    discovery_policy_for_source,
    record_source_discovery_metrics,
    semantic_prefilter_allowed,
)
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.oxylabs import OxylabsClient

logger = logging.getLogger(__name__)

SOURCE_NAME = "oxylabs_news"

# All queries (English + multilingual) are defined centrally in:
#   src/edu_cti/core/config.py → NEWS_SEARCH_QUERIES_EN + NEWS_SEARCH_QUERIES_MULTILINGUAL
# Edit config.py to add/modify queries — changes apply here automatically.
OXYLABS_QUERIES = NEWS_SEARCH_QUERIES_ALL

# Delay between Oxylabs API calls (we're well under rate limit but polite)
REQUEST_DELAY = 0.5


def _is_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _generate_yearly_windows(start_year: int) -> List[tuple]:
    """Generate yearly date windows from start_year to present."""
    windows = []
    now = datetime.utcnow()
    for year in range(start_year, now.year + 1):
        date_from = f"{year}-01-01"
        date_to = f"{year}-12-31" if year < now.year else now.strftime("%Y-%m-%d")
        windows.append((date_from, date_to, str(year)))
    return windows


def build_oxylabs_news_incidents(
    *,
    max_pages: Optional[int] = None,
    client=None,
    save_callback: Optional[Callable[[List[BaseIncident]], None]] = None,
    incremental: bool = True,
    max_age_days: int = 30,
) -> List[BaseIncident]:
    """
    Discover education-sector cyber incidents via Oxylabs Google News SERP.

    Args:
        max_pages: Not used (kept for interface compatibility)
        client: Not used (uses OxylabsClient internally)
        save_callback: Optional callback to save incidents incrementally
        incremental: If True, fetch last max_age_days only. If False, walk
                     yearly windows from HISTORICAL_START_YEAR to present.
        max_age_days: Days back to search in incremental mode (default: 30)

    Returns:
        List of BaseIncident objects
    """
    oxylabs = OxylabsClient()
    if not oxylabs._is_configured():
        record_source_discovery_metrics(
            SOURCE_NAME,
            {
                "rss_results_seen": 0,
                "source_rows_created": 0,
                "duplicates_skipped": 0,
                "invalid_url_skipped": 0,
                "out_of_window_skipped": 0,
                "semantic_skipped": 0,
            },
        )
        logger.warning(
            "Oxylabs not configured (OXYLABS_USERNAME/OXYLABS_PASSWORD missing) — "
            "skipping oxylabs_news source"
        )
        return []

    all_incidents: List[BaseIncident] = []
    seen_urls: set = set()
    now_iso = datetime.utcnow().isoformat()
    discovery_metrics = {
        "rss_results_seen": 0,
        "source_rows_created": 0,
        "duplicates_skipped": 0,
        "invalid_url_skipped": 0,
        "out_of_window_skipped": 0,
        "semantic_skipped": 0,
    }
    policy = discovery_policy_for_source(SOURCE_NAME)
    if policy == QUERY_SCOPED_HIGH_RECALL and semantic_prefilter_allowed(SOURCE_NAME):
        raise RuntimeError(f"{SOURCE_NAME} must not use semantic pre-filters")

    if incremental:
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        date_from = cutoff.strftime("%Y-%m-%d")
        date_to = datetime.utcnow().strftime("%Y-%m-%d")
        windows = [(date_from, date_to, f"last {max_age_days}d")]
        logger.info(f"Oxylabs News: incremental mode ({date_from} → {date_to})")
    else:
        windows = _generate_yearly_windows(HISTORICAL_START_YEAR)
        logger.info(
            f"Oxylabs News: historical mode — {len(windows)} yearly windows "
            f"from {HISTORICAL_START_YEAR} to present"
        )

    total_results = 0
    total_matched = 0

    for win_from, win_to, win_label in windows:
        if not incremental:
            logger.info(f"Oxylabs News: window {win_label} ({win_from} → {win_to})")

        window_incidents: List[BaseIncident] = []

        for query in OXYLABS_QUERIES:
            results = oxylabs.search_news(
                query,
                max_results=10,
                date_from=win_from,
                date_to=win_to,
            )
            total_results += len(results)
            discovery_metrics["rss_results_seen"] += len(results)

            for item in results:
                url = item.get("url", "")
                if not _is_http_url(url):
                    discovery_metrics["invalid_url_skipped"] += 1
                    continue
                if url in seen_urls:
                    discovery_metrics["duplicates_skipped"] += 1
                    continue

                title = item.get("title", "")
                description = item.get("description", "")
                source_name = item.get("source", "")

                seen_urls.add(url)
                total_matched += 1

                incident_id = make_incident_id(SOURCE_NAME, url)

                incident = BaseIncident(
                    incident_id=incident_id,
                    source=SOURCE_NAME,
                    source_event_id=url,
                    # Search-result headlines are too noisy to persist as the
                    # victim identity seed. Let downstream resolution/enrichment
                    # recover identity from better evidence instead.
                    institution_name="",
                    victim_raw_name=None,
                    institution_type=None,
                    country=None,
                    region=None,
                    city=None,
                    incident_date=None,  # LLM will extract from article
                    date_precision="unknown",
                    source_published_date=None,
                    ingested_at=now_iso,
                    title=title[:200],
                    subtitle=description[:300] if description else None,
                    primary_url=None,
                    all_urls=[url],
                    attack_type_hint=None,
                    status="suspected",
                    source_confidence="medium",
                    notes=(
                        f"source={source_name};query={query[:60]};window={win_label};"
                        f"discovery_policy={policy}"
                    ),
                    raw_source_payload={
                        "discovery_policy": policy,
                        "query": query,
                        "window": win_label,
                        "search_source": source_name,
                    },
                )

                window_incidents.append(incident)
                all_incidents.append(incident)
                discovery_metrics["source_rows_created"] += 1

                if save_callback:
                    save_callback([incident])

            time.sleep(REQUEST_DELAY)

        if not incremental:
            logger.info(
                f"  {win_label}: {len(window_incidents)} new incidents "
                f"({total_results} results scanned so far)"
            )

    record_source_discovery_metrics(SOURCE_NAME, discovery_metrics)
    logger.info(
        f"Oxylabs News complete: {total_results} results scanned, "
        f"{total_matched} candidates, {len(all_incidents)} unique incidents, "
        f"discovery_metrics={discovery_metrics}"
    )
    return all_incidents
