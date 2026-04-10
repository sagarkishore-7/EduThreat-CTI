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

from src.edu_cti.core.config import HISTORICAL_START_YEAR, NEWS_SEARCH_QUERIES_ALL
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.core.oxylabs import OxylabsClient

logger = logging.getLogger(__name__)

SOURCE_NAME = "oxylabs_news"

# All queries (English + multilingual) are defined centrally in:
#   src/edu_cti/core/config.py → NEWS_SEARCH_QUERIES_EN + NEWS_SEARCH_QUERIES_MULTILINGUAL
# Edit config.py to add/modify queries — changes apply here automatically.
OXYLABS_QUERIES = NEWS_SEARCH_QUERIES_ALL

# ---- LEGACY BLOCK (kept only to avoid git diff noise) ----
# The following lists are no longer used — they were moved to config.py.
_DEPRECATED_ENGLISH_QUERIES = [
    "university cyber attack UK",
    "school ransomware Australia",
    "university data breach Canada",
    "university hacked India",
    "college data breach South Africa",
    "school cyberattack New Zealand",
]

# Multilingual queries — covers the 14 major languages from googlenews_rss
MULTILINGUAL_QUERIES = [
    # Spanish (ES / MX / AR / CO)
    "universidad ciberataque",
    "universidad ransomware",
    "escuela ataque cibernético",
    "universidad hackeo",
    "universidad brecha de datos",
    "colegio ciberataque",
    "datos estudiantes filtrados",
    # French (FR / CA / BE)
    "université cyberattaque",
    "université ransomware",
    "école piratage informatique",
    "université fuite de données",
    "lycée attaque informatique",
    "données étudiants volées",
    # German (DE / AT / CH)
    "universität cyberangriff",
    "hochschule ransomware",
    "schule hackerangriff",
    "universität datenleck",
    "schule datenpanne",
    "studenten daten gestohlen",
    # Portuguese (BR / PT)
    "universidade ataque cibernético",
    "universidade ransomware",
    "escola invasão hacker",
    "faculdade vazamento dados",
    "universidade dados estudantes",
    # Italian (IT)
    "università attacco informatico",
    "università ransomware",
    "scuola violazione dati",
    "università hacker attacco",
    # Dutch (NL / BE)
    "universiteit cyberaanval",
    "school ransomware aanval",
    "universiteit datalek",
    "hogeschool hackaanval",
    # Japanese (JP)
    "大学 サイバー攻撃",
    "大学 ランサムウェア",
    "学校 情報漏洩",
    "大学 不正アクセス",
    "教育機関 サイバー攻撃",
    # Korean (KR)
    "대학교 사이버공격",
    "학교 랜섬웨어",
    "대학 해킹",
    "학생 정보 유출",
    # Chinese (TW / CN)
    "大學 網路攻擊",
    "學校 勒索軟體",
    "大學 資料外洩",
    "教育機構 駭客攻擊",
    # Arabic (SA / AE / EG)
    "جامعة هجوم إلكتروني",
    "مدرسة اختراق إلكتروني",
    "جامعة برامج فدية",
    "بيانات طلاب مسربة",
    # Turkish (TR)
    "üniversite siber saldırı",
    "okul ransomware saldırısı",
    "üniversite veri ihlali",
    "okul bilgisayar saldırısı",
    # Polish (PL)
    "uniwersytet cyberatak",
    "szkoła ransomware",
    "uczelnia atak hakerski",
    "dane studentów wyciek",
    # Russian (RU)
    "университет кибератака",
    "школа хакерская атака",
    "вуз ransomware атака",
    "утечка данных студентов",
    # Hindi (IN — romanised, works in Google Search)
    "university cyber attack India",
    "school ransomware attack India",
    "college data breach India",
    "vishwavidyalaya cyber hamla",
]
# ---- END LEGACY BLOCK ----

# Delay between Oxylabs API calls (we're well under rate limit but polite)
REQUEST_DELAY = 0.5


def _is_cyber_relevant(text: str) -> bool:
    """Quick check if text contains cybersecurity-related terms."""
    from src.edu_cti.core.config import CYBER_KEYWORDS
    lowered = text.lower()
    return any(k.lower() in lowered for k in CYBER_KEYWORDS)


def _is_edu_relevant(text: str) -> bool:
    """Quick check if text contains education-related terms."""
    from src.edu_cti.core.config import EDUCATION_KEYWORDS
    lowered = text.lower()
    return any(k.lower() in lowered for k in EDUCATION_KEYWORDS)


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
        logger.warning(
            "Oxylabs not configured (OXYLABS_USERNAME/OXYLABS_PASSWORD missing) — "
            "skipping oxylabs_news source"
        )
        return []

    all_incidents: List[BaseIncident] = []
    seen_urls: set = set()
    now_iso = datetime.utcnow().isoformat()

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

            for item in results:
                url = item.get("url", "")
                if not url or url in seen_urls:
                    continue

                title = item.get("title", "")
                description = item.get("description", "")
                source_name = item.get("source", "")
                combined = f"{title} {description}"

                # Both filters must pass — must mention both cyber and edu terms
                if not _is_cyber_relevant(combined):
                    continue
                if not _is_edu_relevant(combined):
                    continue

                seen_urls.add(url)
                total_matched += 1

                incident_id = make_incident_id(SOURCE_NAME, url)

                incident = BaseIncident(
                    incident_id=incident_id,
                    source=SOURCE_NAME,
                    source_event_id=url,
                    university_name=title[:200],
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
                    notes=f"source={source_name};query={query[:60]};window={win_label}",
                )

                window_incidents.append(incident)
                all_incidents.append(incident)

                if save_callback:
                    save_callback([incident])

            time.sleep(REQUEST_DELAY)

        if not incremental:
            logger.info(
                f"  {win_label}: {len(window_incidents)} new incidents "
                f"({total_results} results scanned so far)"
            )

    logger.info(
        f"Oxylabs News complete: {total_results} results scanned, "
        f"{total_matched} passed filters, {len(all_incidents)} unique incidents"
    )
    return all_incidents
