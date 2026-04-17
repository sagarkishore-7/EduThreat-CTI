"""
Central configuration constants for EduThreat-CTI.

Supports environment variables for configuration:
- EDU_CTI_DB_PATH: Database file path (default: data/eduthreat.db)
- EDU_CTI_LOG_LEVEL: Logging level (default: INFO)
- EDU_CTI_LOG_FILE: Log file path (default: logs/pipeline.log)
- EDU_CTI_DATA_DIR: Data directory (default: data)
"""

import os
from pathlib import Path
from typing import List, Tuple

# Load .env file if present (must be before any os.getenv calls)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, rely on system env vars


def _env_flag(name: str, default: str = "0") -> bool:
    """Parse a boolean-like environment flag."""
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}

# ---- Networking / scraping ----

REQUEST_TIMEOUT_SECONDS = 30
HTTP_MAX_RETRIES = 4
HTTP_BACKOFF_BASE = 1.5  # seconds
HTTP_MIN_DELAY = 0.5
HTTP_MAX_DELAY = 2.5

HTTP_USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
]

# =============================================================================
# SEARCH QUERIES — Education Cybersecurity Incident Discovery
# =============================================================================
# All keyword queries used by the pipeline sources are defined here so
# researchers can review, extend, or translate them in one place.
#
# Used by:
#   - Oxylabs News source  (src/edu_cti/sources/rss/oxylabs_news.py)
#   - Google News RSS source (src/edu_cti/sources/rss/googlenews_rss.py)
#   - News scrapers (SecurityWeek, DarkReading, etc.)
# =============================================================================

# ---- English queries ----
# Broad education + cybersecurity combos sent to search engines and news scrapers.
# Combining both terms in a single query keeps precision high — generic terms like
# "university" or "ransomware" alone would drown results in irrelevant content.
NEWS_SEARCH_QUERIES_EN: List[str] = [
    # Higher education — attack types
    "university cyberattack",
    "university ransomware",
    "university data breach",
    "university hacked",
    "university phishing attack",
    "university cyber incident",
    "campus network attack",
    # College
    "college cyberattack",
    "college data breach",
    "college hacked",
    # K-12 school districts
    "school district ransomware",
    "school district cyberattack",
    "school data breach",
    "school hacked",
    "school board data breach",
    "k-12 cyberattack",
    "k-12 ransomware",
    # Student data
    "student data leaked",
    "student records breach",
    # Broader education sector
    "education sector cyberattack",
    "education ransomware attack",
    "education institution attack",
    "academic institution breach",
    # Regional English variants
    "university cyber attack UK",
    "school ransomware Australia",
    "university data breach Canada",
    "university hacked India",
    "college data breach South Africa",
    "school cyberattack New Zealand",
]

# ---- Multilingual queries ----
# Same concepts as NEWS_SEARCH_QUERIES_EN translated into 13 additional languages.
# Oxylabs Google News SERP handles multilingual queries natively — just pass the
# query string in the target language and results come back in that language.
NEWS_SEARCH_QUERIES_MULTILINGUAL: List[str] = [
    # Spanish (Spain / Mexico / Argentina / Colombia)
    "universidad ciberataque",
    "universidad ransomware",
    "escuela ataque cibernético",
    "universidad hackeo",
    "universidad brecha de datos",
    "colegio ciberataque",
    "datos estudiantes filtrados",

    # French (France / Canada / Belgium)
    "université cyberattaque",
    "université ransomware",
    "école piratage informatique",
    "université fuite de données",
    "lycée attaque informatique",
    "données étudiants volées",

    # German (Germany / Austria / Switzerland)
    "universität cyberangriff",
    "hochschule ransomware",
    "schule hackerangriff",
    "universität datenleck",
    "schule datenpanne",
    "studenten daten gestohlen",

    # Portuguese (Brazil / Portugal)
    "universidade ataque cibernético",
    "universidade ransomware",
    "escola invasão hacker",
    "faculdade vazamento dados",
    "universidade dados estudantes",

    # Italian (Italy)
    "università attacco informatico",
    "università ransomware",
    "scuola violazione dati",
    "università hacker attacco",

    # Dutch (Netherlands / Belgium)
    "universiteit cyberaanval",
    "school ransomware aanval",
    "universiteit datalek",
    "hogeschool hackaanval",

    # Japanese (Japan)
    "大学 サイバー攻撃",
    "大学 ランサムウェア",
    "学校 情報漏洩",
    "大学 不正アクセス",
    "教育機関 サイバー攻撃",

    # Korean (South Korea)
    "대학교 사이버공격",
    "학교 랜섬웨어",
    "대학 해킹",
    "학생 정보 유출",

    # Chinese (Taiwan / mainland)
    "大學 網路攻擊",
    "學校 勒索軟體",
    "大學 資料外洩",
    "教育機構 駭客攻擊",

    # Arabic (Saudi Arabia / UAE / Egypt)
    "جامعة هجوم إلكتروني",
    "مدرسة اختراق إلكتروني",
    "جامعة برامج فدية",
    "بيانات طلاب مسربة",

    # Turkish (Turkey)
    "üniversite siber saldırı",
    "okul ransomware saldırısı",
    "üniversite veri ihlali",
    "okul bilgisayar saldırısı",

    # Polish (Poland)
    "uniwersytet cyberatak",
    "szkoła ransomware",
    "uczelnia atak hakerski",
    "dane studentów wyciek",

    # Russian (Russia)
    "университет кибератака",
    "школа хакерская атака",
    "вуз ransomware атака",
    "утечка данных студентов",

    # Hindi (India — romanised, works in Google Search)
    "university cyber attack India",
    "school ransomware attack India",
    "college data breach India",
    "vishwavidyalaya cyber hamla",
]

# Combined list used by Oxylabs News source (94 queries across 14 languages)
NEWS_SEARCH_QUERIES_ALL: List[str] = NEWS_SEARCH_QUERIES_EN + NEWS_SEARCH_QUERIES_MULTILINGUAL

# Legacy alias — kept so existing code that imports NEWS_SEARCH_QUERIES still works
NEWS_SEARCH_QUERIES: List[str] = NEWS_SEARCH_QUERIES_EN

# ---- Google News RSS queries ----
# Each tuple: (query, language_code, country_code)
# Used by the Google News RSS source which requires explicit lang/country params
# in the RSS URL: ?hl={lang}&gl={country}&ceid={country}:{lang}
GOOGLE_NEWS_RSS_QUERIES: List[Tuple[str, str, str]] = [
    # English — US
    ("university cyberattack", "en", "US"),
    ("university ransomware", "en", "US"),
    ("university data breach", "en", "US"),
    ("college cyberattack", "en", "US"),
    ("school district ransomware", "en", "US"),
    ("school data breach", "en", "US"),
    ("education sector cyberattack", "en", "US"),
    ("student data breach", "en", "US"),
    ("k-12 cyberattack", "en", "US"),
    ("university hacked", "en", "US"),
    # English — UK
    ("university cyberattack", "en", "GB"),
    ("school ransomware", "en", "GB"),
    ("university data breach", "en", "GB"),
    # English — Australia
    ("university cyberattack", "en", "AU"),
    ("school data breach", "en", "AU"),
    # English — India
    ("university cyberattack", "en", "IN"),
    ("college hacked India", "en", "IN"),
    ("IIT cyber attack", "en", "IN"),
    # Spanish
    ("universidad ciberataque", "es", "ES"),
    ("universidad ransomware", "es", "ES"),
    ("escuela ataque cibernético", "es", "ES"),
    ("universidad hackeo", "es", "MX"),
    ("universidad brecha datos", "es", "AR"),
    # French
    ("université cyberattaque", "fr", "FR"),
    ("université ransomware", "fr", "FR"),
    ("école piratage informatique", "fr", "FR"),
    ("université fuite données", "fr", "CA"),
    # German
    ("universität cyberangriff", "de", "DE"),
    ("hochschule ransomware", "de", "DE"),
    ("schule hackerangriff", "de", "DE"),
    ("universität datenleck", "de", "DE"),
    # Portuguese
    ("universidade ataque cibernético", "pt", "BR"),
    ("universidade ransomware", "pt", "BR"),
    ("escola invasão hacker", "pt", "BR"),
    # Italian
    ("università attacco informatico", "it", "IT"),
    ("università ransomware", "it", "IT"),
    ("scuola violazione dati", "it", "IT"),
    # Dutch
    ("universiteit cyberaanval", "nl", "NL"),
    ("school ransomware", "nl", "NL"),
    # Japanese
    ("大学 サイバー攻撃", "ja", "JP"),
    ("大学 ランサムウェア", "ja", "JP"),
    ("学校 情報漏洩", "ja", "JP"),
    # Korean
    ("대학교 사이버공격", "ko", "KR"),
    ("학교 랜섬웨어", "ko", "KR"),
    ("대학 해킹", "ko", "KR"),
    # Chinese
    ("大学 网络攻击", "zh", "TW"),
    ("学校 勒索软件", "zh", "TW"),
    # Arabic
    ("جامعة هجوم إلكتروني", "ar", "SA"),
    ("مدرسة اختراق", "ar", "AE"),
    # Turkish
    ("üniversite siber saldırı", "tr", "TR"),
    ("okul ransomware", "tr", "TR"),
    # Polish
    ("uniwersytet cyberatak", "pl", "PL"),
    ("szkoła ransomware", "pl", "PL"),
    # Russian
    ("университет кибератака", "ru", "RU"),
    ("школа хакерская атака", "ru", "RU"),
    # Hindi
    ("university cyber attack India", "hi", "IN"),
]

# Legacy keyword list — used for post-fetch filtering (matches_keywords)
NEWS_KEYWORDS: List[str] = [
    "university",
    "universities",
    "school",
    "college",
    "campus",
    "education",
    "academy",
]

# Cybersecurity relevance keywords — article MUST contain at least one of these
# to pass the relevance filter (prevents collecting sports/admissions/general news)
CYBER_KEYWORDS: List[str] = [
    "cyberattack", "cyber attack", "cyber-attack",
    "ransomware", "malware", "phishing",
    "data breach", "data leak", "data exposure",
    "hack", "hacked", "hacking", "hacker",
    "breach", "breached",
    "threat actor", "threat group",
    "vulnerability", "exploit",
    "ddos", "denial of service",
    "encryption", "encrypted files",
    "ransom", "extortion",
    "infosteal", "credential", "credentials stolen",
    "unauthorized access", "intrusion",
    "incident response", "security incident",
    "compromised", "compromise",
    "dark web", "darknet",
    "exfiltrat",  # catches exfiltrate, exfiltration, exfiltrated
    "sensitive data", "personal data",
    "cybersecurity", "cyber security",
    "cve-", "zero-day", "zero day",
]

# Education keywords for RSS feed filtering
# Focused on terms that specifically identify educational institutions in cyber attack news
# Kept minimal to avoid false positives while catching relevant incidents
EDUCATION_KEYWORDS: List[str] = [
    # Core institution types (most common in breach headlines)
    "university",
    "college",
    "school district",
    "school board",
    # Specific education terms rarely used outside education context
    "student data",
    "student records",
    "student information",
    "faculty",
    "alumni",
    # K-12 specific
    "k-12",
    "k12",
    "high school",
    "elementary school",
    "middle school",
    # Higher education specific
    "campus",
    "higher education",
    # Research institutions
    "research institute",
    "research university",
    "academic research",
    # Government education bodies
    "department of education",
    "ministry of education",
    "public schools",
]

# Default page-walk limits (None = fetch all pages, can be overridden per source/CLI)
# Set to None by default to fetch all available pages
NEWS_MAX_PAGES = 50  # Safety cap: 50 pages × 20 articles = 1000 per term

# The Record historical search can return long duplicate-heavy tails after the
# first genuinely new pages. Stop early once we see a sustained stale streak.
THERECORD_EMPTY_PAGE_STOP = 5
THERECORD_STALE_PAGE_STOP = 12

# Source identifiers
SOURCE_DATABREACHES = "databreaches"
SOURCE_SECURITYWEEK = "securityweek"
SOURCE_THERECORD = "therecord"
SOURCE_DARKREADING = "darkreading"
SOURCE_RANSOMLOOK = "ransomlook"
SOURCE_CISA_KEV = "cisa_kev"
SOURCE_OTX = "otx_alienvault"
SOURCE_CISA_RSS = "cisa_alerts"
SOURCE_INTL_RSS = "international_rss"

# Environment variable configuration
# Auto-detect Railway environment and use persistent storage
def _detect_railway() -> bool:
    """Detect if running on Railway platform."""
    # Check Railway-specific environment variables first (most reliable)
    if (
        os.getenv("RAILWAY_ENVIRONMENT") is not None
        or os.getenv("RAILWAY_PROJECT_ID") is not None
        or os.getenv("RAILWAY_SERVICE_ID") is not None
    ):
        return True
    
    # Check if /app/data exists and is writable (Railway volume mount)
    railway_data = Path("/app/data")
    if railway_data.exists() and os.access(railway_data, os.W_OK):
        return True
    
    return False

def _get_data_dir() -> Path:
    """Get data directory based on environment."""
    # Check for explicit override (highest priority)
    if os.getenv("EDU_CTI_DATA_DIR"):
        data_dir = Path(os.getenv("EDU_CTI_DATA_DIR"))
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError):
            pass  # Directory creation will be retried later if needed
        return data_dir
    
    # Auto-detect Railway
    if _detect_railway():
        # Railway persistent storage
        railway_data = Path("/app/data")
        # Try to create, but don't fail if we can't (e.g., testing locally)
        try:
            if railway_data.parent.exists() and os.access(railway_data.parent, os.W_OK):
                railway_data.mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError):
            # If we can't create /app/data, we're probably not on Railway
            # Fall through to local data directory
            pass
        else:
            return railway_data
    
    # Local development - use ./data
    local_data = Path("data")
    try:
        local_data.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError):
        pass  # Will be created when actually needed
    return local_data

# Set data directory and database path
DATA_DIR = _get_data_dir()
DB_PATH = DATA_DIR / os.getenv("EDU_CTI_DB_PATH", "eduthreat.db")

# Logging configuration
LOG_LEVEL = os.getenv("EDU_CTI_LOG_LEVEL", "INFO")
LOG_FILE = Path(os.getenv("EDU_CTI_LOG_FILE", "logs/pipeline.log"))

# Ensure directories exist (with error handling)
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except (OSError, PermissionError) as e:
    # Log but don't fail - directory creation will be retried when actually needed
    pass

try:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
except (OSError, PermissionError):
    pass

# Log the detected configuration (only if logging is configured)
try:
    import logging
    _logger = logging.getLogger(__name__)
    if _logger.isEnabledFor(logging.INFO):
        _logger.info(f"Data directory: {DATA_DIR.absolute()}")
        _logger.info(f"Database path: {DB_PATH.absolute()}")
        if _detect_railway():
            _logger.info("Railway environment detected - using persistent storage")
except Exception:
    pass  # Logging not configured yet

# ---- Phase 2: LLM Enrichment ----

# Ollama Cloud configuration
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "https://ollama.com")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3.5:cloud")  # Best for structured CTI extraction

# Enrichment processing configuration
ENRICHMENT_BATCH_SIZE = int(os.getenv("ENRICHMENT_BATCH_SIZE", "10"))  # Process N incidents per batch
ENRICHMENT_MAX_RETRIES = int(os.getenv("ENRICHMENT_MAX_RETRIES", "3"))  # Max retries per incident
ENRICHMENT_RATE_LIMIT_DELAY = float(os.getenv("ENRICHMENT_RATE_LIMIT_DELAY", "2.0"))  # Seconds between API calls
ENRICHMENT_WORKERS = int(os.getenv("ENRICHMENT_WORKERS", "3"))  # Parallel LLM workers (1-8)
# Max consecutive SERP failures before permanently deleting the incident.
# URL-less incidents that never yield search results are unenrichable;
# deleting them after N attempts stops them retrying on every pipeline run.
SERP_MAX_ATTEMPTS = int(os.getenv("SERP_MAX_ATTEMPTS", "3"))

# Sources to skip in fetch + enrichment phases (IOC/malware feeds, not news articles)
# Ingestion code is kept intact; re-enable by removing from this set.
ENRICHMENT_SKIP_SOURCES: set = {
    "threatfox",
    "urlhaus",
    "otx_alienvault",
    "cisa_kev",
}

# Sources where every fetch tier fails (paywall, login-gate, etc.).
# For these, skip all 4 fetch tiers entirely and go straight to SERP fallback.
FETCH_IMPOSSIBLE_SOURCES: set = {
    "securityweek",
}

# ---- Phase 2.1: IOC Enrichment (External APIs) ----

# AlienVault OTX (free, register at https://otx.alienvault.com)
OTX_API_KEY = os.getenv("OTX_API_KEY", "")

# Oxylabs API configuration (web scraping and SERP discovery)
OXYLABS_USERNAME = os.getenv("OXYLABS_USERNAME", "")
OXYLABS_PASSWORD = os.getenv("OXYLABS_PASSWORD", "")
ENABLE_OXYLABS_NEWS_HISTORICAL = _env_flag("ENABLE_OXYLABS_NEWS_HISTORICAL", "1")
ENABLE_OXYLABS_NEWS_DAILY = _env_flag("ENABLE_OXYLABS_NEWS_DAILY", "0")

# Historical scraping start year (applies to date-paginated sources)
HISTORICAL_START_YEAR = int(os.getenv("HISTORICAL_START_YEAR", "2000"))

# Google News RSS has no meaningful coverage before ~2019.
# Queries for years before this threshold will be skipped to avoid empty result sets.
GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR = 2019
