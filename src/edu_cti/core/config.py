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
from typing import Dict, List, Tuple

from src.edu_cti.core.discovery_policy import BROAD_CYBER_SOURCE_DOMAINS
from src.edu_cti_v2.env import get_int  # backward-compatible env access (new name → legacy)

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
# Same concepts as NEWS_SEARCH_QUERIES_EN translated into additional languages.
# Keep the language grouping explicit so Google News RSS can query every language
# against every configured country/edition for that language.
NEWS_SEARCH_QUERIES_MULTILINGUAL_BY_LANG: Dict[str, List[str]] = {
    # Spanish (Spain / Mexico / Argentina / Colombia)
    "es": [
        "universidad ciberataque",
        "universidad ransomware",
        "escuela ataque cibernético",
        "universidad hackeo",
        "universidad brecha de datos",
        "colegio ciberataque",
        "datos estudiantes filtrados",
    ],
    # French (France / Canada / Belgium)
    "fr": [
        "université cyberattaque",
        "université ransomware",
        "école piratage informatique",
        "université fuite de données",
        "lycée attaque informatique",
        "données étudiants volées",
    ],
    # German (Germany / Austria / Switzerland)
    "de": [
        "universität cyberangriff",
        "hochschule ransomware",
        "schule hackerangriff",
        "universität datenleck",
        "schule datenpanne",
        "studenten daten gestohlen",
    ],
    # Portuguese (Brazil / Portugal)
    "pt": [
        "universidade ataque cibernético",
        "universidade ransomware",
        "escola invasão hacker",
        "faculdade vazamento dados",
        "universidade dados estudantes",
    ],
    # Italian (Italy)
    "it": [
        "università attacco informatico",
        "università ransomware",
        "scuola violazione dati",
        "università hacker attacco",
    ],
    # Dutch (Netherlands / Belgium)
    "nl": [
        "universiteit cyberaanval",
        "school ransomware aanval",
        "universiteit datalek",
        "hogeschool hackaanval",
    ],
    # Japanese (Japan)
    "ja": [
        "大学 サイバー攻撃",
        "大学 ランサムウェア",
        "学校 情報漏洩",
        "大学 不正アクセス",
        "教育機関 サイバー攻撃",
    ],
    # Korean (South Korea)
    "ko": [
        "대학교 사이버공격",
        "학교 랜섬웨어",
        "대학 해킹",
        "학생 정보 유출",
    ],
    # Chinese (Taiwan / mainland)
    "zh": [
        "大學 網路攻擊",
        "學校 勒索軟體",
        "大學 資料外洩",
        "教育機構 駭客攻擊",
    ],
    # Arabic (Saudi Arabia / UAE / Egypt)
    "ar": [
        "جامعة هجوم إلكتروني",
        "مدرسة اختراق إلكتروني",
        "جامعة برامج فدية",
        "بيانات طلاب مسربة",
    ],
    # Turkish (Turkey)
    "tr": [
        "üniversite siber saldırı",
        "okul ransomware saldırısı",
        "üniversite veri ihlali",
        "okul bilgisayar saldırısı",
    ],
    # Polish (Poland)
    "pl": [
        "uniwersytet cyberatak",
        "szkoła ransomware",
        "uczelnia atak hakerski",
        "dane studentów wyciek",
    ],
    # Russian (Russia)
    "ru": [
        "университет кибератака",
        "школа хакерская атака",
        "вуз ransomware атака",
        "утечка данных студентов",
    ],
    # Hindi (India — romanised, works in Google Search)
    "hi": [
        "university cyber attack India",
        "school ransomware attack India",
        "college data breach India",
        "vishwavidyalaya cyber hamla",
    ],
    # Nordic countries
    "sv": [
        "universitet cyberattack",
        "skola ransomware",
        "studentdata läcka",
        "universitet dataintrång",
    ],
    "no": [
        "universitet dataangrep",
        "skole løsepengevirus",
        "studentdata lekkasje",
    ],
    "da": [
        "universitet cyberangreb",
        "skole ransomware",
        "studentdata læk",
    ],
    "fi": [
        "yliopisto kyberhyökkäys",
        "koulu kiristyshaittaohjelma",
        "opiskelijatiedot tietovuoto",
    ],
    # Eastern Europe
    "cs": [
        "univerzita kyberútok",
        "škola ransomware",
        "únik dat studentů",
    ],
    "ro": [
        "universitate atac cibernetic",
        "școală ransomware",
        "date studenți scurse",
    ],
    "uk": [
        "університет кібератака",
        "школа ransomware",
        "витік даних студентів",
    ],
    # Southeast and South Asia
    "id": [
        "universitas serangan siber",
        "sekolah ransomware",
        "data mahasiswa bocor",
    ],
    "ms": [
        "universiti serangan siber",
        "sekolah ransomware",
        "data pelajar bocor",
    ],
    "th": [
        "มหาวิทยาลัย โจมตีไซเบอร์",
        "โรงเรียน แรนซัมแวร์",
        "ข้อมูลนักเรียน รั่วไหล",
    ],
    "vi": [
        "đại học tấn công mạng",
        "trường học ransomware",
        "dữ liệu sinh viên bị lộ",
    ],
    "bn": [
        "বিশ্ববিদ্যালয় সাইবার হামলা",
        "স্কুল র‍্যানসমওয়্যার",
        "শিক্ষার্থী তথ্য ফাঁস",
    ],
    "ur": [
        "یونیورسٹی سائبر حملہ",
        "اسکول رینسم ویئر",
        "طلبہ کا ڈیٹا لیک",
    ],
}

NEWS_SEARCH_QUERIES_MULTILINGUAL: List[str] = [
    query
    for queries in NEWS_SEARCH_QUERIES_MULTILINGUAL_BY_LANG.values()
    for query in queries
]

# Site-restricted recovery queries let the high-recall Google/Oxylabs path pick
# up relevant stories from broad cyber-news sources without loosening those
# sources' own noisy site/feed scrapers.
NEWS_SEARCH_SITE_RESTRICTED_TEMPLATES: List[str] = [
    "university ransomware",
    "university data breach",
    "school district ransomware",
    "school data breach",
    "college cyberattack",
    "student records breach",
]

NEWS_SEARCH_SITE_RESTRICTED_DOMAINS: List[str] = list(BROAD_CYBER_SOURCE_DOMAINS.values())

NEWS_SEARCH_OFFICIAL_SITE_RESTRICTED_TEMPLATES: List[str] = [
    "university data breach",
    "school district data breach",
    "student records breach",
    "cyber incident university",
    "ransomware school",
]

NEWS_SEARCH_OFFICIAL_SITE_RESTRICTED_DOMAINS: List[str] = [
    "oag.ca.gov",
    "maine.gov",
    "atg.wa.gov",
    "mass.gov",
    "ocrportal.hhs.gov",
    "oaic.gov.au",
    "ico.org.uk",
    "cnil.fr",
    "dataprotection.ie",
    "ncsc.gov.uk",
    "k12six.org",
    "idtheftcenter.org",
]

NEWS_SEARCH_SITE_RESTRICTED_QUERIES: List[str] = [
    f"site:{domain} {query}"
    for domain in NEWS_SEARCH_SITE_RESTRICTED_DOMAINS
    for query in NEWS_SEARCH_SITE_RESTRICTED_TEMPLATES
] + [
    f"site:{domain} {query}"
    for domain in NEWS_SEARCH_OFFICIAL_SITE_RESTRICTED_DOMAINS
    for query in NEWS_SEARCH_OFFICIAL_SITE_RESTRICTED_TEMPLATES
]

NEWS_SEARCH_QUERIES_EN_WITH_SITE: List[str] = NEWS_SEARCH_QUERIES_EN + NEWS_SEARCH_SITE_RESTRICTED_QUERIES

# Combined list used by Oxylabs News source.
NEWS_SEARCH_QUERIES_ALL: List[str] = NEWS_SEARCH_QUERIES_EN_WITH_SITE + NEWS_SEARCH_QUERIES_MULTILINGUAL

# Legacy alias — kept so existing code that imports NEWS_SEARCH_QUERIES still works
NEWS_SEARCH_QUERIES: List[str] = NEWS_SEARCH_QUERIES_EN

# ---- Google News RSS queries ----
# Each tuple: (query, language_code, country_code). Google News RSS requires
# explicit language/country params in the RSS URL:
# ?hl={lang}&gl={country}&ceid={country}:{lang}
GOOGLE_NEWS_RSS_COUNTRIES_BY_LANG: Dict[str, List[str]] = {
    "en": [
        "US",
        "GB",
        "CA",
        "AU",
        "IN",
        "NZ",
        "ZA",
        "IE",
        "NG",
        "KE",
        "GH",
        "SG",
        "MY",
        "ID",
        "TH",
        "VN",
        "SE",
        "NO",
        "DK",
        "FI",
        "CZ",
        "RO",
        "UA",
        "CL",
        "PE",
    ],
    "es": ["ES", "MX", "AR", "CO"],
    "fr": ["FR", "CA", "BE"],
    "de": ["DE", "AT", "CH"],
    "pt": ["BR", "PT"],
    "it": ["IT"],
    "nl": ["NL", "BE"],
    "ja": ["JP"],
    "ko": ["KR"],
    "zh": ["TW", "CN"],
    "ar": ["SA", "AE", "EG"],
    "tr": ["TR"],
    "pl": ["PL"],
    "ru": ["RU"],
    "hi": ["IN"],
    "sv": ["SE"],
    "no": ["NO"],
    "da": ["DK"],
    "fi": ["FI"],
    "cs": ["CZ"],
    "ro": ["RO"],
    "uk": ["UA"],
    "id": ["ID"],
    "ms": ["MY", "SG"],
    "th": ["TH"],
    "vi": ["VN"],
    "bn": ["BD", "IN"],
    "ur": ["PK", "IN"],
}


def _expand_google_news_rss_queries() -> List[Tuple[str, str, str]]:
    query_groups: Dict[str, List[str]] = {
        "en": NEWS_SEARCH_QUERIES_EN,
        **NEWS_SEARCH_QUERIES_MULTILINGUAL_BY_LANG,
    }
    expanded: List[Tuple[str, str, str]] = []
    seen: set[Tuple[str, str, str]] = set()
    for lang, queries in query_groups.items():
        for query in queries:
            for country in GOOGLE_NEWS_RSS_COUNTRIES_BY_LANG.get(lang, []):
                key = (query, lang, country)
                if key in seen:
                    continue
                seen.add(key)
                expanded.append(key)
    for query in NEWS_SEARCH_SITE_RESTRICTED_QUERIES:
        key = (query, "en", "US")
        if key in seen:
            continue
        seen.add(key)
        expanded.append(key)
    return expanded


GOOGLE_NEWS_RSS_QUERIES: List[Tuple[str, str, str]] = _expand_google_news_rss_queries()

# Historical Google RSS coverage uses smaller windows to reduce per-feed result
# truncation. Override only if we need to trade coverage for runtime.
GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS = max(
    1,
    get_int(
        "GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS",
        "EDU_CTI_GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS",
        default=21,
    ),
)
GOOGLE_NEWS_RSS_REQUEST_DELAY_SECONDS = max(
    0.0,
    float(os.getenv("EDU_CTI_GOOGLE_NEWS_RSS_REQUEST_DELAY_SECONDS", "1.0")),
)

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
    # --- Broad terms that appear in almost every education breach headline ---
    "school",           # catches "public school", "school cyberattack", etc.
    "education",        # "education sector", "education department"
    "student",          # "student data", "student records", "students affected"
    "academic",         # "academic institution", "academic records"
    # --- Core institution types ---
    "university",
    "college",
    "school district",
    "school board",
    "school system",
    "unified school",   # "Unified School District" (US K-12)
    "community college",
    "state college",
    # --- Specific institution types ---
    "academy",
    "polytechnic",
    "seminary",
    "conservatory",
    "vocational school",
    "technical college",
    "boarding school",
    "charter school",
    "magnet school",
    # --- Early education ---
    "kindergarten",
    "preschool",
    # --- K-12 labels ---
    "k-12",
    "k12",
    "high school",
    "elementary school",
    "middle school",
    "primary school",
    "secondary school",
    # --- People / roles (appear in breach descriptions) ---
    "teacher",
    "faculty",
    "alumni",
    "enrollment",
    # --- Specific education data terms ---
    "student data",
    "student records",
    "student information",
    "student portal",
    "transcript",
    # --- Higher education ---
    "campus",
    "higher education",
    # --- Research institutions ---
    "research institute",
    "research university",
    "academic research",
    # --- EdTech platforms — breaches here always affect students ---
    "edtech",
    "powerschool",                  # Major K-12 SIS — affected 60M+ records
    "schoology",                    # LMS used by thousands of districts
    "infinite campus",              # K-12 SIS
    "student information system",
    "learning management system",
    "learning management",
    # --- Education finance ---
    "student loan",
    "fafsa",                        # US federal student aid — education-specific
    "student aid",
    # --- Government / regulatory education bodies ---
    "department of education",
    "ministry of education",
    "office of education",
    "public schools",
    "board of education",
    # --- Latin-script multilingual equivalents ---
    # (Spanish, French, German, Portuguese, Italian, Dutch, Turkish, Polish)
    "universidad", "universitat", "université", "universidade",
    "università", "universiteit", "üniversite", "uniwersytet",
    "hochschule", "lycée", "lycee", "escuela", "escola",
    "faculdade", "scuola", "hogeschool", "okul", "szkoła",
    "studenten", "estudiantes", "étudiants", "studenti",
    "alunos", "öğrenci", "studenci",
    "gymnasium",                    # European secondary school (not sports)
    "gesamtschule",                 # German comprehensive school
    "realschule",                   # German secondary school
    "grundschule",                  # German primary school
    "berufsschule",                 # German vocational school
    "colegio",                      # Spanish school/college
    "instituto",                    # Spanish/Portuguese educational institute
]

# Education-technology vendor names whose compromise cascades to schools/universities
# (SIS / LMS / student-services platforms). Kept SEPARATE from EDUCATION_KEYWORDS and
# used only to widen the news-discovery title gate (coverage), so a supply-chain article
# that names the vendor but not "school"/"university" still gets fetched; the LLM
# relevance gate remains the precision backstop. Only reasonably unambiguous tokens are
# listed (multi-word where a bare token would over-match general news).
EDTECH_VENDOR_KEYWORDS: List[str] = [
    "powerschool", "schoology", "infinite campus", "instructure", "canvas lms",
    "blackbaud", "ellucian", "moodle", "brightspace", "d2l", "blackboard learn",
    "naviance", "illuminate education", "workday student", "follett destiny",
    "securly", "classlink", "goguardian", "go guardian", "gaggle", "frontline education",
    "renaissance learning", "edgenuity", "jenzabar", "anthology student", "skyward sis",
    "clever inc", "parchment", "unit4 student",
]

# Default page-walk safety cap for news sources (None/CLI can still override per call).
# Env-configurable for coverage tuning: 100 pages ≈ 2,000 results/term ceiling; the
# per-source empty/stale-page stops prevent walking dead tails.
NEWS_MAX_PAGES = max(1, get_int("NEWS_MAX_PAGES", "EDU_CTI_NEWS_MAX_PAGES", default=100))
NEWS_EXACT_PHRASE_MAX_PAGES = get_int(
    "NEWS_EXACT_PHRASE_MAX_PAGES", "EDU_CTI_NEWS_EXACT_PHRASE_MAX_PAGES", default=2
)

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
METRICS_DB_PATH = DATA_DIR / os.getenv("EDU_CTI_METRICS_DB_PATH", "eduthreat_metrics.db")

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
        _logger.info(f"Metrics database path: {METRICS_DB_PATH.absolute()}")
        if _detect_railway():
            _logger.info("Railway environment detected - using persistent storage")
except Exception:
    pass  # Logging not configured yet

# ---- Phase 2: LLM Enrichment ----

# Ollama Cloud configuration
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "https://ollama.com")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3-coder:480b-cloud")  # Production default for Phase 2 extraction (benchmark-selected)

# Enrichment processing configuration
ENRICHMENT_BATCH_SIZE = int(os.getenv("ENRICHMENT_BATCH_SIZE", "10"))  # Process N incidents per batch
ENRICHMENT_MAX_RETRIES = int(os.getenv("ENRICHMENT_MAX_RETRIES", "3"))  # Max retries per incident
ENRICHMENT_RATE_LIMIT_DELAY = float(os.getenv("ENRICHMENT_RATE_LIMIT_DELAY", "2.0"))  # Seconds between API calls
ENRICHMENT_WORKERS = int(os.getenv("ENRICHMENT_WORKERS", "3"))  # Parallel LLM workers (1-8)
PHASE2_MEMORY_MONITOR_ENABLED = _env_flag("PHASE2_MEMORY_MONITOR_ENABLED", "1")
PHASE2_MEMORY_CHECK_INTERVAL = int(os.getenv("PHASE2_MEMORY_CHECK_INTERVAL", "100"))
PHASE2_MEMORY_GC_INTERVAL = int(os.getenv("PHASE2_MEMORY_GC_INTERVAL", "1000"))
PHASE2_MEMORY_SOFT_LIMIT_MB = int(os.getenv("PHASE2_MEMORY_SOFT_LIMIT_MB", "0"))
PHASE2_MEMORY_HARD_LIMIT_MB = int(os.getenv("PHASE2_MEMORY_HARD_LIMIT_MB", "0"))
PHASE2_MEMORY_SOFT_LIMIT_PCT = float(os.getenv("PHASE2_MEMORY_SOFT_LIMIT_PCT", "0.75"))
PHASE2_MEMORY_HARD_LIMIT_PCT = float(os.getenv("PHASE2_MEMORY_HARD_LIMIT_PCT", "0.85"))
# Max consecutive SERP failures before permanently deleting the incident.
# URL-less incidents that never yield search results are unenrichable;
# deleting them after N attempts stops them retrying on every pipeline run.
SERP_MAX_ATTEMPTS = int(os.getenv("SERP_MAX_ATTEMPTS", "3"))
AUTO_RESUME_INTERRUPTED_PIPELINES = _env_flag(
    "AUTO_RESUME_INTERRUPTED_PIPELINES",
    "1" if _detect_railway() else "0",
)

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

# Oxylabs API configuration (optional paid web scraping and SERP fallback)
OXYLABS_USERNAME = os.getenv("OXYLABS_USERNAME", "")
OXYLABS_PASSWORD = os.getenv("OXYLABS_PASSWORD", "")
ENABLE_OXYLABS_NEWS_HISTORICAL = _env_flag("ENABLE_OXYLABS_NEWS_HISTORICAL", "1")
ENABLE_OXYLABS_NEWS_DAILY = _env_flag("ENABLE_OXYLABS_NEWS_DAILY", "0")

# Historical scraping start year (applies to date-paginated sources)
HISTORICAL_START_YEAR = int(os.getenv("HISTORICAL_START_YEAR", "2000"))

# Google News RSS has no meaningful coverage before ~2019.
# Queries for years before this threshold will be skipped to avoid empty result sets.
GOOGLE_NEWS_RSS_EFFECTIVE_START_YEAR = 2019
