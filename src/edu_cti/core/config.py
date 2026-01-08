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
from typing import List

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

# Keyword filtering for news/search scrapers
NEWS_KEYWORDS: List[str] = [
    "university",
    "universities",
    "school",
    "college",
    "campus",
    "education",
    "academy",
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
NEWS_MAX_PAGES = None

# Source identifiers
SOURCE_DATABREACHES = "databreaches"
SOURCE_SECURITYWEEK = "securityweek"
SOURCE_THERECORD = "therecord"
SOURCE_DARKREADING = "darkreading"

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
except:
    pass  # Logging not configured yet

# ---- Phase 2: LLM Enrichment ----

# Ollama Cloud configuration
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "https://ollama.com")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")  # Best for structured CTI extraction

# Enrichment processing configuration
ENRICHMENT_BATCH_SIZE = int(os.getenv("ENRICHMENT_BATCH_SIZE", "10"))  # Process N incidents per batch
ENRICHMENT_MAX_RETRIES = int(os.getenv("ENRICHMENT_MAX_RETRIES", "3"))  # Max retries per incident
ENRICHMENT_RATE_LIMIT_DELAY = float(os.getenv("ENRICHMENT_RATE_LIMIT_DELAY", "2.0"))  # Seconds between API calls
