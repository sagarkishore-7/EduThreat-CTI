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

# Default page-walk limits (None = fetch all pages, can be overridden per source/CLI)
# Set to None by default to fetch all available pages
NEWS_MAX_PAGES = None

# Source identifiers
SOURCE_DATABREACHES = "databreaches"
SOURCE_SECURITYWEEK = "securityweek"
SOURCE_THERECORD = "therecord"
SOURCE_DARKREADING = "darkreading"

# Environment variable configuration
DATA_DIR = Path(os.getenv("EDU_CTI_DATA_DIR", "data"))
DB_PATH = DATA_DIR / os.getenv("EDU_CTI_DB_PATH", "eduthreat.db")
LOG_LEVEL = os.getenv("EDU_CTI_LOG_LEVEL", "INFO")
LOG_FILE = Path(os.getenv("EDU_CTI_LOG_FILE", "logs/pipeline.log"))

# ---- Phase 2: LLM Enrichment ----

# Ollama Cloud configuration
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "https://ollama.com")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "deepseek-v3.1:671b-cloud")  # Best for structured CTI extraction

# Enrichment processing configuration
ENRICHMENT_BATCH_SIZE = int(os.getenv("ENRICHMENT_BATCH_SIZE", "10"))  # Process N incidents per batch
ENRICHMENT_MAX_RETRIES = int(os.getenv("ENRICHMENT_MAX_RETRIES", "3"))  # Max retries per incident
ENRICHMENT_RATE_LIMIT_DELAY = float(os.getenv("ENRICHMENT_RATE_LIMIT_DELAY", "2.0"))  # Seconds between API calls
