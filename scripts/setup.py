#!/usr/bin/env python3
"""
EduThreat-CTI Setup Script

This script helps configure and initialize the EduThreat-CTI pipeline.

Usage:
    python scripts/setup.py              # Interactive setup
    python scripts/setup.py --check      # Verify configuration
    python scripts/setup.py --init-db    # Initialize database only
    python scripts/setup.py --env        # Generate .env file

Environment Variables:
    EDU_CTI_DB_PATH             Database path (default: data/eduthreat.db)
    EDU_CTI_LOG_LEVEL           Log level (default: INFO)
    EDU_CTI_LOG_FILE            Log file path (default: logs/pipeline.log)
    EDU_CTI_DATA_DIR            Data directory (default: data)
    OLLAMA_API_KEY              Ollama Cloud API key (required for Phase 2)
    OLLAMA_HOST                 Ollama host (default: https://ollama.com)
    OLLAMA_MODEL                LLM model (default: deepseek-v3.1:671b-cloud)
    ENRICHMENT_BATCH_SIZE       Batch size for enrichment (default: 10)
    ENRICHMENT_RATE_LIMIT_DELAY Rate limit delay in seconds (default: 2.0)
"""

import argparse
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def check_dependencies():
    """Check if all required dependencies are installed."""
    print("\n📦 Checking dependencies...")
    
    # Map package names to import names where they differ
    required = {
        "requests": "requests",
        "beautifulsoup4": "bs4",
        "lxml": "lxml",
        "pandas": "pandas",
        "pydantic": "pydantic",
        "selenium": "selenium",
        "newspaper3k": "newspaper",
        "brotli": "brotli",
        "python-dateutil": "dateutil",
    }
    
    missing = []
    for pkg, import_name in required.items():
        try:
            __import__(import_name)
            print(f"  ✓ {pkg}")
        except ImportError:
            missing.append(pkg)
            print(f"  ✗ {pkg} (MISSING)")
    
    if missing:
        print(f"\n⚠️  Missing packages: {', '.join(missing)}")
        print("   Install with: pip install -r requirements.txt")
        return False
    
    print("  All dependencies installed!")
    return True


def check_database():
    """Check database configuration and state."""
    print("\n🗄️  Checking database...")
    
    from src.edu_cti.core.config import DB_PATH
    
    if DB_PATH.exists():
        import sqlite3
        conn = sqlite3.connect(str(DB_PATH))
        
        # Check tables
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"  ✓ Database exists: {DB_PATH}")
        print(f"  ✓ Tables: {', '.join(tables) if tables else '(none)'}")
        
        # Check incident counts
        if "incidents" in tables:
            cursor = conn.execute("SELECT COUNT(*) FROM incidents")
            count = cursor.fetchone()[0]
            print(f"  ✓ Incidents: {count}")
        
        # Check source_state
        if "source_state" in tables:
            cursor = conn.execute("SELECT source, last_pubdate FROM source_state")
            states = cursor.fetchall()
            if states:
                print("  ✓ Source state (incremental tracking):")
                for source, date in states:
                    print(f"      {source}: {date}")
            else:
                print("  ⚠ Source state is empty (first run needed)")
        
        conn.close()
        return True
    else:
        print(f"  ⚠ Database not found: {DB_PATH}")
        print("     Run: python -m src.edu_cti.pipeline.phase1 --full-historical")
        return False


def check_environment():
    """Check environment variables."""
    print("\n🔧 Checking environment variables...")
    
    from src.edu_cti.core import config
    
    vars_to_check = {
        "EDU_CTI_DB_PATH": str(config.DB_PATH),
        "EDU_CTI_LOG_LEVEL": config.LOG_LEVEL,
        "EDU_CTI_DATA_DIR": str(config.DATA_DIR),
        "OLLAMA_API_KEY": "***" if config.OLLAMA_API_KEY else "(not set)",
        "OLLAMA_HOST": config.OLLAMA_HOST,
        "OLLAMA_MODEL": config.OLLAMA_MODEL,
        "ENRICHMENT_BATCH_SIZE": str(config.ENRICHMENT_BATCH_SIZE),
        "ENRICHMENT_RATE_LIMIT_DELAY": str(config.ENRICHMENT_RATE_LIMIT_DELAY),
    }
    
    for var, value in vars_to_check.items():
        status = "✓" if os.getenv(var) or var.startswith("EDU_CTI") else "⚪"
        print(f"  {status} {var}: {value}")
    
    if not config.OLLAMA_API_KEY:
        print("\n  ⚠️  OLLAMA_API_KEY not set - Phase 2 enrichment will fail")
        print("     Set with: export OLLAMA_API_KEY=your_key_here")
        print("     (Not required for Phase 1 ingestion)")
    
    return True  # Don't fail for missing API key (only needed for Phase 2)


def check_directories():
    """Check and create required directories."""
    print("\n📁 Checking directories...")
    
    from src.edu_cti.core.config import DATA_DIR
    
    dirs = [
        DATA_DIR,
        DATA_DIR / "raw" / "curated",
        DATA_DIR / "raw" / "news",
        DATA_DIR / "raw" / "rss",
        DATA_DIR / "processed",
        Path("logs"),
    ]
    
    for d in dirs:
        if d.exists():
            print(f"  ✓ {d}")
        else:
            d.mkdir(parents=True, exist_ok=True)
            print(f"  ✓ {d} (created)")
    
    return True


def init_database():
    """Initialize the database."""
    print("\n🗄️  Initializing database...")
    
    from src.edu_cti.core.db import get_connection, init_db
    
    conn = get_connection()
    init_db(conn)
    conn.close()
    
    print("  ✓ Database initialized!")
    return True


def generate_env_file():
    """Generate a .env.example file."""
    env_content = """# EduThreat-CTI Environment Configuration
# Copy this to .env and customize as needed

# ---- Data & Logging ----
EDU_CTI_DATA_DIR=data
EDU_CTI_DB_PATH=eduthreat.db
EDU_CTI_LOG_LEVEL=INFO
EDU_CTI_LOG_FILE=logs/pipeline.log

# ---- Phase 2: LLM Enrichment ----
# Required for Phase 2 enrichment
OLLAMA_API_KEY=your_api_key_here
OLLAMA_HOST=https://ollama.com
OLLAMA_MODEL=deepseek-v3.1:671b-cloud

# Enrichment processing settings
ENRICHMENT_BATCH_SIZE=10
ENRICHMENT_MAX_RETRIES=3
ENRICHMENT_RATE_LIMIT_DELAY=2.0
"""
    
    env_path = PROJECT_ROOT / ".env.example"
    with open(env_path, "w") as f:
        f.write(env_content)
    
    print(f"\n✓ Generated {env_path}")
    print("  Copy to .env and set OLLAMA_API_KEY for Phase 2 enrichment")
    return True


def print_quick_start():
    """Print quick start guide."""
    print("\n" + "="*60)
    print("📖 Quick Start Guide")
    print("="*60)
    print("""
1️⃣  First-time setup (historical scrape - takes 2-3 hours):
    python -m src.edu_cti.pipeline.phase1 --full-historical

2️⃣  Daily updates (incremental - takes seconds):
    python -m src.edu_cti.pipeline.phase1

3️⃣  Phase 2 enrichment (requires OLLAMA_API_KEY):
    export OLLAMA_API_KEY=your_key_here
    python -m src.edu_cti.pipeline.phase2 --limit 10

4️⃣  Export enriched data to CSV:
    python -m src.edu_cti.pipeline.phase2 --export-csv

📊 Check source state:
    sqlite3 data/eduthreat.db "SELECT * FROM source_state"

📁 Output files:
    - data/eduthreat.db                    # SQLite database
    - data/processed/base_dataset.csv      # Phase 1 incidents
    - data/processed/enriched_dataset.csv  # Phase 2 enriched data
""")


def main():
    parser = argparse.ArgumentParser(
        description="EduThreat-CTI Setup and Configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--check", action="store_true",
        help="Check configuration and dependencies"
    )
    parser.add_argument(
        "--init-db", action="store_true",
        help="Initialize the database"
    )
    parser.add_argument(
        "--env", action="store_true",
        help="Generate .env.example file"
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("🎓 EduThreat-CTI Setup")
    print("="*60)
    
    if args.env:
        generate_env_file()
        return
    
    if args.init_db:
        check_directories()
        init_database()
        return
    
    # Default: run all checks
    all_ok = True
    
    all_ok &= check_dependencies()
    all_ok &= check_directories()
    all_ok &= check_environment()
    all_ok &= check_database()
    
    print_quick_start()
    
    if all_ok:
        print("\n✅ All checks passed! Ready to run pipelines.")
    else:
        print("\n⚠️  Some checks failed. Review the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

