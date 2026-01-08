#!/usr/bin/env python3
"""
Script to migrate existing database to Railway persistent storage.

This script:
1. Copies the database from the repo (data/eduthreat.db) to Railway volume (/app/data/eduthreat.db)
2. Verifies the migration
3. Updates the database path configuration

Run this once after setting up Railway persistent storage.
"""

import os
import shutil
import sqlite3
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.edu_cti.core.config import DATA_DIR, DB_PATH


def migrate_database():
    """Migrate database to Railway persistent storage."""
    print("="*70)
    print("Database Migration to Railway Persistent Storage")
    print("="*70)
    
    # Source: local repo database (check multiple possible locations)
    possible_sources = [
        Path("data/eduthreat.db"),
        Path("/app/data/eduthreat.db"),  # Already in Railway
        Path("../data/eduthreat.db"),  # If running from scripts/
    ]
    
    source_db = None
    for path in possible_sources:
        if path.exists():
            source_db = path
            break
    
    # Destination: Railway persistent volume
    dest_dir = Path("/app/data")
    dest_db = dest_dir / "eduthreat.db"
    
    # Check if source exists
    if source_db is None:
        print(f"❌ Source database not found in any of these locations:")
        for path in possible_sources:
            print(f"   - {path.absolute()}")
        print("   If this is a fresh Railway deployment, the DB will be created automatically.")
        print("   If DB already exists at /app/data, migration is not needed.")
        return False
    
    print(f"📦 Source: {source_db.absolute()}")
    print(f"📦 Destination: {dest_db}")
    
    # Get source DB size
    source_size = source_db.stat().st_size / (1024 * 1024)  # MB
    print(f"📊 Source DB size: {source_size:.2f} MB")
    
    # Create destination directory
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"✓ Created destination directory: {dest_dir}")
    
    # Check if destination already exists
    if dest_db.exists():
        print(f"⚠️  Destination database already exists: {dest_db}")
        response = input("   Overwrite? (yes/no): ").strip().lower()
        if response != "yes":
            print("❌ Migration cancelled")
            return False
        
        # Backup existing
        backup_path = dest_db.with_suffix(f".db.backup.{int(os.path.getmtime(dest_db))}")
        shutil.copy2(dest_db, backup_path)
        print(f"✓ Backed up existing DB to: {backup_path}")
    
    # Copy database
    print(f"\n📋 Copying database...")
    try:
        shutil.copy2(source_db, dest_db)
        print(f"✓ Database copied successfully")
    except Exception as e:
        print(f"❌ Failed to copy database: {e}")
        return False
    
    # Verify destination
    if not dest_db.exists():
        print(f"❌ Destination database not found after copy")
        return False
    
    dest_size = dest_db.stat().st_size / (1024 * 1024)  # MB
    print(f"📊 Destination DB size: {dest_size:.2f} MB")
    
    if source_size != dest_size:
        print(f"⚠️  Size mismatch! Source: {source_size:.2f} MB, Dest: {dest_size:.2f} MB")
        return False
    
    # Verify database integrity
    print(f"\n🔍 Verifying database integrity...")
    try:
        conn = sqlite3.connect(str(dest_db))
        cur = conn.execute("SELECT COUNT(*) FROM incidents")
        incident_count = cur.fetchone()[0]
        
        cur = conn.execute("SELECT COUNT(*) FROM incident_enrichments_flat WHERE is_education_related = 1")
        enriched_count = cur.fetchone()[0]
        
        conn.close()
        
        print(f"✓ Database verified")
        print(f"   Incidents: {incident_count}")
        print(f"   Enriched (education): {enriched_count}")
    except Exception as e:
        print(f"❌ Database verification failed: {e}")
        return False
    
    print("\n" + "="*70)
    print("✅ Migration completed successfully!")
    print("="*70)
    print(f"\nDatabase is now at: {dest_db}")
    print(f"Make sure EDU_CTI_DATA_DIR=/app/data is set in Railway environment variables.")
    
    return True


if __name__ == "__main__":
    success = migrate_database()
    sys.exit(0 if success else 1)
