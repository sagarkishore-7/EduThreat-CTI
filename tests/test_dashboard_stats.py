import asyncio
import sqlite3
from datetime import datetime
from unittest.mock import patch

import pytest

from src.edu_cti.api.database import (
    count_education_incidents,
    get_dashboard_stats,
    get_incident_by_id,
    get_incidents_by_country,
    get_incidents_paginated,
    get_incidents_by_ransomware_family,
    get_recent_incidents,
    get_attack_vector_by_institution,
    get_ransom_economics,
    get_threat_actor_categories,
)
from src.edu_cti.api.main import get_stats
from src.edu_cti.core.countries import normalize_countries_in_database
from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table


@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "dashboard.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_incident_enrichments_table(conn)
    yield conn
    conn.close()


def _sample_incident() -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id("test_source", "https://example.com/dashboard|2025-01-15"),
        source="test_source",
        source_event_id="dashboard_event",
        institution_name="Test University",
        victim_raw_name="Test University",
        institution_type="university",
        country="United States",
        region="North America",
        city=None,
        incident_date="2025-01-15",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        title="Dashboard stats incident",
        subtitle=None,
        primary_url=None,
        all_urls=["https://example.com/article"],
        attack_type_hint="ransomware",
        status="confirmed",
        source_confidence="high",
    )


def test_education_incident_count_excludes_orphan_flat_rows(temp_db):
    conn = temp_db
    incident = _sample_incident()
    insert_incident(conn, incident)
    orphan_source = _sample_incident()
    orphan_source.incident_id = make_incident_id("test_source", "https://example.com/orphan|2025-01-16")
    orphan_source.source_event_id = "orphan_event"
    orphan_source.incident_date = "2025-01-16"
    orphan_source.title = "Orphan source incident"
    insert_incident(conn, orphan_source)

    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, attack_category, threat_actor_name, was_ransom_demanded, ransom_amount, created_at, updated_at, enriched_summary)
        VALUES (?, 1, 'ransomware_double_extortion', 'Vice Society', 1, 100000, ?, ?, ?)
        """,
        (incident.incident_id, now, now, "Real incident"),
    )
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, attack_category, threat_actor_name, was_ransom_demanded, ransom_amount, created_at, updated_at, enriched_summary)
        VALUES (?, 1, 'ransomware_double_extortion', 'Ghost Family', 1, 200000, ?, ?, ?)
        """,
        (orphan_source.incident_id, now, now, "Will become orphaned"),
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        "DELETE FROM incidents WHERE incident_id = ?",
        (orphan_source.incident_id,),
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")

    assert count_education_incidents(conn) == 1

    stats = get_dashboard_stats(conn)
    assert stats["education_incidents"] == 1
    assert stats["incidents_with_ransomware"] == 1
    assert stats["unique_ransomware_families"] == 1

    ransomware = get_incidents_by_ransomware_family(conn, limit=10)
    assert ransomware == [{"category": "Vice Society", "count": 1, "percentage": 100.0}]

    economics = get_ransom_economics(conn)
    assert economics["total_ransomware"] == 1
    assert economics["demanded_count"] == 1
    assert economics["total_demanded"] == 100000


def test_threat_actor_and_impact_analytics_exclude_orphan_rows(temp_db):
    conn = temp_db
    incident = _sample_incident()
    insert_incident(conn, incident)

    orphan_source = _sample_incident()
    orphan_source.incident_id = make_incident_id("test_source", "https://example.com/orphan-actor|2025-01-17")
    orphan_source.source_event_id = "orphan_actor_event"
    orphan_source.incident_date = "2025-01-17"
    orphan_source.title = "Orphan actor incident"
    insert_incident(conn, orphan_source)

    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, institution_type, attack_vector, threat_actor_name, created_at, updated_at, enriched_summary)
        VALUES (?, 1, 'university_public', 'ransomware', 'Vice Society', ?, ?, ?)
        """,
        (incident.incident_id, now, now, "Real actor incident"),
    )
    conn.execute(
        """
        INSERT INTO incident_enrichments
        (incident_id, enrichment_data, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            incident.incident_id,
            '{"attack_dynamics":{"threat_actor_category":"ransomware_group"}}',
            now,
            now,
        ),
    )

    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, institution_type, attack_vector, threat_actor_name, created_at, updated_at, enriched_summary)
        VALUES (?, 1, 'research_institute', 'ransomware', 'Ghost Actor', ?, ?, ?)
        """,
        (orphan_source.incident_id, now, now, "Ghost actor incident"),
    )
    conn.execute(
        """
        INSERT INTO incident_enrichments
        (incident_id, enrichment_data, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            orphan_source.incident_id,
            '{"attack_dynamics":{"threat_actor_category":"ghost_group"}}',
            now,
            now,
        ),
    )
    conn.commit()
    conn.execute(
        "DELETE FROM incidents WHERE incident_id = ?",
        (orphan_source.incident_id,),
    )
    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")

    actor_categories = get_threat_actor_categories(conn)
    assert actor_categories == [{"category": "ransomware_group", "count": 1, "percentage": 100.0}]

    attack_vectors = get_attack_vector_by_institution(conn, limit=10)
    assert attack_vectors["institution_types"] == ["university_public"]
    assert attack_vectors["vectors"] == ["ransomware"]
    assert attack_vectors["data"] == [
        {"institution_type": "university_public", "attack_vector": "ransomware", "count": 1}
    ]


def test_stats_endpoint_bypasses_cache_while_pipeline_is_running():
    stats_data = {
        "total_incidents": 10,
        "education_incidents": 7,
        "enriched_incidents": 8,
        "unenriched_incidents": 2,
        "incidents_with_ransomware": 3,
        "incidents_with_data_breach": 2,
        "countries_affected": 4,
        "unique_threat_actors": 2,
        "unique_ransomware_families": 2,
        "data_sources": 5,
        "avg_recovery_days": 11.2,
        "total_financial_impact": 0.0,
        "incidents_with_mitre": 1,
        "last_updated": "2026-04-15T11:30:00Z",
    }

    class _DummyConn:
        def close(self):
            return None

    with patch("src.edu_cti.api.main._pipeline_is_running", return_value=True), \
         patch("src.edu_cti.api.main.cache_get") as mock_cache_get, \
         patch("src.edu_cti.api.main.cache_set") as mock_cache_set, \
         patch("src.edu_cti.api.main.get_api_connection", return_value=_DummyConn()), \
         patch("src.edu_cti.api.main.get_dashboard_stats", return_value=stats_data):
        result = asyncio.run(get_stats())

    assert result.education_incidents == 7
    mock_cache_get.assert_not_called()
    mock_cache_set.assert_not_called()


def test_get_incident_by_id_prefers_normalized_enrichment_country(temp_db):
    conn = temp_db
    incident = _sample_incident()
    incident.country = "USA"
    incident.country_code = None
    insert_incident(conn, incident)

    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, country, country_code, created_at, updated_at, enriched_summary)
        VALUES (?, 1, ?, ?, ?, ?, ?)
        """,
        (
            incident.incident_id,
            "United States",
            "US",
            now,
            now,
            "Normalized country from enrichment",
        ),
    )
    conn.commit()

    data = get_incident_by_id(conn, incident.incident_id)

    assert data["country"] == "United States"
    assert data["country_code"] == "US"


def test_incident_summaries_expose_ai_descriptions(temp_db):
    conn = temp_db
    incident = _sample_incident()
    incident.subtitle = '<a href="https://news.google.com/rss/articles/example">RSS item</a>'
    insert_incident(conn, incident)

    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, country, country_code, enriched_summary, created_at, updated_at)
        VALUES (?, 1, ?, ?, ?, ?, ?)
        """,
        (
            incident.incident_id,
            "United States",
            "US",
            "AI summary for the incident.",
            now,
            now,
        ),
    )
    conn.commit()

    incidents, total = get_incidents_paginated(conn, per_page=10)
    assert total == 1
    assert incidents[0]["country_code"] == "US"
    assert incidents[0]["subtitle"] == incident.subtitle
    assert incidents[0]["enriched_summary"] == "AI summary for the incident."

    recent = get_recent_incidents(conn, limit=5)
    assert recent[0]["enriched_summary"] == "AI summary for the incident."


def test_normalize_countries_handles_legacy_flat_schema_without_country_code(tmp_path):
    db_path = tmp_path / "legacy_countries.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE incidents (
            incident_id TEXT PRIMARY KEY,
            country TEXT,
            country_code TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE incident_enrichments_flat (
            incident_id TEXT PRIMARY KEY,
            country TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO incidents (incident_id, country, country_code) VALUES (?, ?, ?)",
        ("incident_1", "USA", None),
    )
    conn.execute(
        "INSERT INTO incident_enrichments_flat (incident_id, country) VALUES (?, ?)",
        ("incident_1", "US"),
    )
    conn.commit()

    updated = normalize_countries_in_database(conn)

    incident_row = conn.execute(
        "SELECT country, country_code FROM incidents WHERE incident_id = ?",
        ("incident_1",),
    ).fetchone()
    flat_row = conn.execute(
        "SELECT country FROM incident_enrichments_flat WHERE incident_id = ?",
        ("incident_1",),
    ).fetchone()
    conn.close()

    assert updated >= 2
    assert incident_row["country"] == "United States"
    assert incident_row["country_code"] == "US"
    assert flat_row["country"] == "United States"


def test_normalize_countries_fills_missing_codes_for_supported_non_us_countries(tmp_path):
    db_path = tmp_path / "legacy_missing_codes.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE incidents (
            incident_id TEXT PRIMARY KEY,
            country TEXT,
            country_code TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE incident_enrichments_flat (
            incident_id TEXT PRIMARY KEY,
            country TEXT,
            country_code TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO incidents (incident_id, country, country_code) VALUES (?, ?, ?)",
        ("incident_ly", "Libya", None),
    )
    conn.execute(
        "INSERT INTO incident_enrichments_flat (incident_id, country, country_code) VALUES (?, ?, ?)",
        ("incident_ly", "LY", None),
    )
    conn.commit()

    updated = normalize_countries_in_database(conn)

    incident_row = conn.execute(
        "SELECT country, country_code FROM incidents WHERE incident_id = ?",
        ("incident_ly",),
    ).fetchone()
    flat_row = conn.execute(
        "SELECT country, country_code FROM incident_enrichments_flat WHERE incident_id = ?",
        ("incident_ly",),
    ).fetchone()
    conn.close()

    assert updated >= 2
    assert incident_row["country"] == "Libya"
    assert incident_row["country_code"] == "LY"
    assert flat_row["country"] == "Libya"
    assert flat_row["country_code"] == "LY"


def test_get_incidents_by_country_returns_flag_for_libya(temp_db):
    conn = temp_db
    incident = _sample_incident()
    incident.incident_id = make_incident_id("test_source", "https://example.com/libya|2025-01-15")
    incident.source_event_id = "libya_event"
    incident.country = "Libya"
    incident.country_code = "LY"
    insert_incident(conn, incident)

    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, country, country_code, created_at, updated_at, enriched_summary)
        VALUES (?, 1, ?, ?, ?, ?, ?)
        """,
        (
            incident.incident_id,
            "Libya",
            "LY",
            now,
            now,
            "Libya incident",
        ),
    )
    conn.commit()

    data = get_incidents_by_country(conn, limit=10)
    libya = next(item for item in data if item["category"] == "Libya")

    assert libya["country_code"] == "LY"
    assert libya["flag_emoji"] == "🇱🇾"
