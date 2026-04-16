import asyncio
from datetime import datetime
from unittest.mock import patch

import pytest

from src.edu_cti.api.database import count_education_incidents, get_dashboard_stats
from src.edu_cti.api.main import get_stats
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
        university_name="Test University",
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
        (incident_id, is_education_related, created_at, updated_at, enriched_summary)
        VALUES (?, 1, ?, ?, ?)
        """,
        (incident.incident_id, now, now, "Real incident"),
    )
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        """
        INSERT INTO incident_enrichments_flat
        (incident_id, is_education_related, created_at, updated_at, enriched_summary)
        VALUES (?, 1, ?, ?, ?)
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
    assert get_dashboard_stats(conn)["education_incidents"] == 1


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
