"""Tests for SERP URL persistence in fetching_strategy._append_url_to_incident."""

import sqlite3
from pathlib import Path

import pytest

from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
from src.edu_cti.pipeline.phase2.utils.fetching_strategy import _append_url_to_incident


@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_incident_enrichments_table(conn)
    yield conn
    conn.close()


def _insert(conn, incident_id: str, all_urls):
    urls_list = [u.strip() for u in (all_urls or "").split(";") if u.strip()]
    inc = BaseIncident(
        incident_id=incident_id,
        source="test",
        source_event_id=incident_id,
        title="Test Incident",
        institution_name="Test University",
        victim_raw_name="Test University",
        institution_type="university",
        country="United States",
        region=None,
        city=None,
        incident_date="2025-01-01",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        subtitle=None,
        primary_url=None,
        all_urls=urls_list,
        status="confirmed",
        source_confidence="high",
    )
    insert_incident(conn, inc)
    conn.commit()


class TestAppendUrlToIncident:

    def test_appends_new_url(self, temp_db):
        """New URL is added to empty all_urls."""
        _insert(temp_db, "test_001", None)
        _append_url_to_incident(temp_db, "test_001", "https://example.com/article")
        row = temp_db.execute(
            "SELECT all_urls FROM incidents WHERE incident_id = ?", ("test_001",)
        ).fetchone()
        assert "https://example.com/article" in (row[0] or "")

    def test_appends_to_existing_urls(self, temp_db):
        """New URL is appended to existing semicolon-separated list."""
        _insert(temp_db, "test_002", "https://original.com/article")
        _append_url_to_incident(temp_db, "test_002", "https://serp.com/news")
        row = temp_db.execute(
            "SELECT all_urls FROM incidents WHERE incident_id = ?", ("test_002",)
        ).fetchone()
        urls = {u.strip() for u in (row[0] or "").split(";") if u.strip()}
        assert "https://original.com/article" in urls
        assert "https://serp.com/news" in urls

    def test_no_duplicate_added(self, temp_db):
        """Appending a URL already in all_urls does not create duplicates."""
        existing = "https://original.com/article"
        _insert(temp_db, "test_003", existing)
        _append_url_to_incident(temp_db, "test_003", existing)
        row = temp_db.execute(
            "SELECT all_urls FROM incidents WHERE incident_id = ?", ("test_003",)
        ).fetchone()
        urls = [u.strip() for u in (row[0] or "").split(";") if u.strip()]
        assert urls.count(existing) == 1

    def test_unknown_incident_no_error(self, temp_db):
        """Non-existent incident_id silently does nothing."""
        _append_url_to_incident(temp_db, "nonexistent_id", "https://example.com/x")
        # No exception raised, no row created
        row = temp_db.execute(
            "SELECT 1 FROM incidents WHERE incident_id = ?", ("nonexistent_id",)
        ).fetchone()
        assert row is None
