"""
End-to-end pipeline tests.

Each test simulates the full data journey for a specific field or failure mode:

  LLM JSON output
    → _flatten_enrichment_for_db()  (db.py)
    → SQLite (in-memory)
    → get_incident_by_id()           (api/database.py)
    → Pydantic model construction    (api/main.py pattern)
    → assert correct value reaches the API layer

This catches silent field-drops and type mismatches that unit tests miss
because they only test individual layers in isolation.
"""

import json
import sqlite3
from datetime import datetime
from typing import Any, Optional, Dict
from unittest.mock import MagicMock, NonCallableMagicMock

import pytest
from pydantic import ValidationError

from src.edu_cti.api.database import get_incident_by_id
from src.edu_cti.api.models import DataImpact, SystemImpact, IncidentDetail
from src.edu_cti.pipeline.phase2.storage.db import (
    _flatten_enrichment_for_db,
    init_incident_enrichments_table,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _create_db() -> sqlite3.Connection:
    """In-memory SQLite with the full pipeline schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.execute("""
        CREATE TABLE incidents (
            incident_id TEXT PRIMARY KEY,
            institution_name TEXT,
            victim_raw_name TEXT,
            institution_type TEXT,
            country TEXT,
            region TEXT,
            city TEXT,
            incident_date TEXT,
            date_precision TEXT,
            title TEXT,
            subtitle TEXT,
            attack_type_hint TEXT,
            attack_category TEXT,
            status TEXT DEFAULT 'suspected',
            source_confidence TEXT DEFAULT 'medium',
            all_urls TEXT,
            primary_url TEXT,
            leak_site_url TEXT,
            source_published_date TEXT,
            llm_enriched INTEGER DEFAULT 0,
            llm_enriched_at TEXT,
            ingested_at TEXT,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE incident_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id TEXT NOT NULL,
            source TEXT,
            source_event_id TEXT,
            first_seen_at TEXT,
            confidence TEXT
        )
    """)
    init_incident_enrichments_table(conn)
    conn.commit()
    return conn


def _insert_incident(conn: sqlite3.Connection, incident_id: str, institution_name: str = "Test University") -> None:
    conn.execute(
        "INSERT INTO incidents (incident_id, institution_name, ingested_at) VALUES (?, ?, ?)",
        (incident_id, institution_name, datetime.utcnow().isoformat()),
    )
    conn.commit()


def _stub_enrichment(
    *,
    data_impact: Optional[dict] = None,
    system_impact: Optional[dict] = None,
    attack_dynamics=None,
) -> MagicMock:
    """Minimal CTIEnrichmentResult stub.

    All attributes default to None so _flatten_enrichment_for_db skips them.
    Pass dict values for data_impact / system_impact to test fallback paths.
    """
    e = MagicMock()
    e.education_relevance = None
    e.attack_dynamics = attack_dynamics  # None or real mock with .ransomware_family etc.
    e.data_impact = data_impact          # None or dict with .get()
    e.system_impact = system_impact      # None or dict with .get()
    e.operational_impact = None
    e.user_impact = None
    e.financial_impact = None
    e.regulatory_impact = None
    e.recovery_metrics = None
    e.transparency_metrics = None
    e.timeline = None
    e.mitre_attack_techniques = None
    e.initial_access_description = None
    return e


def _write_flat(conn: sqlite3.Connection, incident_id: str, raw_json: dict[str, Any]) -> None:
    """Run the flat-dict builder and insert into incident_enrichments_flat."""
    flat = _flatten_enrichment_for_db(_stub_enrichment(), raw_json_data=raw_json)
    flat["incident_id"] = incident_id
    flat["created_at"] = datetime.utcnow().isoformat()
    flat["updated_at"] = datetime.utcnow().isoformat()
    flat["enriched_at"] = datetime.utcnow().isoformat()

    cols = [k for k, v in flat.items() if v is not None and not isinstance(v, (MagicMock, NonCallableMagicMock))]
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    conn.execute(
        f"INSERT OR REPLACE INTO incident_enrichments_flat ({col_names}) VALUES ({placeholders})",
        [flat[c] for c in cols],
    )

    # Also write stub to incident_enrichments (required for get_incident_by_id full-JSON path)
    conn.execute(
        """INSERT OR REPLACE INTO incident_enrichments
           (incident_id, final_enrichment_json, created_at, updated_at)
           VALUES (?, ?, ?, ?)""",
        (incident_id, json.dumps(raw_json), flat["created_at"], flat["updated_at"]),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Happy path — all fields flow through
# ---------------------------------------------------------------------------

class TestHappyPath:
    def test_scalar_fields_reach_api(self):
        conn = _create_db()
        _insert_incident(conn, "test_001")
        _write_flat(conn, "test_001", {
            "incident_severity": "high",
            "attack_category": "ransomware",
            "threat_actor_category": "ransomware_gang",
            "threat_actor_motivation": "financial",
            "threat_actor_origin_country": "Russia",
            "ransomware_family": "LockBit",
        })

        incident = get_incident_by_id(conn, "test_001")
        assert incident["incident_severity"] == "high"
        assert incident["attack_category"] == "ransomware"
        assert incident["threat_actor_category"] == "ransomware_gang"
        assert incident["threat_actor_motivation"] == "financial"
        assert incident["ransomware_family"] == "LockBit"

    def test_data_categories_list_roundtrip(self):
        conn = _create_db()
        _insert_incident(conn, "test_002")
        _write_flat(conn, "test_002", {
            "data_categories": ["student_pii", "employee_ssn", "financial_records"],
        })

        incident = get_incident_by_id(conn, "test_002")
        assert incident["data_categories"] == ["student_pii", "employee_ssn", "financial_records"]
        # Must be accepted by Pydantic without ValidationError
        di = DataImpact(data_categories=incident["data_categories"])
        assert di.data_categories == ["student_pii", "employee_ssn", "financial_records"]

    def test_systems_affected_list_roundtrip(self):
        conn = _create_db()
        _insert_incident(conn, "test_003")
        _write_flat(conn, "test_003", {
            "systems_affected_codes": ["email", "lms", "student_portal"],
        })

        incident = get_incident_by_id(conn, "test_003")
        assert incident["systems_affected"] == ["email", "lms", "student_portal"]
        si = SystemImpact(systems_affected=incident["systems_affected"])
        assert si.systems_affected == ["email", "lms", "student_portal"]

    def test_boolean_fields_roundtrip(self):
        conn = _create_db()
        _insert_incident(conn, "test_004")
        _write_flat(conn, "test_004", {
            "data_breached": True,
            "data_exfiltrated": True,
            "gdpr_breach": True,
            "hipaa_breach": False,
            "ferpa_breach": True,
            "from_backup": True,
            "mfa_implemented": False,
        })

        incident = get_incident_by_id(conn, "test_004")
        assert incident["data_breached"] in (True, 1)
        assert incident["gdpr_breach"] in (True, 1)

    def test_integer_fields_roundtrip(self):
        conn = _create_db()
        _insert_incident(conn, "test_005")
        _write_flat(conn, "test_005", {
            "records_affected_exact": 52000,
            "records_affected_min": 40000,
            "records_affected_max": 60000,
            "pii_records_leaked": 15000,
        })

        incident = get_incident_by_id(conn, "test_005")
        assert incident["records_affected_exact"] == 52000
        assert incident["records_affected_min"] == 40000

    def test_timeline_dates_reach_api_detail(self):
        conn = _create_db()
        _insert_incident(conn, "test_timeline_001")
        _write_flat(conn, "test_timeline_001", {
            "attack_category": "ransomware",
        })
        conn.execute(
            """
            INSERT INTO incident_timeline
                (incident_id, seq_order, event_date, date_precision, event_type, event_description, actor_attribution)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "test_timeline_001",
                0,
                "2024-02-20",
                "day",
                "initial_access",
                "Suspicious network activity detected",
                None,
            ),
        )
        conn.commit()

        incident = get_incident_by_id(conn, "test_timeline_001")
        assert incident["timeline"] == [
            {
                "date": "2024-02-20",
                "date_precision": "day",
                "event_type": "initial_access",
                "event_description": "Suspicious network activity detected",
                "actor_attribution": None,
            }
        ]

        detail = IncidentDetail(**incident)
        assert detail.timeline is not None
        assert detail.timeline[0].date == "2024-02-20"


# ---------------------------------------------------------------------------
# String-vs-list bug (the regression that caused 500s)
# ---------------------------------------------------------------------------

class TestStringVsListCoercion:
    """LLM emits a bare string; pipeline must coerce to list before Pydantic."""

    def test_data_categories_bare_string_coerced(self):
        """When LLM outputs data_categories as a string, pipeline coerces to [str]."""
        conn = _create_db()
        _insert_incident(conn, "test_str_001")

        # Manually write JSON-encoded string (what json.dumps("student_pii") produces)
        flat = _flatten_enrichment_for_db(_stub_enrichment(), raw_json_data={})
        flat["incident_id"] = "test_str_001"
        flat["data_categories"] = json.dumps("student_pii")  # '"student_pii"'
        flat["created_at"] = flat["updated_at"] = flat["enriched_at"] = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO incident_enrichments_flat (incident_id, data_categories, created_at, updated_at, enriched_at) VALUES (?, ?, ?, ?, ?)",
            (flat["incident_id"], flat["data_categories"], flat["created_at"], flat["updated_at"], flat["enriched_at"]),
        )
        conn.execute(
            "INSERT OR REPLACE INTO incident_enrichments (incident_id, final_enrichment_json, created_at, updated_at) VALUES (?, ?, ?, ?)",
            ("test_str_001", json.dumps({}), flat["created_at"], flat["updated_at"]),
        )
        conn.commit()

        incident = get_incident_by_id(conn, "test_str_001")

        # database.py must coerce string → list
        cats = incident.get("data_categories")
        assert cats == ["student_pii"], f"Expected ['student_pii'], got {cats!r}"

        # Must not raise ValidationError
        di = DataImpact(data_categories=cats)
        assert di.data_categories == ["student_pii"]

    def test_systems_affected_bare_string_coerced(self):
        conn = _create_db()
        _insert_incident(conn, "test_str_002")

        flat_ts = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO incident_enrichments_flat (incident_id, systems_affected_codes, created_at, updated_at, enriched_at) VALUES (?, ?, ?, ?, ?)",
            ("test_str_002", json.dumps("email_system"), flat_ts, flat_ts, flat_ts),
        )
        conn.execute(
            "INSERT OR REPLACE INTO incident_enrichments (incident_id, final_enrichment_json, created_at, updated_at) VALUES (?, ?, ?, ?)",
            ("test_str_002", json.dumps({}), flat_ts, flat_ts),
        )
        conn.commit()

        incident = get_incident_by_id(conn, "test_str_002")
        systems = incident.get("systems_affected")
        assert systems == ["email_system"], f"Expected ['email_system'], got {systems!r}"

        si = SystemImpact(systems_affected=systems)
        assert si.systems_affected == ["email_system"]

    def test_pydantic_rejects_raw_string_directly(self):
        """Confirm the Pydantic contract: strings must never reach the model directly."""
        with pytest.raises(ValidationError):
            DataImpact(data_categories="student_pii")

        with pytest.raises(ValidationError):
            SystemImpact(systems_affected="email")

    def test_empty_json_string_becomes_none(self):
        """json.dumps("") → '""'; parsed back → ""; falsy string → None."""
        conn = _create_db()
        _insert_incident(conn, "test_str_003")

        flat_ts = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO incident_enrichments_flat (incident_id, data_categories, created_at, updated_at, enriched_at) VALUES (?, ?, ?, ?, ?)",
            ("test_str_003", json.dumps(""), flat_ts, flat_ts, flat_ts),
        )
        conn.execute(
            "INSERT OR REPLACE INTO incident_enrichments (incident_id, final_enrichment_json, created_at, updated_at) VALUES (?, ?, ?, ?)",
            ("test_str_003", json.dumps({}), flat_ts, flat_ts),
        )
        conn.commit()

        incident = get_incident_by_id(conn, "test_str_003")
        # Empty-string JSON should resolve to None, not [""]
        cats = incident.get("data_categories")
        assert cats is None or cats == [], f"Expected None or [], got {cats!r}"


# ---------------------------------------------------------------------------
# Silent field-drop detection
# ---------------------------------------------------------------------------

class TestFieldDropDetection:
    """New fields added to the schema must survive the full pipeline."""

    def test_incident_severity_not_dropped(self):
        conn = _create_db()
        _insert_incident(conn, "test_drop_001")
        _write_flat(conn, "test_drop_001", {"incident_severity": "critical"})
        incident = get_incident_by_id(conn, "test_drop_001")
        assert incident.get("incident_severity") == "critical", "incident_severity silently dropped"

    def test_threat_actor_category_not_dropped(self):
        conn = _create_db()
        _insert_incident(conn, "test_drop_002")
        _write_flat(conn, "test_drop_002", {"threat_actor_category": "apt_nation_state"})
        incident = get_incident_by_id(conn, "test_drop_002")
        assert incident.get("threat_actor_category") == "apt_nation_state"

    def test_threat_actor_origin_country_not_dropped(self):
        conn = _create_db()
        _insert_incident(conn, "test_drop_003")
        _write_flat(conn, "test_drop_003", {"threat_actor_origin_country": "North Korea"})
        incident = get_incident_by_id(conn, "test_drop_003")
        assert incident.get("threat_actor_origin_country") == "North Korea"

    def test_forensics_firm_not_dropped(self):
        conn = _create_db()
        _insert_incident(conn, "test_drop_004")
        _write_flat(conn, "test_drop_004", {"forensics_firm": "Mandiant"})
        incident = get_incident_by_id(conn, "test_drop_004")
        assert incident.get("forensics_firm") == "Mandiant"

    def test_regulatory_flags_not_dropped(self):
        conn = _create_db()
        _insert_incident(conn, "test_drop_005")
        _write_flat(conn, "test_drop_005", {
            "gdpr_breach": True,
            "hipaa_breach": False,
            "ferpa_breach": True,
        })
        incident = get_incident_by_id(conn, "test_drop_005")
        assert incident.get("gdpr_breach") in (True, 1)
        assert incident.get("ferpa_breach") in (True, 1)

    def test_vendor_name_not_dropped(self):
        conn = _create_db()
        _insert_incident(conn, "test_drop_006")
        _write_flat(conn, "test_drop_006", {
            "third_party_vendor_impact": True,
            "vendor_name": "PowerSchool",
        })
        incident = get_incident_by_id(conn, "test_drop_006")
        assert incident.get("vendor_name") == "PowerSchool"


# ---------------------------------------------------------------------------
# data_impact fallback: data_types_affected → data_categories
# ---------------------------------------------------------------------------

class TestFallbackPaths:
    def test_data_types_affected_fallback_reaches_api(self):
        """When data_categories absent, data_impact.data_types_affected is the fallback."""
        e = _stub_enrichment(data_impact={"data_types_affected": ["health_records", "ssn"]})
        flat = _flatten_enrichment_for_db(e, raw_json_data={})
        assert flat["data_categories"] is not None
        assert json.loads(flat["data_categories"]) == ["health_records", "ssn"]

    def test_explicit_data_categories_takes_priority_over_fallback(self):
        e = _stub_enrichment(data_impact={"data_types_affected": ["health_records"]})
        flat = _flatten_enrichment_for_db(e, raw_json_data={"data_categories": ["student_pii"]})
        assert json.loads(flat["data_categories"]) == ["student_pii"]

    def test_ransomware_family_fallback_chain(self):
        """ransomware_family_or_group → ransomware_family → attack_dynamics."""
        ad = MagicMock()
        ad.ransomware_family = "LockBit"
        ad.attack_vector = None
        ad.data_exfiltration = None
        ad.ransom_demanded = None
        ad.ransom_paid = None

        e = _stub_enrichment(attack_dynamics=ad)

        # Top-level key wins
        flat = _flatten_enrichment_for_db(e, raw_json_data={"ransomware_family_or_group": "BlackCat"})
        assert flat["ransomware_family"] == "BlackCat"

        # Falls back to attack_dynamics
        flat2 = _flatten_enrichment_for_db(e, raw_json_data={})
        assert flat2["ransomware_family"] == "LockBit"


# ---------------------------------------------------------------------------
# Pydantic IncidentDetail — full model construction doesn't crash
# ---------------------------------------------------------------------------

class TestIncidentDetailConstruction:
    """IncidentDetail must accept the dict that get_incident_by_id returns."""

    def test_fully_enriched_incident_builds_without_error(self):
        conn = _create_db()
        _insert_incident(conn, "test_full_001", "Pacific Northwest University")
        _write_flat(conn, "test_full_001", {
            "incident_severity": "critical",
            "attack_category": "ransomware",
            "threat_actor_name": "BlackCat",
            "threat_actor_category": "ransomware_gang",
            "threat_actor_motivation": "financial",
            "ransomware_family": "BlackCat/ALPHV",
            "data_categories": ["student_pii", "ssn", "financial_records"],
            "systems_affected_codes": ["email", "lms", "erp"],
            "records_affected_exact": 127500,
            "data_breached": True,
            "data_exfiltrated": True,
            "gdpr_breach": False,
            "hipaa_breach": False,
            "ferpa_breach": True,
            "was_ransom_demanded": True,
            "ransom_amount": 4750000,
            "ransom_paid": True,
            "from_backup": False,
            "forensics_firm": "CrowdStrike",
            "mfa_implemented": True,
        })

        incident = get_incident_by_id(conn, "test_full_001")
        assert incident is not None

        # Build Pydantic sub-objects exactly as main.py does
        data_cats = incident.get("data_categories")
        di = DataImpact(
            data_breached=incident.get("data_breached"),
            data_exfiltrated=incident.get("data_exfiltrated"),
            data_categories=data_cats if isinstance(data_cats, list) else None,
            records_affected_exact=incident.get("records_affected_exact"),
        )
        assert di.data_categories == ["student_pii", "ssn", "financial_records"]
        assert di.records_affected_exact == 127500

        systems = incident.get("systems_affected")
        si = SystemImpact(
            systems_affected=systems if isinstance(systems, list) else None,
            email_system_affected=incident.get("email_system_affected"),
        )
        assert si.systems_affected == ["email", "lms", "erp"]

    def test_sparse_incident_builds_without_error(self):
        """Incidents with mostly-null enrichment should not raise ValidationError."""
        conn = _create_db()
        _insert_incident(conn, "test_sparse_001")
        # No enrichment written — just the base incident row

        incident = get_incident_by_id(conn, "test_sparse_001")
        assert incident is not None

        di = DataImpact(
            data_categories=None,
            records_affected_exact=None,
        )
        assert di.data_categories is None

    def test_string_in_data_categories_blocked_before_pydantic(self):
        """Simulate a string slipping past the parse layer — main.py guard must catch it."""
        raw_string = "student_pii"  # not a list
        guarded = raw_string if isinstance(raw_string, list) else None
        di = DataImpact(data_categories=guarded)
        assert di.data_categories is None


# ---------------------------------------------------------------------------
# Google News RSS — _resolve_google_news_link removal regression
# ---------------------------------------------------------------------------

class TestGoogleNewsRSSCleanup:
    """After removing _resolve_google_news_link, items must not be silently dropped."""

    def test_function_no_longer_exists(self):
        import src.edu_cti.sources.rss.googlenews_rss as gnr
        assert not hasattr(gnr, "_resolve_google_news_link"), \
            "_resolve_google_news_link should have been removed — it silently dropped all items when googlenewsdecoder was not installed"

    def test_build_function_keeps_item_even_when_google_wrapper_cannot_be_resolved(self, monkeypatch):
        import src.edu_cti.sources.rss.googlenews_rss as gnr

        monkeypatch.setattr(gnr, "GOOGLE_NEWS_QUERIES", [("university ransomware", "en", "US")])
        monkeypatch.setattr(gnr, "_fetch_google_news_rss", lambda _url: [{
            "title": "University hit by ransomware",
            "link": "https://news.google.com/articles/CBMitest123",
            "pub_date": "Wed, 15 Apr 2026 10:00:00 +0000",
            "description": "Ransomware attack on State University",
            "source_name": "CyberNews",
        }])
        monkeypatch.setattr(gnr, "_resolve_google_news_article_url", lambda _url: None)
        monkeypatch.setattr(gnr.time, "sleep", lambda _: None)

        saved = []
        incidents = gnr.build_googlenews_rss_incidents(
            incremental=True,
            max_age_days=3650,
            save_callback=saved.extend,
        )

        # Item must not be dropped even though the URL is a google.com redirect.
        assert len(incidents) == 1, "Google News item was dropped — check link handling"
        # When the wrapper cannot be resolved immediately, the wrapper URL is
        # persisted (not dropped) so Phase 2 can retry resolution downstream.
        assert incidents[0].all_urls == ["https://news.google.com/articles/CBMitest123"]
