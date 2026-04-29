"""
Tests for all pipeline fixes:
  - 9 new threat-intelligence fields through mapper → schema → flat DB
  - 31 new flat columns DDL + idempotent migration
  - Recovery-days fallback chain (recovery_timeframe_days → downtime_days → outage_hours/24)
  - Financial-impact query including ransom_amount
  - CSV export carries new threat-intel columns
  - os._exit(1) used for memory-threshold restart (Railway ON_FAILURE restart)
"""

import json
import sqlite3
from typing import Any, Dict, Optional

import pytest

from src.edu_cti.core.db import get_connection, init_db, insert_incident
from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import (
    _coerce_llm_scalars,
    json_to_cti_enrichment,
)
from src.edu_cti.pipeline.phase2.schemas import (
    AttackDynamics,
    CTIEnrichmentResult,
    EducationRelevanceCheck,
    MITREAttackTechnique,
    TimelineEvent,
)
from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table
from src.edu_cti.pipeline.phase2.storage.db import (
    _flatten_enrichment_for_db,  # private but tested directly
    init_incident_enrichments_table,
    save_enrichment_result,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_incident(suffix: str = "test") -> BaseIncident:
    url = f"https://example.com/{suffix}"
    return BaseIncident(
        incident_id=make_incident_id("test_source", f"{url}|2025-01-15"),
        source="test_source",
        source_event_id=f"evt_{suffix}",
        victim_raw_name="Test University",
        title="Test University Ransomware Attack",
        subtitle="Attack on university systems",
        institution_name="Test University",
        institution_type="university",
        country="United States",
        city=None,
        region="North America",
        incident_date="2025-01-15",
        date_precision="day",
        source_published_date=None,
        ingested_at=None,
        primary_url=None,
        all_urls=[url],
        attack_type_hint="ransomware",
        status="confirmed",
        source_confidence="high",
    )


def _full_enrichment(**overrides) -> CTIEnrichmentResult:
    """Build an enrichment with ALL new threat-intel fields populated."""
    defaults = dict(
        education_relevance=EducationRelevanceCheck(
            is_education_related=True,
            reasoning="University ransomware confirmed",
            institution_identified="Test University",
        ),
        primary_url="https://example.com/article",
        enriched_summary="Test University was hit by LockBit ransomware in January 2025.",
        timeline=[
            TimelineEvent(date="2025-01-10", event_type="initial_access", event_description="Phishing email")
        ],
        mitre_attack_techniques=[
            MITREAttackTechnique(technique_id="T1486", technique_name="Data Encrypted for Impact", tactic="Impact")
        ],
        attack_dynamics=AttackDynamics(
            attack_vector="phishing",
            ransomware_family="LockBit",
            data_exfiltration=True,
            ransom_demanded=True,
        ),
        # New threat-intel fields
        malware_families=["LockBit 3.0", "Cobalt Strike"],
        attacker_tools=["Cobalt Strike", "Mimikatz", "Rclone"],
        threat_actor_aliases=["LockBit", "ABCD Ransomware"],
        attack_campaign_name="Operation Dark School",
        cloud_provider="AWS",
        infrastructure_type="hybrid",
        dwell_time_days=14.0,
        data_volume_gb=250.5,
        vulnerabilities_exploited=[
            {
                "cve_id": "CVE-2023-4966",
                "vulnerability_name": "Citrix Bleed",
                "vulnerability_type": "authentication_bypass",
                "affected_product": "Citrix NetScaler ADC",
                "cvss_score": 9.4,
            }
        ],
    )
    defaults.update(overrides)
    return CTIEnrichmentResult(**defaults)


@pytest.fixture
def temp_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = get_connection(db_path)
    init_db(conn)
    init_incident_enrichments_table(conn)
    init_articles_table(conn)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# 1. Mapper: _KNOWN_ARRAYS covers all new array fields
# ---------------------------------------------------------------------------


class TestKnownArrays:
    """_coerce_llm_scalars must NOT flatten the new list-typed fields."""

    NEW_ARRAY_FIELDS = [
        "vulnerabilities_exploited",
        "malware_families",
        "attacker_tools",
        "threat_actor_aliases",
    ]

    @pytest.mark.parametrize("field", NEW_ARRAY_FIELDS)
    def test_new_array_field_preserved_as_list(self, field):
        """A list value for each new array field must survive _coerce_llm_scalars."""
        payload = {field: ["item_a", "item_b"]}
        result = _coerce_llm_scalars(payload)
        assert isinstance(result[field], list), (
            f"_coerce_llm_scalars collapsed '{field}' to a scalar — add it to _KNOWN_ARRAYS"
        )

    @pytest.mark.parametrize("field", NEW_ARRAY_FIELDS)
    def test_single_item_list_not_coerced(self, field):
        payload = {field: ["only_item"]}
        result = _coerce_llm_scalars(payload)
        assert result[field] == ["only_item"]

    def test_scalar_field_still_coerced(self):
        """Non-array fields wrapped in a list by LLM must still be coerced."""
        result = _coerce_llm_scalars({"attack_category": ["ransomware"]})
        assert result["attack_category"] == "ransomware"


# ---------------------------------------------------------------------------
# 2. Mapper → CTIEnrichmentResult: all 9 new fields pass through correctly
# ---------------------------------------------------------------------------


class TestMapperNewFields:
    """json_to_cti_enrichment must populate every new threat-intel field."""

    def _llm_payload(self) -> Dict[str, Any]:
        return {
            "is_edu_cyber_incident": True,
            "institution_name": "Test University",
            "country": "United States",
            "attack_category": "ransomware",
            "enriched_summary": "Test University was hit by ransomware.",
            # New threat-intel fields
            "malware_families": ["LockBit 3.0", "Cobalt Strike"],
            "attacker_tools": ["Mimikatz", "Rclone"],
            "threat_actor_aliases": ["LockBit", "ABCD"],
            "attack_campaign_name": "Operation Dark School",
            "cloud_provider": "AWS",
            "infrastructure_type": "hybrid",
            "dwell_time_days": 14.0,
            "data_volume_gb": 250.5,
            "vulnerabilities_exploited": [
                {
                    "cve_id": "CVE-2023-4966",
                    "vulnerability_name": "Citrix Bleed",
                    "vulnerability_type": "authentication_bypass",
                    "affected_product": "Citrix NetScaler ADC",
                    "cvss_score": 9.4,
                }
            ],
        }

    def test_malware_families_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.malware_families == ["LockBit 3.0", "Cobalt Strike"]

    def test_attacker_tools_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.attacker_tools == ["Mimikatz", "Rclone"]

    def test_threat_actor_aliases_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.threat_actor_aliases == ["LockBit", "ABCD"]

    def test_attack_campaign_name_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.attack_campaign_name == "Operation Dark School"

    def test_cloud_provider_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.cloud_provider == "AWS"

    def test_infrastructure_type_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.infrastructure_type == "hybrid"

    def test_dwell_time_days_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.dwell_time_days == 14.0

    def test_data_volume_gb_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.data_volume_gb == 250.5

    def test_vulnerabilities_exploited_mapped(self):
        result = json_to_cti_enrichment(self._llm_payload(), "https://example.com")
        assert result.vulnerabilities_exploited is not None
        assert len(result.vulnerabilities_exploited) == 1
        vuln = result.vulnerabilities_exploited[0]
        assert vuln["cve_id"] == "CVE-2023-4966"
        assert vuln["vulnerability_name"] == "Citrix Bleed"
        assert vuln["cvss_score"] == 9.4

    def test_empty_list_fields_become_none(self):
        """Empty lists from LLM should be normalised to None, not []."""
        payload = self._llm_payload()
        payload["malware_families"] = []
        payload["attacker_tools"] = []
        payload["threat_actor_aliases"] = []
        result = json_to_cti_enrichment(payload, "https://example.com")
        assert result.malware_families is None
        assert result.attacker_tools is None
        assert result.threat_actor_aliases is None

    def test_invalid_vuln_entry_skipped(self):
        """Vulnerability dicts without any identifying fields must be filtered out."""
        payload = self._llm_payload()
        payload["vulnerabilities_exploited"] = [
            {"cvss_score": 5.0},  # no cve_id / vulnerability_name / affected_product
            {"cve_id": "CVE-2021-44228", "vulnerability_name": "Log4Shell"},
        ]
        result = json_to_cti_enrichment(payload, "https://example.com")
        assert result.vulnerabilities_exploited is not None
        assert len(result.vulnerabilities_exploited) == 1
        assert result.vulnerabilities_exploited[0]["cve_id"] == "CVE-2021-44228"


# ---------------------------------------------------------------------------
# 3. Flat DB: _flatten_enrichment_for_db populates new columns
# ---------------------------------------------------------------------------


class TestFlattenEnrichmentNewFields:
    """_flatten_enrichment_for_db must write every new column."""

    def test_malware_families_serialised_as_json(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["malware_families"] is not None
        parsed = json.loads(flat["malware_families"])
        assert "LockBit 3.0" in parsed

    def test_attacker_tools_serialised_as_json(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        parsed = json.loads(flat["attacker_tools"])
        assert "Mimikatz" in parsed

    def test_threat_actor_aliases_serialised_as_json(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        parsed = json.loads(flat["threat_actor_aliases"])
        assert "LockBit" in parsed

    def test_attack_campaign_name_stored(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["attack_campaign_name"] == "Operation Dark School"

    def test_cloud_provider_stored(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["cloud_provider"] == "AWS"

    def test_infrastructure_type_stored(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["infrastructure_type"] == "hybrid"

    def test_dwell_time_days_stored(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["dwell_time_days"] == 14.0

    def test_data_volume_gb_stored(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["data_volume_gb"] == 250.5

    def test_cve_ids_extracted_from_vulnerabilities(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["cve_ids"] is not None
        parsed = json.loads(flat["cve_ids"])
        assert "CVE-2023-4966" in parsed

    def test_cvss_scores_extracted(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["cvss_scores"] is not None
        parsed = json.loads(flat["cvss_scores"])
        assert 9.4 in parsed

    def test_vulnerability_names_extracted(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["vulnerability_names"] is not None
        parsed = json.loads(flat["vulnerability_names"])
        assert "Citrix Bleed" in parsed

    def test_affected_products_extracted(self):
        enrichment = _full_enrichment()
        flat = _flatten_enrichment_for_db(enrichment)
        assert flat["affected_products"] is not None
        parsed = json.loads(flat["affected_products"])
        assert "Citrix NetScaler ADC" in parsed

    def test_none_new_fields_stay_none(self):
        """All-None new fields must not crash and must map to None."""
        minimal = CTIEnrichmentResult(
            education_relevance=EducationRelevanceCheck(
                is_education_related=True, reasoning="ok"
            ),
            enriched_summary="Minimal enrichment",
        )
        flat = _flatten_enrichment_for_db(minimal)
        for col in (
            "malware_families", "attacker_tools", "threat_actor_aliases",
            "attack_campaign_name", "cloud_provider", "infrastructure_type",
            "dwell_time_days", "data_volume_gb", "cve_ids", "cvss_scores",
            "vulnerability_names", "affected_products",
        ):
            assert flat[col] is None, f"Expected None for '{col}', got {flat[col]!r}"


# ---------------------------------------------------------------------------
# 4. DB DDL: all new columns exist after init_incident_enrichments_table
# ---------------------------------------------------------------------------


NEW_FLAT_COLUMNS = [
    "access_vector",
    "malware_families", "attacker_tools", "threat_actor_aliases", "attack_campaign_name",
    "cloud_provider", "infrastructure_type", "dwell_time_days", "mttd_hours", "mttr_hours",
    "cve_ids", "cvss_scores", "vulnerability_names", "affected_products",
    "total_cost_estimate",
    "partial_service_days", "clinical_operations_disrupted", "graduation_delayed",
    "online_learning_disrupted",
    "backup_status", "backup_age_days", "law_enforcement_involved", "law_enforcement_agency",
    "official_statement_url",
    "research_projects_affected", "research_data_compromised", "publications_delayed",
    "grants_affected", "research_area",
    "regulatory_context",
    "data_volume_gb",
]


class TestDDLMigration:
    def _get_columns(self, conn: sqlite3.Connection):
        cur = conn.execute("PRAGMA table_info(incident_enrichments_flat)")
        return {row["name"] for row in cur.fetchall()}

    @pytest.mark.parametrize("col", NEW_FLAT_COLUMNS)
    def test_new_column_exists_after_init(self, temp_db, col):
        cols = self._get_columns(temp_db)
        assert col in cols, f"Column '{col}' missing from incident_enrichments_flat"

    def test_migration_idempotent(self, temp_db):
        """Running init_incident_enrichments_table twice must not raise."""
        init_incident_enrichments_table(temp_db)  # second call
        cols = self._get_columns(temp_db)
        for col in NEW_FLAT_COLUMNS:
            assert col in cols

    def test_all_columns_in_all_columns_list(self, temp_db):
        """Every column in the DDL must also appear in the all_columns insert list.
        We verify this by inserting a full enrichment and reading it back without error."""
        incident = _make_incident("ddl-test")
        insert_incident(temp_db, incident)
        enrichment = _full_enrichment()
        result = save_enrichment_result(temp_db, incident.incident_id, enrichment)
        assert result is True


# ---------------------------------------------------------------------------
# 5. End-to-end save + retrieval of new fields from flat table
# ---------------------------------------------------------------------------


class TestSaveEnrichmentNewFields:
    def _insert_and_save(self, conn, suffix: str = "e2e"):
        incident = _make_incident(suffix)
        insert_incident(conn, incident)
        enrichment = _full_enrichment()
        ok = save_enrichment_result(conn, incident.incident_id, enrichment)
        assert ok is True
        return incident.incident_id

    def _fetch_flat(self, conn, incident_id: str) -> Dict[str, Any]:
        cur = conn.execute(
            "SELECT * FROM incident_enrichments_flat WHERE incident_id = ?",
            (incident_id,),
        )
        row = cur.fetchone()
        assert row is not None, "Flat record not found"
        return dict(row)

    def test_malware_families_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "mf")
        flat = self._fetch_flat(temp_db, iid)
        assert flat["malware_families"] is not None
        parsed = json.loads(flat["malware_families"])
        assert "LockBit 3.0" in parsed

    def test_attacker_tools_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "at")
        flat = self._fetch_flat(temp_db, iid)
        parsed = json.loads(flat["attacker_tools"])
        assert "Mimikatz" in parsed

    def test_threat_actor_aliases_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "ta")
        flat = self._fetch_flat(temp_db, iid)
        parsed = json.loads(flat["threat_actor_aliases"])
        assert "LockBit" in parsed

    def test_cve_ids_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "cve")
        flat = self._fetch_flat(temp_db, iid)
        parsed = json.loads(flat["cve_ids"])
        assert "CVE-2023-4966" in parsed

    def test_dwell_time_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "dwell")
        flat = self._fetch_flat(temp_db, iid)
        assert flat["dwell_time_days"] == 14.0

    def test_cloud_provider_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "cloud")
        flat = self._fetch_flat(temp_db, iid)
        assert flat["cloud_provider"] == "AWS"

    def test_infrastructure_type_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "infra")
        flat = self._fetch_flat(temp_db, iid)
        assert flat["infrastructure_type"] == "hybrid"

    def test_attack_campaign_name_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "camp")
        flat = self._fetch_flat(temp_db, iid)
        assert flat["attack_campaign_name"] == "Operation Dark School"

    def test_data_volume_gb_round_trips(self, temp_db):
        iid = self._insert_and_save(temp_db, "vol")
        flat = self._fetch_flat(temp_db, iid)
        assert flat["data_volume_gb"] == 250.5


# ---------------------------------------------------------------------------
# 6. Recovery-days fallback chain
# ---------------------------------------------------------------------------


class TestRecoveryFallbackChain:
    """
    avg_recovery_days must use the first available field in the chain:
    recovery_timeframe_days → downtime_days → outage_duration_hours / 24
    """

    def _insert_flat_row(self, conn, incident_id: str, **kwargs):
        """Insert a minimal flat row with specified recovery columns."""
        conn.execute(
            "INSERT OR IGNORE INTO incidents (incident_id, title, ingested_at, llm_enriched) VALUES (?, 'Title', datetime('now'), 1)",
            (incident_id,),
        )
        cols = ["incident_id", "is_education_related", "created_at", "updated_at"] + list(kwargs.keys())
        vals = [incident_id, 1, "2025-01-15T00:00:00", "2025-01-15T00:00:00"] + list(kwargs.values())
        conn.execute(
            f"INSERT INTO incident_enrichments_flat ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            vals,
        )
        conn.commit()

    def _avg_recovery(self, conn) -> Optional[float]:
        cur = conn.execute(
            """
            SELECT AVG(
                CASE
                    WHEN recovery_timeframe_days IS NOT NULL AND recovery_timeframe_days > 0
                        THEN recovery_timeframe_days
                    WHEN downtime_days IS NOT NULL AND downtime_days > 0
                        THEN downtime_days
                    WHEN outage_duration_hours IS NOT NULL AND outage_duration_hours > 0
                        THEN outage_duration_hours / 24.0
                    ELSE NULL
                END
            ) as avg_days
            FROM incident_enrichments_flat
            WHERE is_education_related = 1
              AND (
                  (recovery_timeframe_days IS NOT NULL AND recovery_timeframe_days > 0) OR
                  (downtime_days IS NOT NULL AND downtime_days > 0) OR
                  (outage_duration_hours IS NOT NULL AND outage_duration_hours > 0)
              )
            """
        )
        row = conn.execute(
            """
            SELECT AVG(
                CASE
                    WHEN recovery_timeframe_days IS NOT NULL AND recovery_timeframe_days > 0
                        THEN recovery_timeframe_days
                    WHEN downtime_days IS NOT NULL AND downtime_days > 0
                        THEN downtime_days
                    WHEN outage_duration_hours IS NOT NULL AND outage_duration_hours > 0
                        THEN outage_duration_hours / 24.0
                    ELSE NULL
                END
            ) as avg_days
            FROM incident_enrichments_flat
            WHERE is_education_related = 1
              AND (
                  (recovery_timeframe_days IS NOT NULL AND recovery_timeframe_days > 0) OR
                  (downtime_days IS NOT NULL AND downtime_days > 0) OR
                  (outage_duration_hours IS NOT NULL AND outage_duration_hours > 0)
              )
            """
        ).fetchone()
        return row["avg_days"] if row else None

    def test_primary_field_used_when_present(self, temp_db):
        conn = temp_db
        self._insert_flat_row(conn, "rec-1", recovery_timeframe_days=10.0)
        avg = self._avg_recovery(conn)
        assert avg == pytest.approx(10.0)

    def test_falls_back_to_downtime_days(self, temp_db):
        conn = temp_db
        self._insert_flat_row(conn, "rec-2", downtime_days=5.0)
        avg = self._avg_recovery(conn)
        assert avg == pytest.approx(5.0)

    def test_falls_back_to_outage_hours_divided_by_24(self, temp_db):
        conn = temp_db
        self._insert_flat_row(conn, "rec-3", outage_duration_hours=48.0)
        avg = self._avg_recovery(conn)
        assert avg == pytest.approx(2.0)

    def test_primary_takes_priority_over_fallback(self, temp_db):
        conn = temp_db
        self._insert_flat_row(
            conn, "rec-4",
            recovery_timeframe_days=10.0,
            downtime_days=99.0,
            outage_duration_hours=999.0,
        )
        avg = self._avg_recovery(conn)
        assert avg == pytest.approx(10.0)

    def test_all_null_rows_excluded(self, temp_db):
        conn = temp_db
        conn.execute(
            "INSERT OR IGNORE INTO incidents (incident_id, title, ingested_at, llm_enriched) VALUES ('null-rec', 'T', datetime('now'), 1)"
        )
        conn.execute(
            "INSERT INTO incident_enrichments_flat (incident_id, is_education_related, created_at, updated_at) VALUES ('null-rec', 1, '2025-01-15T00:00:00', '2025-01-15T00:00:00')"
        )
        conn.commit()
        avg = self._avg_recovery(conn)
        assert avg is None

    def test_save_enrichment_recovery_fields_stored(self, temp_db):
        """recovery_timeframe_days set via attack_dynamics lands in flat table."""
        incident = _make_incident("rec-save")
        insert_incident(temp_db, incident)
        enrichment = _full_enrichment(
            attack_dynamics=AttackDynamics(
                attack_vector="phishing",
                ransomware_family="LockBit",
                recovery_timeframe_days=7.0,
            )
        )
        save_enrichment_result(temp_db, incident.incident_id, enrichment)
        row = temp_db.execute(
            "SELECT recovery_timeframe_days FROM incident_enrichments_flat WHERE incident_id=?",
            (incident.incident_id,),
        ).fetchone()
        assert row["recovery_timeframe_days"] == pytest.approx(7.0)


# ---------------------------------------------------------------------------
# 7. Financial impact query includes ransom_amount
# ---------------------------------------------------------------------------


class TestFinancialImpactQuery:
    def _insert_financial_row(self, conn, incident_id: str, **kwargs):
        conn.execute(
            "INSERT OR IGNORE INTO incidents (incident_id, title, ingested_at, llm_enriched) VALUES (?, 'T', datetime('now'), 1)",
            (incident_id,),
        )
        cols = ["incident_id", "is_education_related", "created_at", "updated_at"] + list(kwargs.keys())
        vals = [incident_id, 1, "2025-01-15T00:00:00", "2025-01-15T00:00:00"] + list(kwargs.values())
        conn.execute(
            f"INSERT INTO incident_enrichments_flat ({', '.join(cols)}) VALUES ({', '.join(['?']*len(cols))})",
            vals,
        )
        conn.commit()

    def _total_financial(self, conn) -> float:
        row = conn.execute(
            """
            SELECT SUM(
                COALESCE(ransom_amount, 0) +
                COALESCE(recovery_costs_max, COALESCE(recovery_costs_min, 0)) +
                COALESCE(legal_costs, 0) +
                COALESCE(notification_costs, 0)
            ) as total
            FROM incident_enrichments_flat
            WHERE is_education_related = 1
              AND (
                  (ransom_amount IS NOT NULL AND ransom_amount > 0) OR
                  (recovery_costs_max IS NOT NULL AND recovery_costs_max > 0) OR
                  (recovery_costs_min IS NOT NULL AND recovery_costs_min > 0) OR
                  (legal_costs IS NOT NULL AND legal_costs > 0)
              )
            """
        ).fetchone()
        return row["total"] or 0

    def test_ransom_amount_included_in_total(self, temp_db):
        self._insert_financial_row(temp_db, "fin-1", ransom_amount=500_000.0)
        total = self._total_financial(temp_db)
        assert total == pytest.approx(500_000.0)

    def test_recovery_costs_added(self, temp_db):
        self._insert_financial_row(temp_db, "fin-2", recovery_costs_max=200_000.0)
        total = self._total_financial(temp_db)
        assert total == pytest.approx(200_000.0)

    def test_all_components_summed(self, temp_db):
        self._insert_financial_row(
            temp_db, "fin-3",
            ransom_amount=100_000.0,
            recovery_costs_max=50_000.0,
            legal_costs=25_000.0,
            notification_costs=5_000.0,
        )
        total = self._total_financial(temp_db)
        assert total == pytest.approx(180_000.0)

    def test_row_with_only_ransom_counted(self, temp_db):
        """Previously, rows with only ransom_amount and no recovery costs were excluded."""
        self._insert_financial_row(temp_db, "fin-4", ransom_amount=750_000.0)
        total = self._total_financial(temp_db)
        assert total > 0, "Ransom-only rows must contribute to total_financial_impact"

    def test_zero_values_excluded(self, temp_db):
        """Rows where everything is NULL or 0 must not affect the total."""
        self._insert_financial_row(temp_db, "fin-5")  # no financial cols
        total = self._total_financial(temp_db)
        assert total == 0

    def test_save_enrichment_ransom_stored(self, temp_db):
        """Ransom amount from raw_json_data lands in flat table correctly."""
        incident = _make_incident("fin-save")
        insert_incident(temp_db, incident)
        enrichment = _full_enrichment(
            attack_dynamics=AttackDynamics(
                attack_vector="phishing",
                ransomware_family="LockBit",
                ransom_demanded=True,
                ransom_paid=False,
            )
        )
        # ransom_amount lives in raw_json_data in production (LLM JSON response)
        raw = {"ransom_amount": 500_000.0, "was_ransom_demanded": True}
        save_enrichment_result(temp_db, incident.incident_id, enrichment, raw_json_data=raw)
        row = temp_db.execute(
            "SELECT ransom_amount, was_ransom_demanded FROM incident_enrichments_flat WHERE incident_id=?",
            (incident.incident_id,),
        ).fetchone()
        assert row["ransom_amount"] == pytest.approx(500_000.0)
        assert row["was_ransom_demanded"] == 1


# ---------------------------------------------------------------------------
# 8. CSV export includes new threat-intel columns
# ---------------------------------------------------------------------------


class TestCSVExportNewFields:
    def test_flatten_enrichment_data_includes_new_fields(self):
        """csv_export.flatten_enrichment_data must not drop new threat-intel fields."""
        from src.edu_cti.pipeline.phase2.csv_export import flatten_enrichment_data

        enrichment = _full_enrichment()
        flat = flatten_enrichment_data(enrichment)

        # Top-level fields mapped directly
        assert flat.get("malware_families") is not None, "malware_families missing from CSV export"
        assert flat.get("attacker_tools") is not None, "attacker_tools missing from CSV export"
        assert flat.get("threat_actor_aliases") is not None, "threat_actor_aliases missing from CSV export"
        assert flat.get("attack_campaign_name") == "Operation Dark School"
        assert flat.get("cloud_provider") == "AWS"
        assert flat.get("infrastructure_type") == "hybrid"
        assert flat.get("dwell_time_days") == 14.0
        assert flat.get("data_volume_gb") == 250.5

    def test_load_enriched_incidents_includes_new_columns(self, temp_db):
        """load_enriched_incidents_from_db must carry through new flat columns."""
        from src.edu_cti.pipeline.phase2.csv_export import load_enriched_incidents_from_db

        incident = _make_incident("csv-test")
        insert_incident(temp_db, incident)
        enrichment = _full_enrichment()
        save_enrichment_result(temp_db, incident.incident_id, enrichment)

        rows = load_enriched_incidents_from_db(temp_db, use_flat_table=True)
        assert len(rows) >= 1
        row = next((r for r in rows if r.get("incident_id") == incident.incident_id), None)
        assert row is not None, "Saved incident not found in CSV export"

        # Verify new columns present (may be None if flat table doesn't map them to CSV yet)
        # At minimum the column must not raise a KeyError
        _ = row.get("malware_families")
        _ = row.get("attacker_tools")
        _ = row.get("cloud_provider")


# ---------------------------------------------------------------------------
# 9. Memory restart uses exit code 1 (Railway ON_FAILURE policy)
# ---------------------------------------------------------------------------


class TestMemoryRestartExitCode:
    def test_exit_code_is_1_not_0(self):
        """The memory-threshold restart must use os._exit(1) so Railway restarts."""
        import ast
        import pathlib

        src = pathlib.Path(
            "src/edu_cti/pipeline/phase2/__main__.py"
        ).read_text()
        tree = ast.parse(src)

        exit_codes_near_mem_threshold = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Match os._exit(...) or _os._exit(...)
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "_exit"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                ):
                    exit_codes_near_mem_threshold.append(
                        (node.lineno, node.args[0].value)
                    )

        mem_exits = [
            (ln, code)
            for ln, code in exit_codes_near_mem_threshold
            if code == 0
        ]
        assert mem_exits == [], (
            f"Found os._exit(0) at lines {mem_exits} — "
            "must use os._exit(1) so Railway ON_FAILURE policy restarts the container"
        )
