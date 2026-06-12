"""Clean dataset export from the star-schema analytical layer.

Every dataset is a flat, already-normalized join over the dimension/fact/bridge
tables, so the output opens in any tool with no preprocessing: categorical values
are single canonical slugs (no mixed casing), and multi-valued CTI is either a
joined long table (one row per incident-technique, incident-CVE, IOC) or a
delimited column on the incident table.

Datasets:
  incidents  one row per open canonical incident with normalized dimension labels
  iocs       one row per (incident, IOC)
  mitre      one row per (incident, MITRE technique)
  cves       one row per (incident, CVE)
  campaigns  one row per campaign with member counts and actor/vendor/CVE lists

Used by the CLI ``eduthreat-v2-export`` and by the ``/api/v2/export`` routes.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.orm import Session

DATASETS = ("incidents", "iocs", "mitre", "cves", "campaigns")

# Each dataset is a single SQL statement over the star schema. Keeping them as
# explicit SQL makes the exported column contract obvious and stable.
_QUERIES: dict[str, str] = {
    "incidents": """
        SELECT
          f.canonical_incident_id::text          AS incident_id,
          f.institution_name,
          dit.slug                               AS institution_type,
          f.vendor_name,
          dc.country_code,
          dc.name                                AS country,
          f.region, f.city,
          dac.slug                               AS attack_category,
          dac.family                             AS attack_family,
          dav.slug                               AS attack_vector,
          dsev.slug                              AS severity,
          dta.label                              AS threat_actor,
          drf.label                              AS ransomware_family,
          f.incident_date, f.detection_date, f.disclosure_date,
          f.incident_year, f.incident_quarter,
          f.dwell_time_days, f.disclosure_lag_days, f.recovery_days, f.downtime_days,
          f.records_affected_exact, f.records_affected_min, f.records_affected_max,
          f.ransom_demanded_usd, f.ransom_paid_usd,
          f.data_exfiltrated, f.data_encrypted, f.is_vendor_breach,
          f.teaching_disrupted, f.research_disrupted,
          f.attribution_confidence, f.source_reliability,
          f.enrichment_confidence, f.completeness_score, f.source_count,
          (SELECT string_agg(dmt2.technique_id, '|' ORDER BY dmt2.technique_id)
             FROM bridge_incident_mitre_technique bmt2
             JOIN dim_mitre_technique dmt2 ON dmt2.technique_id = bmt2.technique_id
            WHERE bmt2.canonical_incident_id = f.canonical_incident_id) AS mitre_techniques,
          (SELECT string_agg(ddc.slug, '|' ORDER BY ddc.slug)
             FROM bridge_incident_data_category bdc
             JOIN dim_data_category ddc ON ddc.id = bdc.data_category_id
            WHERE bdc.canonical_incident_id = f.canonical_incident_id) AS data_categories,
          (SELECT string_agg(bcv.cve_id, '|' ORDER BY bcv.cve_id)
             FROM bridge_incident_cve bcv
            WHERE bcv.canonical_incident_id = f.canonical_incident_id) AS cves
        FROM fact_incident f
        LEFT JOIN dim_institution_type dit ON dit.id = f.institution_type_id
        LEFT JOIN dim_attack_category dac ON dac.id = f.attack_category_id
        LEFT JOIN dim_attack_vector dav ON dav.id = f.attack_vector_id
        LEFT JOIN dim_severity dsev ON dsev.id = f.severity_id
        LEFT JOIN dim_threat_actor dta ON dta.id = f.primary_actor_id
        LEFT JOIN dim_ransomware_family drf ON drf.id = f.ransomware_family_id
        LEFT JOIN dim_country dc ON dc.country_code = f.country_code
        ORDER BY f.incident_date NULLS LAST, incident_id
    """,
    "iocs": """
        SELECT i.canonical_incident_id::text AS incident_id, f.institution_name,
               i.ioc_type, i.value, i.confidence
        FROM incident_ioc i
        LEFT JOIN fact_incident f ON f.canonical_incident_id = i.canonical_incident_id
        ORDER BY incident_id, i.ioc_type, i.value
    """,
    "mitre": """
        SELECT b.canonical_incident_id::text AS incident_id, f.institution_name,
               b.technique_id, dmt.name AS technique_name, b.tactic_slug AS tactic
        FROM bridge_incident_mitre_technique b
        JOIN dim_mitre_technique dmt ON dmt.technique_id = b.technique_id
        LEFT JOIN fact_incident f ON f.canonical_incident_id = b.canonical_incident_id
        ORDER BY incident_id, b.technique_id
    """,
    "cves": """
        SELECT b.canonical_incident_id::text AS incident_id, f.institution_name,
               b.cve_id, dcve.year AS cve_year
        FROM bridge_incident_cve b
        JOIN dim_cve dcve ON dcve.cve_id = b.cve_id
        LEFT JOIN fact_incident f ON f.canonical_incident_id = b.canonical_incident_id
        ORDER BY incident_id, b.cve_id
    """,
    "campaigns": """
        SELECT id AS campaign_id, campaign_name, campaign_type, status, confidence,
               first_seen_date, last_seen_date, member_count, confirmed_member_count,
               actors::text AS actors, vendors::text AS vendors, cves::text AS cves,
               attack_categories::text AS attack_categories
        FROM campaigns
        WHERE status <> 'suppressed'
        ORDER BY confidence DESC NULLS LAST, member_count DESC
    """,
}


def fetch_rows(session: Session, dataset: str) -> list[dict[str, Any]]:
    if dataset not in _QUERIES:
        raise ValueError(f"unknown dataset {dataset!r}; choose from {', '.join(DATASETS)}")
    result = session.execute(text(_QUERIES[dataset]))
    columns = list(result.keys())
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _jsonable(value: Any) -> Any:
    """Coerce a DB value to something JSON/CSV can serialise.

    The star-schema queries return Postgres-native types — dates, UUIDs, Decimals,
    occasionally bytes/sets — that ``json.dumps`` cannot serialise on its own. CSV
    tolerated them (the writer stringifies everything) but the JSON export 500'd.
    Handle the common types explicitly and let ``to_json``'s ``default=str`` catch
    anything else.
    """
    import decimal
    import uuid

    if value is None:
        return None
    if hasattr(value, "isoformat"):  # date / datetime / time
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, decimal.Decimal):
        # int if it's whole, else float — keeps numbers numeric in JSON
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, (set, frozenset)):
        return sorted(str(v) for v in value)
    return value


def to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow({k: _jsonable(v) for k, v in row.items()})
    return buf.getvalue()


def to_json(rows: list[dict[str, Any]]) -> str:
    # default=str is a belt-and-suspenders fallback for any DB type _jsonable
    # doesn't explicitly handle, so the export can never 500 on serialisation.
    return json.dumps(
        [{k: _jsonable(v) for k, v in r.items()} for r in rows], indent=2, default=str
    )


def export_dataset(session: Session, dataset: str, fmt: str = "csv") -> str:
    rows = fetch_rows(session, dataset)
    if fmt == "csv":
        return to_csv(rows)
    if fmt == "json":
        return to_json(rows)
    raise ValueError(f"unknown format {fmt!r}; choose csv or json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export the EduThreat dataset from the star schema")
    parser.add_argument("dataset", choices=DATASETS)
    parser.add_argument("--format", choices=("csv", "json"), default="csv")
    parser.add_argument("--out", help="output file (default stdout)")
    args = parser.parse_args()

    from src.edu_cti_v2.db import create_session_factory
    from src.edu_cti_v2.db.config import V2DatabaseSettings

    settings = V2DatabaseSettings.from_env()
    session_factory = create_session_factory(settings)
    with session_factory() as session:
        payload = export_dataset(session, args.dataset, args.format)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as handle:
            handle.write(payload)
        print(f"wrote {args.dataset}.{args.format} -> {args.out}")
    else:
        print(payload)


if __name__ == "__main__":
    main()
