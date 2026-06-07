"""Normalized analytics breakdowns served from the star-schema layer.

These replace the read-time `Counter` aggregation over mixed-case canonical
columns with plain SQL ``GROUP BY`` over the controlled-vocabulary dimensions, so
the values returned are already normalized (one row per concept, no casing
duplicates). The return shape matches the legacy
``/analytics/breakdowns`` payload so the frontend can consume it without a
client-side merge.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

# Each breakdown is a GROUP BY over a dimension joined to fact_incident, already
# normalized. The per-row key matches the legacy breakdown payload (country /
# attack_category / institution_type / severity) so this is a drop-in for the
# frontend. country joins the ISO dimension for code + display name.
_BREAKDOWNS = {
    "countries": ("country", """
        SELECT dc.country_code AS country, dc.country_code, dc.name AS label,
               count(*) AS incident_count
        FROM fact_incident f
        JOIN dim_country dc ON dc.country_code = f.country_code
        GROUP BY dc.country_code, dc.name
        ORDER BY incident_count DESC NULLS LAST
        LIMIT :limit
    """),
    "attack_categories": ("attack_category", """
        SELECT dac.slug AS attack_category, count(*) AS incident_count
        FROM fact_incident f
        JOIN dim_attack_category dac ON dac.id = f.attack_category_id
        GROUP BY dac.slug
        ORDER BY incident_count DESC
        LIMIT :limit
    """),
    "institution_types": ("institution_type", """
        SELECT dit.slug AS institution_type, count(*) AS incident_count
        FROM fact_incident f
        JOIN dim_institution_type dit ON dit.id = f.institution_type_id
        GROUP BY dit.slug
        ORDER BY incident_count DESC
        LIMIT :limit
    """),
    "severities": ("severity", """
        SELECT dsev.slug AS severity, count(*) AS incident_count
        FROM fact_incident f
        JOIN dim_severity dsev ON dsev.id = f.severity_id
        GROUP BY dsev.slug
        ORDER BY incident_count DESC
    """),
}


def star_layer_ready(session: Session) -> bool:
    """Whether the star layer has been backfilled (so it can serve reads)."""
    try:
        return bool(session.execute(text("SELECT 1 FROM fact_incident LIMIT 1")).first())
    except Exception:
        return False


def get_star_breakdowns(session: Session, *, breakdown_limit: int = 20) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for facet_key, (value_key, sql) in _BREAKDOWNS.items():
        params: dict[str, Any] = {}
        if ":limit" in sql:
            params["limit"] = breakdown_limit
        rows = session.execute(text(sql), params).mappings().all()
        out[facet_key] = [
            {value_key: r[value_key], "incident_count": int(r["incident_count"]),
             **({"country_code": r["country_code"]} if "country_code" in r else {})}
            for r in rows
        ]
    return out
