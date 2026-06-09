"""Read-side helpers for Postgres-backed canonical incidents."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
import re
from typing import Any, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import (
    ArticleFetchAttempt,
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    CanonicalTimelineEvent,
    SourceIncident,
)
from src.edu_cti_v2.repositories import AnalyticsRefreshRepository, ArticleRepository, CanonicalIncidentRepository

_DASHBOARD_COUNTRY_LIMIT = 500


def _serialize_membership(
    membership: CanonicalMembership,
    *,
    source_details: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    payload = {
        "source_incident_id": str(membership.source_incident_id),
        "match_type": membership.match_type,
        "match_score": float(membership.match_score or 0.0),
        "survivor_score": float(membership.survivor_score or 0.0),
        "is_primary_member": bool(membership.is_primary_member),
        "field_contribution": membership.field_contribution or {},
        "matcher_version": membership.matcher_version,
        "matched_at": membership.matched_at.isoformat() if membership.matched_at else None,
    }
    if source_details:
        payload.update(
            {
                "source_name": source_details.get("source_name"),
                "source_group": source_details.get("source_group"),
                "collected_at": source_details.get("collected_at"),
                "source_published_at": source_details.get("source_published_at"),
                "raw_title": source_details.get("raw_title"),
                "raw_subtitle": source_details.get("raw_subtitle"),
                "raw_victim_name": source_details.get("raw_victim_name"),
                "raw_institution_name": source_details.get("raw_institution_name"),
                "raw_institution_type": source_details.get("raw_institution_type"),
                "raw_country": source_details.get("raw_country"),
                "raw_region": source_details.get("raw_region"),
                "raw_city": source_details.get("raw_city"),
                "source_urls": source_details.get("source_urls") or [],
            }
        )
    return payload


def _serialize_timeline_event(event: CanonicalTimelineEvent) -> dict[str, Any]:
    return {
        "seq_order": event.seq_order,
        "event_date": event.event_date.isoformat() if event.event_date else None,
        "date_precision": event.date_precision,
        "event_type": event.event_type,
        "event_description": event.event_description,
        "actor_attribution": event.actor_attribution,
        "source_enrichment_id": str(event.source_enrichment_id) if event.source_enrichment_id else None,
    }


def _serialize_fetch_attempt(attempt: ArticleFetchAttempt) -> dict[str, Any]:
    return {
        "fetch_tier": attempt.fetch_tier,
        "attempted_at": attempt.attempted_at.isoformat() if attempt.attempted_at else None,
        "worker_id": attempt.worker_id,
        "success": bool(attempt.success),
        "http_status": attempt.http_status,
        "latency_ms": attempt.latency_ms,
        "content_length": attempt.content_length,
        "error_code": attempt.error_code,
        "error_message": attempt.error_message,
        "response_metadata": attempt.response_metadata or {},
    }


def _value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _extract_field_provenance_map(raw_provenance: dict[str, Any] | None) -> dict[str, Any]:
    payload = raw_provenance or {}
    nested = payload.get("field_sources")
    if isinstance(nested, dict):
        return nested
    return payload if isinstance(payload, dict) else {}


def _extract_source_disclosure_document(raw_provenance: dict[str, Any] | None) -> dict[str, Any]:
    payload = raw_provenance or {}
    disclosure = payload.get("source_disclosure")
    return disclosure if isinstance(disclosure, dict) else {}


def _display_disclosure_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return f"{value}"
    if isinstance(value, list):
        parts = [_display_disclosure_value(item) for item in value]
        return ", ".join(part for part in parts if part)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    text = str(value).strip()
    return text or None


def _disclosure_fingerprint(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


_SCORE_BREAKDOWN_LABELS = {
    "source_rank": "Source Trust",
    "structured_field_coverage": "Field Coverage",
    "summary_richness": "Summary Depth",
    "timeline_depth": "Timeline Depth",
    "named_victim_bonus": "Named Victim",
    "incident_date_bonus": "Incident Date",
    "actor_or_family_bonus": "Actor/Family",
    "country_bonus": "Country",
    "identity_title_alignment_bonus": "Title Match",
    "identity_title_alignment_penalty": "Title Mismatch",
    "enrichment_confidence_bonus": "LLM Confidence",
}


def _build_selected_source_reasons(selected_source_summary: dict[str, Any], source_count: int) -> list[str]:
    reasons: list[str] = []
    survivor_score = selected_source_summary.get("survivor_score")
    if survivor_score is not None:
        reasons.append(f"Highest survivor score across {source_count} supporting source(s)")

    breakdown = selected_source_summary.get("score_breakdown") or {}
    ordered_positive = [
        (key, int(value))
        for key, value in breakdown.items()
        if isinstance(value, (int, float)) and value > 0
    ]
    ordered_positive.sort(key=lambda item: item[1], reverse=True)
    for key, _value in ordered_positive[:3]:
        label = _SCORE_BREAKDOWN_LABELS.get(key)
        if label:
            reasons.append(label)

    if not reasons:
        reasons.append("Selected as the strongest supporting source after canonical scoring")
    return reasons


def _extract_resolved_disclosure_values(
    canonical: CanonicalIncident,
    projection: dict[str, Any],
) -> dict[str, Any]:
    attack_dynamics = _projection_section(projection, "attack_dynamics")
    data_impact = _projection_section(projection, "data_impact")
    system_impact = _projection_section(projection, "system_impact")
    user_impact = _projection_section(projection, "user_impact")
    financial_impact = _projection_section(projection, "financial_impact")
    recovery_metrics = _projection_section(projection, "recovery_metrics")
    transparency_metrics = _projection_section(projection, "transparency_metrics")
    values = {
        "institution_name": canonical.institution_name,
        "institution_type": canonical.institution_type,
        "vendor_name": canonical.vendor_name,
        "country": canonical.country,
        "region": canonical.region,
        "city": canonical.city,
        "incident_date": canonical.incident_date.isoformat() if canonical.incident_date else None,
        "date_precision": canonical.date_precision,
        "attack_category": canonical.attack_category,
        "attack_vector": canonical.attack_vector or attack_dynamics.get("attack_vector"),
        "threat_actor_name": canonical.threat_actor_name,
        "ransomware_family": canonical.ransomware_family or attack_dynamics.get("ransomware_family"),
        "severity": canonical.severity,
        "canonical_summary": canonical.canonical_summary,
        "records_affected_exact": data_impact.get("records_affected_exact"),
        "records_affected_min": data_impact.get("records_affected_min"),
        "records_affected_max": data_impact.get("records_affected_max"),
        "data_categories": data_impact.get("data_categories") or data_impact.get("data_types_affected"),
        "data_exfiltrated": data_impact.get("data_exfiltrated"),
        "data_breached": projection.get("data_breached"),
        "systems_affected": system_impact.get("systems_affected"),
        "critical_systems_affected": system_impact.get("critical_systems_affected"),
        "third_party_vendor_impact": system_impact.get("third_party_vendor_impact"),
        "vendor_name_detail": system_impact.get("vendor_name") or canonical.vendor_name,
        "total_individuals_affected": user_impact.get("total_individuals_affected"),
        "ransom_amount_exact": financial_impact.get("ransom_amount_exact"),
        "public_disclosure": transparency_metrics.get("public_disclosure"),
        "public_disclosure_date": transparency_metrics.get("public_disclosure_date"),
        "recovery_duration_days": recovery_metrics.get("recovery_duration_days"),
    }
    return {key: value for key, value in values.items() if _value_present(value)}


def _summary_from_canonical(
    canonical: CanonicalIncident,
    enrichment: Optional[CanonicalEnrichment],
    *,
    membership_count: int,
) -> dict[str, Any]:
    analytics_projection = (getattr(enrichment, "analytics_projection", None) if enrichment else None) or {}
    display_name = canonical.institution_name or canonical.vendor_name
    return {
        "canonical_incident_id": str(canonical.id),
        "display_name": display_name,
        "institution_name": canonical.institution_name,
        "vendor_name": canonical.vendor_name,
        "institution_type": canonical.institution_type,
        "country": canonical.country,
        "country_code": canonical.country_code,
        "region": canonical.region,
        "city": canonical.city,
        "incident_date": canonical.incident_date.isoformat() if canonical.incident_date else None,
        "date_precision": canonical.date_precision,
        "attack_category": canonical.attack_category,
        "attack_vector": canonical.attack_vector,
        "threat_actor_name": canonical.threat_actor_name,
        "ransomware_family": canonical.ransomware_family,
        "is_education_related": canonical.is_education_related,
        "severity": canonical.severity,
        "canonical_summary": canonical.canonical_summary,
        "status": canonical.status,
        "membership_count": membership_count,
        "selected_source_enrichment_id": (
            str(enrichment.selected_source_enrichment_id)
            if enrichment and enrichment.selected_source_enrichment_id
            else None
        ),
        "analytics_projection": analytics_projection,
        "first_seen_at": canonical.first_seen_at.isoformat() if canonical.first_seen_at else None,
        "last_seen_at": canonical.last_seen_at.isoformat() if canonical.last_seen_at else None,
        "updated_at": canonical.updated_at.isoformat() if canonical.updated_at else None,
    }


def _to_count_by_category(
    items: list[dict[str, Any]],
    *,
    label_key: str,
    count_key: str = "incident_count",
    country_code_key: Optional[str] = None,
) -> list[dict[str, Any]]:
    total = sum(int(item.get(count_key) or 0) for item in items) or 0
    results: list[dict[str, Any]] = []
    for item in items:
        count = int(item.get(count_key) or 0)
        label = item.get(label_key)
        if label is None:
            continue
        payload = {
            "category": label,
            "count": count,
            "percentage": (count / total * 100.0) if total else 0.0,
        }
        if country_code_key:
            payload["country_code"] = item.get(country_code_key)
            # Pass region + flag through when present so the map/dashboard can use
            # the authoritative, normalized values directly (no name guessing).
            if "region" in item:
                payload["region"] = item.get("region")
            if "flag_emoji" in item:
                payload["flag_emoji"] = item.get("flag_emoji")
        results.append(payload)
    return results


def _to_time_series(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "date": str(item.get("bucket_start")),
            "count": int(item.get("incident_count") or 0),
        }
        for item in items
        if item.get("bucket_start") is not None
    ]


def _to_recent_incidents(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "incident_id": item["canonical_incident_id"],
            "institution_name": item.get("display_name") or item.get("institution_name") or "Unknown",
            "country": item.get("country"),
            "attack_category": item.get("attack_category"),
            "ransomware_family": item.get("ransomware_family"),
            "incident_date": item.get("incident_date"),
            "title": item.get("canonical_summary"),
            "enriched_summary": item.get("canonical_summary"),
            "threat_actor_name": item.get("threat_actor_name"),
        }
        for item in items
    ]


def _humanize_slug(value: str | None) -> str:
    if not value:
        return "Unknown"
    return value.replace("_", " ").replace("/", " / ").title()


def _projection_section(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, dict) else {}


def _coerce_int(value: Any) -> int | None:
    if value in (None, "", False):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


# Canonical ATT&CK enterprise tactics, keyed by their normalized (lowercase,
# separator-stripped) form so that variant spellings the LLM may emit
# ("defense_evasion", "defense-evasion", "Defense Evasion", the TA-id) all map to
# one canonical column. Without this, variant spellings of the same tactic create
# duplicate columns in the MITRE matrix.
_ATTACK_TACTICS: dict[str, str] = {
    "reconnaissance": "Reconnaissance",
    "resourcedevelopment": "Resource Development",
    "initialaccess": "Initial Access",
    "execution": "Execution",
    "persistence": "Persistence",
    "privilegeescalation": "Privilege Escalation",
    "defenseevasion": "Defense Evasion",
    "credentialaccess": "Credential Access",
    "discovery": "Discovery",
    "lateralmovement": "Lateral Movement",
    "collection": "Collection",
    "commandandcontrol": "Command and Control",
    "exfiltration": "Exfiltration",
    "impact": "Impact",
}
# TA-id aliases → canonical name.
_ATTACK_TACTIC_IDS: dict[str, str] = {
    "ta0043": "Reconnaissance", "ta0042": "Resource Development", "ta0001": "Initial Access",
    "ta0002": "Execution", "ta0003": "Persistence", "ta0004": "Privilege Escalation",
    "ta0005": "Defense Evasion", "ta0006": "Credential Access", "ta0007": "Discovery",
    "ta0008": "Lateral Movement", "ta0009": "Collection", "ta0011": "Command and Control",
    "ta0010": "Exfiltration", "ta0040": "Impact",
}


def _canonical_tactic(value: str | None) -> str:
    """Map any spelling/casing/separator/TA-id variant of an ATT&CK tactic to a
    single canonical display name, so the matrix shows one column per tactic."""
    if not value:
        return "Unknown"
    norm = re.sub(r"[^a-z0-9]+", "", str(value).lower())
    if norm in _ATTACK_TACTIC_IDS:
        return _ATTACK_TACTIC_IDS[norm]
    if norm in _ATTACK_TACTICS:
        return _ATTACK_TACTICS[norm]
    # "command&control" / "c2" style and unknowns fall back to humanized text.
    if norm in {"c2", "commandcontrol"}:
        return "Command and Control"
    return _humanize_slug(value)


def _normalize_institution_segment(institution_type: str | None, vendor_name: str | None) -> str:
    if vendor_name:
        return "Education Vendor / Provider"

    raw = (institution_type or "").strip().lower()
    if not raw:
        return "Other Education"

    if any(token in raw for token in ("vendor", "provider", "technology_provider", "service_provider")):
        return "Education Vendor / Provider"
    if "school_district" in raw or "k12" in raw or raw == "school":
        return "K-12"
    if any(token in raw for token in ("university", "college", "higher_education", "community_college")):
        return "Higher Education"
    if any(token in raw for token in ("hospital", "medical", "research")):
        return "Academic Medical / Research"
    return "Other Education"


def _normalize_attack_cluster(attack_category: str | None) -> str:
    raw = (attack_category or "").strip().lower()
    if not raw:
        return "Unspecified"
    if raw.startswith("ransomware"):
        return "Ransomware & Extortion"
    if raw.startswith("data_breach"):
        return "Data Breach & Exposure"
    if raw in {"third_party_compromise", "supply_chain_software"}:
        return "Third-Party & Supply Chain"
    if raw == "unauthorized_access":
        return "Unauthorized Access"
    if raw.startswith("ddos"):
        return "Service Disruption"
    if raw == "web_defacement":
        return "Website Defacement"
    return _humanize_slug(raw)


def _normalize_attack_vector(attack_vector: str | None) -> str | None:
    raw = (attack_vector or "").strip().lower()
    if not raw or raw in {"unknown", "other", "n/a"}:
        return None

    labels = {
        "phishing_email": "Phishing Email",
        "third_party_vendor": "Third-Party Vendor",
        "stolen_credentials": "Stolen Credentials",
        "supply_chain_compromise": "Supply Chain Compromise",
        "vulnerability_exploit_known": "Known Vulnerability Exploit",
        "vulnerability_exploit_zero_day": "Zero-Day Exploit",
        "exposed_service": "Exposed Service",
        "misconfiguration": "Misconfiguration",
        "malicious_link": "Malicious Link",
        "ddos": "DDoS",
    }
    return labels.get(raw, _humanize_slug(raw))


def _to_ranked_items(counter: Counter[str], *, total: int, label_key: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for label, count in counter.most_common():
        if not label:
            continue
        results.append(
            {
                label_key: label,
                "count": int(count),
                "percentage": (count / total * 100.0) if total else 0.0,
            }
        )
    return results


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if item not in (None, "", [])]
    return [value]


def _dedupe_preserve_order(values: list[Any]) -> list[Any]:
    seen: set[Any] = set()
    result: list[Any] = []
    for value in values:
        marker = value if isinstance(value, (str, int, float, bool, type(None))) else repr(value)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(value)
    return result


def _diamond_confidence(score: int) -> str:
    if score >= 3:
        return "high"
    if score == 2:
        return "medium"
    if score == 1:
        return "low"
    return "none"


def _build_diamond_projection(
    canonical: CanonicalIncident,
    enrichment: Optional[CanonicalEnrichment],
    *,
    selected_source: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    projection = (getattr(enrichment, "canonical_projection", None) if enrichment else None) or {}
    attack_dynamics = _projection_section(projection, "attack_dynamics")
    system_impact = _projection_section(projection, "system_impact")

    display_name = canonical.institution_name or canonical.vendor_name
    attack_vector = canonical.attack_vector or attack_dynamics.get("attack_vector")
    attack_chain = _as_list(attack_dynamics.get("attack_chain"))
    mitre_techniques = _as_list(projection.get("mitre_attack_techniques"))
    vulnerabilities = _as_list(projection.get("vulnerabilities_exploited"))
    malware_families = _as_list(projection.get("malware_families"))
    attacker_tools = _as_list(projection.get("attacker_tools"))

    adversary_name = projection.get("threat_actor") or canonical.threat_actor_name
    adversary_score = sum(
        1
        for value in (
            adversary_name,
            projection.get("threat_actor_category"),
            projection.get("threat_actor_claim_url") or projection.get("leak_site_url"),
        )
        if value
    )
    capability_score = sum(
        1
        for value in (
            canonical.attack_category,
            attack_vector,
            canonical.ransomware_family,
            attack_chain or None,
            mitre_techniques or None,
            vulnerabilities or None,
            malware_families or None,
            attacker_tools or None,
            projection.get("initial_access_description"),
        )
        if value
    )

    infrastructure_components: list[str] = []
    claim_url = projection.get("threat_actor_claim_url")
    leak_site_url = projection.get("leak_site_url")
    if claim_url:
        infrastructure_components.append("actor_claim_site")
    if leak_site_url:
        infrastructure_components.append("leak_site")
    if projection.get("dark_web_posting_confirmed"):
        infrastructure_components.append("dark_web_posting")
    if (
        system_impact.get("third_party_vendor_impact")
        or projection.get("third_party_vendor_impact")
        or (attack_vector or "") == "third_party_vendor"
        or (canonical.attack_category or "") in {"third_party_compromise", "supply_chain_software"}
    ):
        infrastructure_components.append("third_party_platform")
    if vulnerabilities:
        infrastructure_components.append("exploited_public_vulnerability")
    infrastructure_components = _dedupe_preserve_order(infrastructure_components)
    infrastructure_score = sum(
        1
        for value in (
            claim_url or leak_site_url,
            system_impact.get("vendor_name") or canonical.vendor_name,
            infrastructure_components or None,
        )
        if value
    )

    victim_score = sum(
        1
        for value in (
            display_name,
            canonical.country or canonical.country_code,
            canonical.incident_date,
        )
        if value
    )
    victim_scope = "direct_victim"
    if canonical.vendor_name and not canonical.institution_name:
        victim_scope = "vendor_victim"
    elif (
        system_impact.get("third_party_vendor_impact")
        or projection.get("third_party_vendor_impact")
        or (attack_vector or "") == "third_party_vendor"
        or canonical.vendor_name
    ):
        victim_scope = "institution_via_vendor"

    source_article_url = None
    if selected_source:
        source_article_url = (
            selected_source.get("article_resolved_url")
            or selected_source.get("article_url")
        )

    present_vertices = {
        "victim": bool(display_name),
        "adversary": bool(adversary_name),
        "capability": bool(canonical.attack_category or attack_vector or canonical.ransomware_family),
        "infrastructure": bool(infrastructure_components or claim_url or leak_site_url),
    }

    return {
        "model_version": "diamond_v1",
        "event_meta": {
            "event_id": str(canonical.id),
            "event_date": canonical.incident_date.isoformat() if canonical.incident_date else None,
            "date_precision": canonical.date_precision,
            "source_article_url": source_article_url,
            "all_core_vertices_present": all(present_vertices.values()),
            "present_vertex_count": sum(1 for value in present_vertices.values() if value),
        },
        "victim": {
            "name": display_name,
            "institution_name": canonical.institution_name,
            "vendor_name": canonical.vendor_name,
            "institution_type": canonical.institution_type,
            "country": canonical.country,
            "country_code": canonical.country_code,
            "region": canonical.region,
            "city": canonical.city,
            "is_education_related": canonical.is_education_related,
            "scope": victim_scope,
            "present": present_vertices["victim"],
            "confidence": _diamond_confidence(victim_score),
            "evidence_fields": [
                field
                for field, value in (
                    ("institution_name", canonical.institution_name),
                    ("vendor_name", canonical.vendor_name),
                    ("country", canonical.country or canonical.country_code),
                    ("incident_date", canonical.incident_date),
                )
                if value
            ],
        },
        "adversary": {
            "name": adversary_name,
            "category": projection.get("threat_actor_category"),
            "motivation": projection.get("threat_actor_motivation"),
            "origin_country": projection.get("threat_actor_origin_country"),
            "claim_url": claim_url,
            "present": present_vertices["adversary"],
            "confidence": _diamond_confidence(adversary_score),
            "evidence_fields": [
                field
                for field, value in (
                    ("threat_actor_name", adversary_name),
                    ("threat_actor_category", projection.get("threat_actor_category")),
                    ("threat_actor_motivation", projection.get("threat_actor_motivation")),
                    ("threat_actor_origin_country", projection.get("threat_actor_origin_country")),
                    ("threat_actor_claim_url", claim_url),
                )
                if value
            ],
        },
        "capability": {
            "attack_category": canonical.attack_category,
            "attack_vector": attack_vector,
            "ransomware_family": canonical.ransomware_family,
            "attack_chain": attack_chain,
            "mitre_attack_techniques": mitre_techniques,
            "initial_access_description": projection.get("initial_access_description"),
            "vulnerabilities_exploited": vulnerabilities,
            "malware_families": malware_families,
            "attacker_tools": attacker_tools,
            "present": present_vertices["capability"],
            "confidence": _diamond_confidence(capability_score),
            "evidence_fields": [
                field
                for field, value in (
                    ("attack_category", canonical.attack_category),
                    ("attack_vector", attack_vector),
                    ("ransomware_family", canonical.ransomware_family),
                    ("attack_chain", attack_chain or None),
                    ("mitre_attack_techniques", mitre_techniques or None),
                    ("vulnerabilities_exploited", vulnerabilities or None),
                    ("malware_families", malware_families or None),
                    ("attacker_tools", attacker_tools or None),
                    ("initial_access_description", projection.get("initial_access_description")),
                )
                if value
            ],
        },
        "infrastructure": {
            "components": infrastructure_components,
            "leak_site_url": leak_site_url,
            "claim_url": claim_url,
            "service_provider": system_impact.get("vendor_name") or canonical.vendor_name,
            "third_party_vendor_impact": (
                system_impact.get("third_party_vendor_impact")
                if system_impact.get("third_party_vendor_impact") is not None
                else projection.get("third_party_vendor_impact")
            ),
            "dark_web_posting_confirmed": projection.get("dark_web_posting_confirmed"),
            "present": present_vertices["infrastructure"],
            "confidence": _diamond_confidence(infrastructure_score),
            "evidence_fields": [
                field
                for field, value in (
                    ("leak_site_url", leak_site_url),
                    ("threat_actor_claim_url", claim_url),
                    ("vendor_name", system_impact.get("vendor_name") or canonical.vendor_name),
                    ("third_party_vendor_impact", system_impact.get("third_party_vendor_impact")),
                    ("dark_web_posting_confirmed", projection.get("dark_web_posting_confirmed")),
                    ("vulnerabilities_exploited", vulnerabilities or None),
                )
                if value
            ],
        },
    }


def _dashboard_stats_from_rollup(rollup: dict[str, Any], *, refreshed_at: str) -> dict[str, Any]:
    total_incidents = int(rollup.get("canonical_incident_count") or 0)
    enriched_incidents = int(rollup.get("enriched_canonical_count") or 0)
    return {
        "total_incidents": total_incidents,
        "education_incidents": int(rollup.get("education_related_count") or 0),
        "enriched_incidents": enriched_incidents,
        "unenriched_incidents": max(total_incidents - enriched_incidents, 0),
        "incidents_with_ransomware": int(rollup.get("incidents_with_ransomware") or 0),
        "incidents_with_data_breach": int(rollup.get("incidents_with_data_breach") or 0),
        "countries_affected": int(rollup.get("countries_affected") or 0),
        "unique_threat_actors": int(rollup.get("unique_threat_actors") or 0),
        "unique_ransomware_families": int(rollup.get("unique_ransomware_families") or 0),
        "data_sources": 0,
        "avg_recovery_days": None,
        "total_financial_impact": 0,
        "incidents_with_mitre": 0,
        "last_updated": refreshed_at,
    }


def _is_full_dashboard_snapshot(payload: dict[str, Any]) -> bool:
    required = {
        "totals",
        "stats",
        "intelligence_summary",
        "incidents_by_country",
        "incidents_by_attack_type",
        "incidents_by_ransomware",
        "incidents_over_time",
        "recent_incidents",
    }
    if not required.issubset(payload.keys()):
        return False

    # Map consumers need every country bucket, not just the historical top-10
    # dashboard table. Treat older cached snapshots as stale if they report more
    # affected countries than they include in incidents_by_country.
    stats = payload.get("stats") if isinstance(payload.get("stats"), dict) else {}
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    expected_countries = stats.get("countries_affected") or totals.get("countries_affected") or 0
    try:
        expected_country_count = int(expected_countries or 0)
    except (TypeError, ValueError):
        expected_country_count = 0
    country_rows = payload.get("incidents_by_country")
    included_country_count = len(country_rows) if isinstance(country_rows, list) else 0
    return expected_country_count == 0 or included_country_count >= expected_country_count


def _to_legacy_incident_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "incident_id": item["canonical_incident_id"],
        "institution_name": item.get("display_name") or item.get("institution_name") or "Unknown",
        "institution_type": item.get("institution_type"),
        "country": item.get("country"),
        "country_code": item.get("country_code"),
        "region": item.get("region"),
        "city": item.get("city"),
        "incident_date": item.get("incident_date"),
        "date_precision": item.get("date_precision"),
        "title": item.get("canonical_summary"),
        "subtitle": None,
        "enriched_summary": item.get("canonical_summary"),
        "attack_type_hint": item.get("attack_category"),
        "attack_category": item.get("attack_category"),
        "ransomware_family": item.get("ransomware_family"),
        "threat_actor_name": item.get("threat_actor_name"),
        "status": item.get("status") or "open",
        "source_confidence": "medium",
        "llm_enriched": bool(item.get("selected_source_enrichment_id")),
        "llm_enriched_at": item.get("updated_at"),
        "ingested_at": item.get("first_seen_at"),
        "source_count": item.get("membership_count") or 0,
        "sources": [],
    }


def _to_legacy_timeline(timeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "date": item.get("event_date"),
            "date_precision": item.get("date_precision"),
            "event_description": item.get("event_description"),
            "event_type": item.get("event_type"),
            "actor_attribution": item.get("actor_attribution"),
            "indicators": None,
        }
        for item in timeline
    ]


def _to_legacy_sources(memberships: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "source": item.get("source_name"),
            "source_group": item.get("source_group"),
            "source_event_id": item.get("source_incident_id"),
            "first_seen_at": item.get("collected_at"),
            "source_published_at": item.get("source_published_at"),
            "confidence": None,
            "is_primary_member": bool(item.get("is_primary_member")),
            "raw_title": item.get("raw_title"),
            "raw_subtitle": item.get("raw_subtitle"),
            "raw_institution_name": item.get("raw_institution_name"),
            "source_urls": item.get("source_urls") or [],
        }
        for item in memberships
        if item.get("source_name")
    ]


def _is_public_source_url(url_payload: dict[str, Any]) -> bool:
    kind = str(url_payload.get("url_kind") or "other").strip().lower()
    return kind not in {"leak_site", "screenshot"}


def _append_unique_url(urls: list[str], seen: set[str], candidate: Any) -> None:
    value = str(candidate or "").strip()
    if not value or value in seen:
        return
    seen.add(value)
    urls.append(value)


def _collect_urls(
    selected_source: dict[str, Any] | None,
    memberships: Optional[list[dict[str, Any]]] = None,
) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    if not selected_source:
        selected_source = {}

    for candidate in (selected_source.get("article_resolved_url"), selected_source.get("article_url")):
        _append_unique_url(urls, seen, candidate)

    for membership in memberships or []:
        for source_url in membership.get("source_urls") or []:
            if not isinstance(source_url, dict) or not _is_public_source_url(source_url):
                continue
            _append_unique_url(urls, seen, source_url.get("resolved_url"))
            _append_unique_url(urls, seen, source_url.get("url"))
    return urls


def _build_source_disclosure_payload(
    field_provenance: dict[str, Any] | None,
    memberships: list[dict[str, Any]],
    *,
    resolved_field_values: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    disclosure_doc = _extract_source_disclosure_document(field_provenance)
    sources = disclosure_doc.get("sources") if isinstance(disclosure_doc, dict) else None
    if not isinstance(sources, list) or not sources:
        return {}

    tracked_field_labels = disclosure_doc.get("tracked_field_labels")
    if not isinstance(tracked_field_labels, dict):
        tracked_field_labels = {}
    resolved_field_sources = _extract_field_provenance_map(field_provenance)

    membership_by_incident_id = {
        str(item.get("source_incident_id")): item
        for item in memberships
        if item.get("source_incident_id")
    }
    selected_source_enrichment_id = disclosure_doc.get("selected_source_enrichment_id")
    source_summaries: list[dict[str, Any]] = []
    selected_source_summary: dict[str, Any] | None = None

    for source in sources:
        if not isinstance(source, dict):
            continue
        source_incident_id = str(source.get("source_incident_id") or "")
        membership = membership_by_incident_id.get(source_incident_id, {})
        disclosed_fields = source.get("disclosed_fields")
        if not isinstance(disclosed_fields, list):
            disclosed_fields = [
                key
                for key, value in (source.get("field_values") or {}).items()
                if _value_present(value)
            ]
        summary = {
            "source_enrichment_id": source.get("source_enrichment_id"),
            "source_incident_id": source.get("source_incident_id"),
            "source_name": source.get("source_name") or membership.get("source_name"),
            "source_group": source.get("source_group") or membership.get("source_group"),
            "raw_title": source.get("raw_title") or membership.get("raw_title"),
            "raw_subtitle": source.get("raw_subtitle") or membership.get("raw_subtitle"),
            "source_published_at": source.get("source_published_at") or membership.get("source_published_at"),
            "is_primary_member": bool(source.get("is_primary_member")),
            "survivor_score": source.get("survivor_score"),
            "score_breakdown": source.get("score_breakdown") or {},
            "field_count": int(source.get("field_count") or len(disclosed_fields)),
            "disclosed_fields": disclosed_fields,
            "source_urls": membership.get("source_urls") or [],
        }
        source_summaries.append(summary)
        if str(source.get("source_enrichment_id")) == str(selected_source_enrichment_id):
            selected_source_summary = summary

    source_summaries.sort(
        key=lambda item: (
            not item.get("is_primary_member"),
            -float(item.get("survivor_score") or 0.0),
            item.get("source_name") or "",
        )
    )
    if selected_source_summary is None and source_summaries:
        selected_source_summary = source_summaries[0]

    field_names: list[str] = []
    seen_fields: set[str] = set()
    for source in sources:
        field_values = source.get("field_values")
        if not isinstance(field_values, dict):
            continue
        for field_name, value in field_values.items():
            if not _value_present(value) or field_name in seen_fields:
                continue
            seen_fields.add(field_name)
            field_names.append(field_name)

    field_differences: list[dict[str, Any]] = []
    for field_name in field_names:
        reporting_sources: list[dict[str, Any]] = []
        present_fingerprints: set[str] = set()
        selected_value = None
        selected_display_value = None
        selected_source_name = None
        resolved_value = (resolved_field_values or {}).get(field_name)
        resolved_display_value = _display_disclosure_value(resolved_value)
        resolved_source_enrichment_id = resolved_field_sources.get(field_name)
        resolved_source_name = None
        sources_with_value = 0
        for source in sources:
            if not isinstance(source, dict):
                continue
            field_values = source.get("field_values") or {}
            raw_value = field_values.get(field_name)
            display_value = _display_disclosure_value(raw_value)
            has_value = _value_present(raw_value)
            if has_value:
                sources_with_value += 1
                present_fingerprints.add(_disclosure_fingerprint(raw_value))
            entry = {
                "source_enrichment_id": source.get("source_enrichment_id"),
                "source_incident_id": source.get("source_incident_id"),
                "source_name": source.get("source_name"),
                "is_primary_member": bool(source.get("is_primary_member")),
                "has_value": has_value,
                "value": raw_value,
                "display_value": display_value,
            }
            reporting_sources.append(entry)
            if str(source.get("source_enrichment_id")) == str(selected_source_enrichment_id):
                selected_value = raw_value
                selected_display_value = display_value
                selected_source_name = source.get("source_name")
            if str(source.get("source_enrichment_id")) == str(resolved_source_enrichment_id):
                resolved_source_name = source.get("source_name")

        sources_missing_value = max(len(sources) - sources_with_value, 0)
        has_disparity = len(present_fingerprints) > 1 or sources_missing_value > 0
        field_differences.append(
            {
                "field": field_name,
                "label": tracked_field_labels.get(field_name) or field_name.replace("_", " ").title(),
                "selected_value": selected_value,
                "selected_display_value": selected_display_value,
                "selected_source_name": selected_source_name,
                "resolved_value": resolved_value,
                "resolved_display_value": resolved_display_value,
                "resolved_source_enrichment_id": resolved_source_enrichment_id,
                "resolved_source_name": resolved_source_name,
                "resolved_source_is_selected": (
                    str(resolved_source_enrichment_id) == str(selected_source_enrichment_id)
                    if resolved_source_enrichment_id is not None
                    else None
                ),
                "sources_with_value": sources_with_value,
                "sources_missing_value": sources_missing_value,
                "distinct_value_count": len(present_fingerprints),
                "has_disparity": has_disparity,
                "reporting_sources": reporting_sources,
            }
        )

    field_differences.sort(
        key=lambda item: (
            not item.get("has_disparity", False),
            -int(item.get("sources_missing_value") or 0),
            -int(item.get("distinct_value_count") or 0),
            item.get("label") or "",
        )
    )

    selected_source_reason = None
    if selected_source_summary is not None:
        selected_source_reason = {
            **selected_source_summary,
            "selection_basis": disclosure_doc.get("selection_basis") or "highest_survivor_score",
            "why_selected": _build_selected_source_reasons(selected_source_summary, len(source_summaries)),
        }

    return {
        "selected_source_reason": selected_source_reason,
        "source_summaries": source_summaries,
        "field_differences": field_differences,
    }


class V2CanonicalReadService:
    """Build API-friendly read models from canonical incident tables."""

    def __init__(
        self,
        *,
        canonical_repository: Optional[CanonicalIncidentRepository] = None,
        analytics_refresh_repository: Optional[AnalyticsRefreshRepository] = None,
        article_repository: Optional[ArticleRepository] = None,
    ) -> None:
        self.canonical_repository = canonical_repository or CanonicalIncidentRepository()
        self.analytics_refresh_repository = analytics_refresh_repository or AnalyticsRefreshRepository()
        self.article_repository = article_repository or ArticleRepository()

    def list_recent_incidents(
        self,
        session: Session,
        *,
        limit: int = 50,
        statuses: Sequence[str] = ("open",),
    ) -> list[dict[str, Any]]:
        return self.list_incidents(
            session,
            limit=limit,
            statuses=statuses,
        )["items"]

    def list_incidents(
        self,
        session: Session,
        *,
        limit: int = 50,
        offset: int = 0,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        sort_by: str = "last_seen_at",
        sort_order: str = "desc",
    ) -> dict[str, Any]:
        rows = self.canonical_repository.list_recent_with_enrichment(
            session,
            statuses=statuses,
            limit=limit,
            offset=offset,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        total = self.canonical_repository.count_recent(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        items: list[dict[str, Any]] = []
        for canonical, enrichment, membership_count in rows:
            items.append(
                _summary_from_canonical(
                    canonical,
                    enrichment,
                    membership_count=int(membership_count or 0),
                )
            )
        return {
            "items": items,
            "total": total,
        }

    def list_legacy_incidents(
        self,
        session: Session,
        *,
        limit: int = 20,
        offset: int = 0,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        sort_by: str = "incident_date",
        sort_order: str = "desc",
    ) -> dict[str, Any]:
        result = self.list_incidents(
            session,
            limit=limit,
            offset=offset,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        total = int(result["total"])
        per_page = max(limit, 1)
        page = (offset // per_page) + 1
        total_pages = (total + per_page - 1) // per_page if total else 0
        incidents = [_to_legacy_incident_summary(item) for item in result["items"]]
        return {
            "incidents": incidents,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": total_pages,
                "has_next": offset + per_page < total,
                "has_prev": offset > 0,
            },
        }

    def get_incident_detail(self, session: Session, canonical_incident_id: str) -> dict[str, Any] | None:
        canonical = self.canonical_repository.get_by_id(session, canonical_incident_id)
        if canonical is None:
            return None
        enrichment = self.canonical_repository.get_enrichment(session, canonical_incident_id)
        membership_details = self.canonical_repository.list_membership_details(session, canonical_incident_id)
        timeline = self.canonical_repository.list_timeline_events(session, canonical_incident_id)
        selected_source = self.canonical_repository.get_selected_source_details(session, canonical_incident_id)
        fetch_attempts = []
        if selected_source and selected_source.get("source_incident_id"):
            fetch_attempts = [
                _serialize_fetch_attempt(attempt)
                for attempt in self.article_repository.list_fetch_attempts(
                    session,
                    selected_source["source_incident_id"],
                    limit=10,
                )
            ]
        snapshot = self.analytics_refresh_repository.get_by_key(
            session,
            f"canonical:{canonical_incident_id}",
        )
        serialized_memberships = [
            _serialize_membership(detail["membership"], source_details=detail)
            for detail in membership_details
        ]
        raw_field_provenance = (getattr(enrichment, "field_provenance", None) if enrichment else None) or {}
        canonical_projection = (getattr(enrichment, "canonical_projection", None) if enrichment else None) or {}
        source_disclosure = _build_source_disclosure_payload(
            raw_field_provenance,
            serialized_memberships,
            resolved_field_values=_extract_resolved_disclosure_values(canonical, canonical_projection),
        )
        diamond_model = _build_diamond_projection(
            canonical,
            enrichment,
            selected_source=selected_source,
        )
        return {
            **_summary_from_canonical(
                canonical,
                enrichment,
                membership_count=len(membership_details),
            ),
            "resolution_metadata": canonical.resolution_metadata or {},
            "field_provenance": _extract_field_provenance_map(raw_field_provenance),
            "source_disclosure": source_disclosure,
            "canonical_projection": canonical_projection,
            "diamond_model": diamond_model,
            "selected_source": selected_source,
            "fetch_attempts": fetch_attempts,
            "memberships": serialized_memberships,
            "timeline": [_serialize_timeline_event(event) for event in timeline],
            "snapshot": (snapshot.state_payload if snapshot else None) or {},
        }

    def get_legacy_incident_detail(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> dict[str, Any] | None:
        detail = self.get_incident_detail(session, canonical_incident_id)
        if detail is None:
            return None

        projection = detail.get("canonical_projection") or {}
        attack_dynamics = projection.get("attack_dynamics") or {}
        data_impact = _projection_section(projection, "data_impact")
        system_impact = _projection_section(projection, "system_impact")
        user_impact = _projection_section(projection, "user_impact")
        financial_impact = _projection_section(projection, "financial_impact")
        regulatory_impact = _projection_section(projection, "regulatory_impact")
        research_impact = _projection_section(projection, "research_impact")
        recovery_metrics = _projection_section(projection, "recovery_metrics")
        transparency_metrics = _projection_section(projection, "transparency_metrics")
        selected_source = detail.get("selected_source") or {}
        timeline = _to_legacy_timeline(detail.get("timeline") or [])
        urls = _collect_urls(selected_source, detail.get("memberships") or [])
        attack_vector = detail.get("attack_vector") or attack_dynamics.get("attack_vector") or projection.get("attack_vector")
        ransomware_family = detail.get("ransomware_family") or attack_dynamics.get("ransomware_family") or projection.get("ransomware_family")
        records_affected_exact = (
            data_impact.get("records_affected_exact")
            if data_impact.get("records_affected_exact") is not None
            else projection.get("records_affected_exact")
        )
        records_affected_min = (
            data_impact.get("records_affected_min")
            if data_impact.get("records_affected_min") is not None
            else projection.get("records_affected_min")
        )
        records_affected_max = (
            data_impact.get("records_affected_max")
            if data_impact.get("records_affected_max") is not None
            else projection.get("records_affected_max")
        )
        data_categories = (
            data_impact.get("data_categories")
            or data_impact.get("data_types_affected")
            or projection.get("data_categories")
        )
        data_exfiltrated = (
            data_impact.get("data_exfiltrated")
            if data_impact.get("data_exfiltrated") is not None
            else projection.get("data_exfiltrated")
        )
        if data_exfiltrated is None:
            data_exfiltrated = attack_dynamics.get("data_exfiltration")

        data_breached = projection.get("data_breached")
        if data_breached is None:
            data_breached = bool(
                records_affected_exact
                or records_affected_min
                or records_affected_max
                or data_categories
                or data_impact.get("personal_information")
                or data_impact.get("student_data")
                or data_impact.get("faculty_data")
                or data_impact.get("alumni_data")
                or data_impact.get("administrative_data")
                or data_impact.get("financial_data")
                or data_impact.get("medical_records")
            )

        total_individuals_affected = (
            user_impact.get("total_individuals_affected")
            if user_impact.get("total_individuals_affected") is not None
            else projection.get("total_individuals_affected")
        )
        if total_individuals_affected is None:
            total_individuals_affected = (
                user_impact.get("users_affected_exact")
                if user_impact.get("users_affected_exact") is not None
                else records_affected_exact
            )

        return {
            "incident_id": detail["canonical_incident_id"],
            "institution_name": detail.get("display_name") or detail.get("institution_name") or "Unknown",
            "institution_type": detail.get("institution_type"),
            "institution_size": projection.get("institution_size"),
            "country": detail.get("country"),
            "country_code": detail.get("country_code"),
            "region": detail.get("region"),
            "city": detail.get("city"),
            "incident_date": detail.get("incident_date"),
            "date_precision": detail.get("date_precision"),
            "discovery_date": projection.get("discovery_date"),
            "source_published_date": selected_source.get("article_publish_date"),
            "ingested_at": detail.get("first_seen_at"),
            "title": selected_source.get("article_title") or detail.get("canonical_summary"),
            "subtitle": selected_source.get("raw_subtitle"),
            "enriched_summary": detail.get("canonical_summary"),
            "initial_access_description": projection.get("initial_access_description"),
            "primary_url": urls[0] if urls else None,
            "all_urls": urls,
            "leak_site_url": projection.get("threat_actor_claim_url") or projection.get("leak_site_url"),
            "source_detail_url": projection.get("source_detail_url"),
            "screenshot_url": projection.get("screenshot_url"),
            "attack_type_hint": detail.get("attack_category"),
            "attack_category": detail.get("attack_category"),
            "incident_severity": detail.get("severity"),
            "status": detail.get("status") or "open",
            "source_confidence": "medium",
            "academic_period_affected": projection.get("academic_period_affected"),
            "dark_web_posting_confirmed": projection.get("dark_web_posting_confirmed"),
            "prior_breach_same_institution": projection.get("prior_breach_same_institution"),
            "threat_actor": projection.get("threat_actor") or detail.get("threat_actor_name"),
            "threat_actor_name": detail.get("threat_actor_name"),
            "threat_actor_category": projection.get("threat_actor_category"),
            "threat_actor_motivation": projection.get("threat_actor_motivation"),
            "threat_actor_origin_country": projection.get("threat_actor_origin_country"),
            "threat_actor_claim_url": projection.get("threat_actor_claim_url"),
            "timeline": timeline,
            "mitre_attack_techniques": projection.get("mitre_attack_techniques"),
            "attack_dynamics": {
                "attack_vector": attack_vector,
                "attack_chain": attack_dynamics.get("attack_chain"),
                "ransomware_family": ransomware_family,
                "data_exfiltration": data_exfiltrated,
                "encryption_impact": attack_dynamics.get("encryption_impact"),
                "ransom_demanded": projection.get("was_ransom_demanded") or attack_dynamics.get("ransom_demanded"),
                "ransom_amount": projection.get("ransom_amount") or attack_dynamics.get("ransom_amount"),
                "ransom_paid": projection.get("ransom_paid") or attack_dynamics.get("ransom_paid"),
                "recovery_timeframe_days": projection.get("recovery_duration_days") or attack_dynamics.get("recovery_timeframe_days"),
                "business_impact": projection.get("business_impact") or attack_dynamics.get("business_impact"),
                "operational_impact": projection.get("operational_impact") or attack_dynamics.get("operational_impact"),
            },
            "data_impact": {
                "data_breached": data_breached,
                "data_exfiltrated": data_exfiltrated,
                "data_categories": data_categories,
                "records_affected_exact": records_affected_exact,
                "records_affected_min": records_affected_min,
                "records_affected_max": records_affected_max,
                "pii_records_leaked": projection.get("pii_records_leaked"),
            },
            "system_impact": {
                "systems_affected": system_impact.get("systems_affected") or projection.get("systems_affected"),
                "critical_systems_affected": (
                    system_impact.get("critical_systems_affected")
                    if system_impact.get("critical_systems_affected") is not None
                    else projection.get("critical_systems_affected")
                ),
                "network_compromised": (
                    system_impact.get("network_compromised")
                    if system_impact.get("network_compromised") is not None
                    else projection.get("network_compromised")
                ),
                "email_system_affected": (
                    system_impact.get("email_system_affected")
                    if system_impact.get("email_system_affected") is not None
                    else projection.get("email_system_affected")
                ),
                "student_portal_affected": (
                    system_impact.get("student_portal_affected")
                    if system_impact.get("student_portal_affected") is not None
                    else projection.get("student_portal_affected")
                ),
                "research_systems_affected": (
                    system_impact.get("research_systems_affected")
                    if system_impact.get("research_systems_affected") is not None
                    else projection.get("research_systems_affected")
                ),
                "hospital_systems_affected": (
                    system_impact.get("hospital_systems_affected")
                    if system_impact.get("hospital_systems_affected") is not None
                    else projection.get("hospital_systems_affected")
                ),
                "cloud_services_affected": (
                    system_impact.get("cloud_services_affected")
                    if system_impact.get("cloud_services_affected") is not None
                    else projection.get("cloud_services_affected")
                ),
                "third_party_vendor_impact": (
                    system_impact.get("third_party_vendor_impact")
                    if system_impact.get("third_party_vendor_impact") is not None
                    else projection.get("third_party_vendor_impact")
                ),
                "vendor_name": detail.get("vendor_name"),
            },
            "user_impact": {
                "students_affected": user_impact.get("students_affected") or projection.get("students_affected"),
                "staff_affected": user_impact.get("staff_affected") or projection.get("staff_affected"),
                "faculty_affected": user_impact.get("faculty_affected") or projection.get("faculty_affected"),
                "alumni_affected": user_impact.get("alumni_affected") or projection.get("alumni_affected"),
                "parents_affected": user_impact.get("parents_affected") or projection.get("parents_affected"),
                "applicants_affected": user_impact.get("applicants_affected") or projection.get("applicants_affected"),
                "patients_affected": user_impact.get("patients_affected") or projection.get("patients_affected"),
                "users_affected_min": user_impact.get("users_affected_min") or projection.get("users_affected_min"),
                "users_affected_max": user_impact.get("users_affected_max") or projection.get("users_affected_max"),
                "users_affected_exact": user_impact.get("users_affected_exact") or projection.get("users_affected_exact"),
                "total_individuals_affected": total_individuals_affected,
            },
            "financial_impact": {
                "estimated_total_cost_usd": (
                    financial_impact.get("estimated_total_cost_usd")
                    or financial_impact.get("total_cost_estimate")
                    or projection.get("estimated_total_cost_usd")
                ),
                "ransom_cost_usd": (
                    financial_impact.get("ransom_cost_usd")
                    or financial_impact.get("ransom_amount_exact")
                    or projection.get("ransom_cost_usd")
                ),
                "recovery_cost_usd": (
                    financial_impact.get("recovery_cost_usd")
                    or financial_impact.get("recovery_costs_exact")
                    or projection.get("recovery_cost_usd")
                ),
                "legal_cost_usd": (
                    financial_impact.get("legal_cost_usd")
                    or financial_impact.get("legal_costs")
                    or projection.get("legal_cost_usd")
                ),
                "notification_cost_usd": (
                    financial_impact.get("notification_cost_usd")
                    or financial_impact.get("notification_costs")
                    or projection.get("notification_cost_usd")
                ),
                "insurance_claim": (
                    financial_impact.get("insurance_claim")
                    if financial_impact.get("insurance_claim") is not None
                    else projection.get("insurance_claim")
                ),
                "insurance_payout_usd": (
                    financial_impact.get("insurance_payout_usd")
                    or financial_impact.get("insurance_claim_amount")
                    or projection.get("insurance_payout_usd")
                ),
                "business_impact": financial_impact.get("business_impact") or projection.get("business_impact"),
            },
            "regulatory_impact": {
                "applicable_regulations": regulatory_impact.get("applicable_regulations") or projection.get("applicable_regulations"),
                "gdpr_breach": regulatory_impact.get("gdpr_breach") if regulatory_impact.get("gdpr_breach") is not None else projection.get("gdpr_breach"),
                "hipaa_breach": regulatory_impact.get("hipaa_breach") if regulatory_impact.get("hipaa_breach") is not None else projection.get("hipaa_breach"),
                "ferpa_breach": regulatory_impact.get("ferpa_breach") if regulatory_impact.get("ferpa_breach") is not None else projection.get("ferpa_breach"),
                "breach_notification_required": regulatory_impact.get("breach_notification_required") if regulatory_impact.get("breach_notification_required") is not None else projection.get("breach_notification_required"),
                "notification_sent": regulatory_impact.get("notification_sent") if regulatory_impact.get("notification_sent") is not None else projection.get("notification_sent"),
                "notification_sent_date": regulatory_impact.get("notification_sent_date") or projection.get("notification_sent_date"),
                "notification_delay_days": regulatory_impact.get("notification_delay_days") or projection.get("notification_delay_days"),
                "dpa_notified": regulatory_impact.get("dpa_notified") if regulatory_impact.get("dpa_notified") is not None else projection.get("dpa_notified"),
                "investigation_opened": regulatory_impact.get("investigation_opened") if regulatory_impact.get("investigation_opened") is not None else projection.get("investigation_opened"),
                "fine_imposed": regulatory_impact.get("fine_imposed") if regulatory_impact.get("fine_imposed") is not None else projection.get("fine_imposed"),
                "fine_amount_usd": regulatory_impact.get("fine_amount_usd") or projection.get("fine_amount_usd"),
                "lawsuits_filed": regulatory_impact.get("lawsuits_filed") if regulatory_impact.get("lawsuits_filed") is not None else projection.get("lawsuits_filed"),
                "class_action_filed": regulatory_impact.get("class_action_filed") if regulatory_impact.get("class_action_filed") is not None else projection.get("class_action_filed"),
            },
            "research_impact": {
                "research_projects_affected": research_impact.get("research_projects_affected") or projection.get("research_projects_affected"),
                "research_data_compromised": research_impact.get("research_data_compromised") if research_impact.get("research_data_compromised") is not None else projection.get("research_data_compromised"),
                "publications_delayed": research_impact.get("publications_delayed") if research_impact.get("publications_delayed") is not None else projection.get("publications_delayed"),
                "grants_affected": research_impact.get("grants_affected") or projection.get("grants_affected"),
                "research_area": research_impact.get("research_area") or projection.get("research_area"),
            },
            "recovery_metrics": {
                "recovery_method": recovery_metrics.get("recovery_method") or projection.get("recovery_method"),
                "recovery_duration_days": (
                    recovery_metrics.get("recovery_duration_days")
                    or recovery_metrics.get("recovery_timeframe_days")
                    or projection.get("recovery_duration_days")
                ),
                "from_backup": recovery_metrics.get("from_backup") if recovery_metrics.get("from_backup") is not None else projection.get("from_backup"),
                "backup_status": recovery_metrics.get("backup_status") or projection.get("backup_status"),
                "backup_age_days": recovery_metrics.get("backup_age_days") or projection.get("backup_age_days"),
                "mfa_implemented": recovery_metrics.get("mfa_implemented") if recovery_metrics.get("mfa_implemented") is not None else projection.get("mfa_implemented"),
                "law_enforcement_involved": recovery_metrics.get("law_enforcement_involved") if recovery_metrics.get("law_enforcement_involved") is not None else projection.get("law_enforcement_involved"),
                "law_enforcement_agency": recovery_metrics.get("law_enforcement_agency") or projection.get("law_enforcement_agency"),
                "ir_firm_engaged": (
                    recovery_metrics.get("ir_firm_engaged")
                    or recovery_metrics.get("incident_response_firm")
                    or projection.get("ir_firm_engaged")
                ),
                "forensics_firm": recovery_metrics.get("forensics_firm") or projection.get("forensics_firm"),
                "security_improvements": recovery_metrics.get("security_improvements") or projection.get("security_improvements"),
            },
            "transparency_metrics": {
                "public_disclosure": transparency_metrics.get("public_disclosure") if transparency_metrics.get("public_disclosure") is not None else projection.get("public_disclosure"),
                "public_disclosure_date": transparency_metrics.get("public_disclosure_date") or projection.get("public_disclosure_date"),
                "disclosure_delay_days": transparency_metrics.get("disclosure_delay_days") or projection.get("disclosure_delay_days"),
                "transparency_level": transparency_metrics.get("transparency_level") or projection.get("transparency_level"),
            },
            "llm_enriched": bool(detail.get("selected_source_enrichment_id")),
            "llm_enriched_at": detail.get("updated_at"),
            "sources": _to_legacy_sources(detail.get("memberships") or []),
            "source_disclosure": detail.get("source_disclosure") or {},
            "notes": projection.get("notes"),
            "data_breached": data_breached,
            "data_exfiltrated": data_exfiltrated,
            "records_affected_exact": records_affected_exact,
            "records_affected_min": records_affected_min,
            "records_affected_max": records_affected_max,
            "pii_records_leaked": projection.get("pii_records_leaked"),
            "systems_affected": system_impact.get("systems_affected") or projection.get("systems_affected"),
            "teaching_impacted": projection.get("teaching_impacted"),
            "research_impacted": projection.get("research_impacted"),
            "classes_cancelled": projection.get("classes_cancelled"),
            "exams_postponed": projection.get("exams_postponed"),
            "downtime_days": projection.get("downtime_days"),
            "recovery_costs_min": projection.get("recovery_costs_min"),
            "recovery_costs_max": projection.get("recovery_costs_max"),
            "ransom_amount": projection.get("ransom_amount"),
            "ransom_currency": projection.get("ransom_currency"),
            "ransom_paid": projection.get("ransom_paid"),
            "ransom_paid_amount": projection.get("ransom_paid_amount"),
            "fine_amount": projection.get("fine_amount"),
            "attack_vector": attack_vector,
            "access_vector": projection.get("access_vector") or attack_vector,
            "ransomware_family": ransomware_family,
        }

    def get_dashboard_summary(self, session: Session) -> dict[str, Any]:
        snapshot = self.analytics_refresh_repository.get_by_key(session, "dashboard:global")
        if snapshot is not None and snapshot.state_payload and _is_full_dashboard_snapshot(snapshot.state_payload):
            return snapshot.state_payload

        return self.build_dashboard_payload(session)

    def get_dashboard_stats(self, session: Session) -> dict[str, Any]:
        return self.get_dashboard_summary(session)["stats"]

    def get_intelligence_summary(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, Any]:
        if tuple(statuses) == ("open",):
            snapshot = self.analytics_refresh_repository.get_by_key(session, "dashboard:global")
            if (
                snapshot is not None
                and isinstance(snapshot.state_payload, dict)
                and _is_full_dashboard_snapshot(snapshot.state_payload)
                and isinstance(snapshot.state_payload.get("intelligence_summary"), dict)
            ):
                return snapshot.state_payload["intelligence_summary"]
        return self._build_intelligence_summary(
            session,
            statuses=statuses,
        )

    def _build_intelligence_summary(
        self,
        session: Session,
        *,
        statuses: Sequence[str],
        rollup: Optional[dict[str, Any]] = None,
        countries: Optional[list[dict[str, Any]]] = None,
        ransomware: Optional[list[dict[str, Any]]] = None,
    ) -> dict[str, Any]:
        total_incidents = int(
            (rollup or {}).get("canonical_incident_count")
            or self.canonical_repository.count_recent(session, statuses=statuses)
            or 0
        )
        rows = self.canonical_repository.list_recent_with_enrichment(
            session,
            statuses=statuses,
            limit=max(total_incidents, 1),
            offset=0,
            sort_by="incident_date",
            sort_order="desc",
        )
        country_breakdown = countries or self.canonical_repository.get_country_breakdown(
            session,
            statuses=statuses,
            limit=8,
        )
        ransomware_breakdown = ransomware or self.canonical_repository.get_ransomware_breakdown(
            session,
            statuses=statuses,
            limit=8,
        )
        threat_actor_breakdown = self.canonical_repository.get_threat_actor_breakdown(
            session,
            statuses=statuses,
            limit=8,
        )
        if not isinstance(threat_actor_breakdown, dict):
            threat_actor_breakdown = {}

        institution_segments: Counter[str] = Counter()
        attack_clusters: Counter[str] = Counter()
        attack_vectors: Counter[str] = Counter()
        largest_record_events: list[dict[str, Any]] = []

        actor_attributed_count = 0
        ransomware_count = 0
        breach_count = 0
        vendor_linked_count = 0
        attack_vector_known_count = 0
        known_record_events = 0
        known_record_volume = 0
        timeline_points: list[dict[str, Any]] = []

        for canonical, enrichment, _membership_count in rows:
            projection = (
                getattr(enrichment, "canonical_projection", None)
                if enrichment and isinstance(getattr(enrichment, "canonical_projection", None), dict)
                else {}
            )
            attack_dynamics = _projection_section(projection, "attack_dynamics")
            data_impact = _projection_section(projection, "data_impact")

            display_name = canonical.institution_name or canonical.vendor_name or "Unknown"
            incident_date = canonical.incident_date
            if incident_date is None and canonical.last_seen_at:
                incident_date = canonical.last_seen_at.date()

            segment = _normalize_institution_segment(canonical.institution_type, canonical.vendor_name)
            cluster = _normalize_attack_cluster(canonical.attack_category)
            vector = _normalize_attack_vector(
                attack_dynamics.get("attack_vector") or canonical.attack_vector
            )

            is_actor_attributed = bool(canonical.threat_actor_name)
            is_ransomware = bool(
                canonical.ransomware_family
                or ((canonical.attack_category or "").lower().startswith("ransomware"))
            )
            is_breach = bool(
                (canonical.attack_category or "").lower().startswith("data_breach")
            )
            is_vendor_linked = bool(
                canonical.vendor_name
                or cluster == "Third-Party & Supply Chain"
                or vector in {"Third-Party Vendor", "Supply Chain Compromise"}
            )
            exact_records = _coerce_int(data_impact.get("records_affected_exact"))

            institution_segments[segment] += 1
            attack_clusters[cluster] += 1
            if vector:
                attack_vectors[vector] += 1
                attack_vector_known_count += 1

            if is_actor_attributed:
                actor_attributed_count += 1
            if is_ransomware:
                ransomware_count += 1
            if is_breach:
                breach_count += 1
            if is_vendor_linked:
                vendor_linked_count += 1

            if exact_records and exact_records > 0:
                known_record_events += 1
                known_record_volume += exact_records
                largest_record_events.append(
                    {
                        "incident_id": str(canonical.id),
                        "display_name": display_name,
                        "country": canonical.country,
                        "country_code": canonical.country_code,
                        "incident_date": incident_date.isoformat() if incident_date else None,
                        "records_affected": exact_records,
                        "attack_category": canonical.attack_category,
                    }
                )

            timeline_points.append(
                {
                    "incident_date": incident_date,
                    "ransomware": is_ransomware,
                    "vendor_linked": is_vendor_linked,
                    "breach": is_breach,
                }
            )

        largest_record_events.sort(key=lambda item: int(item["records_affected"]), reverse=True)
        largest_record_events = largest_record_events[:5]

        anchor_date = max(
            (item["incident_date"] for item in timeline_points if item["incident_date"] is not None),
            default=None,
        )
        recent_90d_count = 0
        prior_90d_count = 0
        recent_ransomware_count = 0
        recent_vendor_count = 0
        recent_breach_count = 0
        if anchor_date is not None:
            recent_start = anchor_date - timedelta(days=89)
            prior_start = recent_start - timedelta(days=90)
            prior_end = recent_start - timedelta(days=1)
            for item in timeline_points:
                point_date = item["incident_date"]
                if point_date is None:
                    continue
                if recent_start <= point_date <= anchor_date:
                    recent_90d_count += 1
                    recent_ransomware_count += int(item["ransomware"])
                    recent_vendor_count += int(item["vendor_linked"])
                    recent_breach_count += int(item["breach"])
                elif prior_start <= point_date <= prior_end:
                    prior_90d_count += 1

        recent_change_count = recent_90d_count - prior_90d_count
        recent_change_pct = (
            (recent_change_count / prior_90d_count * 100.0)
            if prior_90d_count
            else None
        )

        total = max(total_incidents, 1)
        segment_breakdown = _to_ranked_items(
            institution_segments,
            total=total_incidents,
            label_key="segment",
        )
        cluster_breakdown = _to_ranked_items(
            attack_clusters,
            total=total_incidents,
            label_key="cluster",
        )
        vector_breakdown = _to_ranked_items(
            attack_vectors,
            total=max(attack_vector_known_count, 1),
            label_key="vector",
        )[:8]

        top_countries = _to_count_by_category(
            country_breakdown,
            label_key="country",
            country_code_key="country_code",
        )
        top_ransomware = _to_count_by_category(
            ransomware_breakdown,
            label_key="ransomware_family",
        )

        threat_actors = list(threat_actor_breakdown.get("threat_actors") or [])
        top_actor = threat_actors[0] if threat_actors else None
        lead_cluster = cluster_breakdown[0] if cluster_breakdown else None
        lead_segment = segment_breakdown[0] if segment_breakdown else None
        lead_family = top_ransomware[0] if top_ransomware else None

        priority_findings: list[dict[str, Any]] = []
        if lead_cluster:
            priority_findings.append(
                {
                    "title": "Primary intrusion pattern",
                    "value": lead_cluster["cluster"],
                    "context": f"{lead_cluster['count']} canonicals, {lead_cluster['percentage']:.1f}% of the open dataset",
                }
            )
        if lead_segment:
            priority_findings.append(
                {
                    "title": "Most exposed victim segment",
                    "value": lead_segment["segment"],
                    "context": f"{lead_segment['count']} incidents across the retained education dataset",
                }
            )
        if top_actor:
            priority_findings.append(
                {
                    "title": "Most active attributed actor",
                    "value": top_actor.get("name"),
                    "context": f"{int(top_actor.get('incident_count') or 0)} canonicals with attribution to this group",
                }
            )
        elif lead_family:
            priority_findings.append(
                {
                    "title": "Leading ransomware family",
                    "value": lead_family["category"],
                    "context": f"{lead_family['count']} canonicals with this family attached",
                }
            )
        if vendor_linked_count:
            priority_findings.append(
                {
                    "title": "Vendor-mediated exposure",
                    "value": f"{vendor_linked_count} incidents",
                    "context": f"{vendor_linked_count / total * 100.0:.1f}% of open canonicals show vendor or supply-chain involvement",
                }
            )

        return {
            "overview": {
                "total_incidents": total_incidents,
                "actor_attributed_count": actor_attributed_count,
                "actor_attributed_share": actor_attributed_count / total * 100.0,
                "ransomware_count": ransomware_count,
                "ransomware_share": ransomware_count / total * 100.0,
                "breach_count": breach_count,
                "breach_share": breach_count / total * 100.0,
                "vendor_linked_count": vendor_linked_count,
                "vendor_linked_share": vendor_linked_count / total * 100.0,
                "known_record_events": known_record_events,
                "known_record_volume": known_record_volume,
            },
            "tempo": {
                "anchor_date": anchor_date.isoformat() if anchor_date else None,
                "recent_90d_count": recent_90d_count,
                "prior_90d_count": prior_90d_count,
                "recent_change_count": recent_change_count,
                "recent_change_pct": recent_change_pct,
                "recent_ransomware_count": recent_ransomware_count,
                "recent_vendor_count": recent_vendor_count,
                "recent_breach_count": recent_breach_count,
            },
            "victimology": {
                "institution_segments": segment_breakdown,
                "top_countries": top_countries,
                "vendor_linked_count": vendor_linked_count,
                "direct_victim_count": max(total_incidents - vendor_linked_count, 0),
            },
            "tradecraft": {
                "attack_clusters": cluster_breakdown,
                "attack_vectors": vector_breakdown,
                "attack_vector_known_count": attack_vector_known_count,
                "attack_vector_known_share": attack_vector_known_count / total * 100.0,
            },
            "attribution": {
                "top_threat_actors": threat_actors,
                "top_ransomware_families": top_ransomware,
                "actor_attributed_count": actor_attributed_count,
                "actor_attributed_share": actor_attributed_count / total * 100.0,
            },
            "exposure": {
                "breach_count": breach_count,
                "known_record_events": known_record_events,
                "known_record_volume": known_record_volume,
                "largest_record_events": largest_record_events,
            },
            "coverage": {
                "attack_vector_known_count": attack_vector_known_count,
                "attack_vector_known_share": attack_vector_known_count / total * 100.0,
                "record_loss_known_count": known_record_events,
                "record_loss_known_share": known_record_events / total * 100.0,
                "attribution_known_count": actor_attributed_count,
                "attribution_known_share": actor_attributed_count / total * 100.0,
            },
            "priority_findings": priority_findings,
        }

    def _build_diamond_summary(
        self,
        session: Session,
        *,
        statuses: Sequence[str],
    ) -> dict[str, Any]:
        total_incidents = int(self.canonical_repository.count_recent(session, statuses=statuses) or 0)
        rows = self.canonical_repository.list_recent_with_enrichment(
            session,
            statuses=statuses,
            limit=max(total_incidents, 1),
            offset=0,
            sort_by="incident_date",
            sort_order="desc",
        )

        vertex_counts = Counter[str]()
        high_confidence_counts = Counter[str]()
        adversaries = Counter[str]()
        capabilities = Counter[str]()
        infrastructure_components = Counter[str]()
        victim_segments = Counter[str]()
        all_core_vertices_count = 0
        vendor_mediated_count = 0

        for canonical, enrichment, _membership_count in rows:
            diamond = _build_diamond_projection(canonical, enrichment)
            for vertex in ("victim", "adversary", "capability", "infrastructure"):
                payload = diamond[vertex]
                if payload.get("present"):
                    vertex_counts[vertex] += 1
                if payload.get("confidence") == "high":
                    high_confidence_counts[vertex] += 1

            if diamond["event_meta"]["all_core_vertices_present"]:
                all_core_vertices_count += 1

            victim_segments[_normalize_institution_segment(canonical.institution_type, canonical.vendor_name)] += 1
            if diamond["victim"].get("scope") == "institution_via_vendor":
                vendor_mediated_count += 1

            adversary_name = diamond["adversary"].get("name")
            if adversary_name:
                adversaries[str(adversary_name)] += 1

            capability_label = diamond["capability"].get("attack_category") or diamond["capability"].get("attack_vector")
            if capability_label:
                capabilities[_humanize_slug(str(capability_label))] += 1

            for component in diamond["infrastructure"].get("components") or []:
                infrastructure_components[str(component)] += 1

        denominator = max(total_incidents, 1)
        return {
            "model_version": "diamond_v1",
            "overview": {
                "total_incidents": total_incidents,
                "all_core_vertices_count": all_core_vertices_count,
                "all_core_vertices_share": all_core_vertices_count / denominator * 100.0,
                "vendor_mediated_count": vendor_mediated_count,
                "vendor_mediated_share": vendor_mediated_count / denominator * 100.0,
            },
            "coverage": {
                "victim_vertex_count": vertex_counts["victim"],
                "victim_vertex_share": vertex_counts["victim"] / denominator * 100.0,
                "adversary_vertex_count": vertex_counts["adversary"],
                "adversary_vertex_share": vertex_counts["adversary"] / denominator * 100.0,
                "capability_vertex_count": vertex_counts["capability"],
                "capability_vertex_share": vertex_counts["capability"] / denominator * 100.0,
                "infrastructure_vertex_count": vertex_counts["infrastructure"],
                "infrastructure_vertex_share": vertex_counts["infrastructure"] / denominator * 100.0,
            },
            "confidence": {
                "victim_high_confidence_count": high_confidence_counts["victim"],
                "adversary_high_confidence_count": high_confidence_counts["adversary"],
                "capability_high_confidence_count": high_confidence_counts["capability"],
                "infrastructure_high_confidence_count": high_confidence_counts["infrastructure"],
            },
            "vertices": {
                "victim_segments": _to_ranked_items(victim_segments, total=total_incidents, label_key="segment"),
                "top_adversaries": _to_ranked_items(adversaries, total=max(vertex_counts["adversary"], 1), label_key="name")[:10],
                "top_capabilities": _to_ranked_items(capabilities, total=max(vertex_counts["capability"], 1), label_key="label")[:10],
                "infrastructure_components": _to_ranked_items(
                    infrastructure_components,
                    total=max(sum(infrastructure_components.values()), 1),
                    label_key="component",
                )[:10],
            },
            "research_notes": {
                "infrastructure_sparse": vertex_counts["infrastructure"] < total_incidents,
                "adversary_sparse": vertex_counts["adversary"] < total_incidents,
                "null_vertices_expected_in_public_reporting": True,
            },
        }

    def build_dashboard_payload(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        refreshed_at: Optional[str] = None,
    ) -> dict[str, Any]:
        effective_refreshed_at = refreshed_at or datetime.now(timezone.utc).isoformat()
        rollup = self.canonical_repository.get_dashboard_rollup(session, statuses=statuses)
        country_limit = min(
            max(int(rollup.get("countries_affected") or 0), 10),
            _DASHBOARD_COUNTRY_LIMIT,
        )
        countries = self.canonical_repository.get_country_breakdown(
            session,
            statuses=statuses,
            limit=country_limit,
        )
        attacks = self.canonical_repository.get_attack_breakdown(session, statuses=statuses)
        ransomware = self.canonical_repository.get_ransomware_breakdown(session, statuses=statuses)
        trend = self.canonical_repository.get_incident_trend(
            session,
            statuses=statuses,
            bucket="month",
            limit=24,
        )
        recent = self.list_incidents(
            session,
            limit=10,
            offset=0,
            statuses=statuses,
            sort_by="incident_date",
            sort_order="desc",
        )["items"]
        intelligence_summary = self._build_intelligence_summary(
            session,
            statuses=statuses,
            rollup=rollup,
            countries=countries,
            ransomware=ransomware,
        )
        diamond_summary = self._build_diamond_summary(
            session,
            statuses=statuses,
        )

        return {
            "totals": rollup,
            "stats": _dashboard_stats_from_rollup(rollup, refreshed_at=effective_refreshed_at),
            "intelligence_summary": intelligence_summary,
            "diamond_summary": diamond_summary,
            "incidents_by_country": _to_count_by_category(
                countries,
                label_key="country",
                country_code_key="country_code",
            ),
            "incidents_by_attack_type": _to_count_by_category(
                attacks,
                label_key="attack_category",
            ),
            "incidents_by_ransomware": _to_count_by_category(
                ransomware,
                label_key="ransomware_family",
            ),
            "incidents_over_time": _to_time_series(trend),
            "recent_incidents": _to_recent_incidents(recent),
            "top_countries": countries,
            "top_attack_categories": attacks,
            "top_ransomware_families": ransomware,
            "refreshed_at": effective_refreshed_at,
        }

    def get_incident_facets(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        facet_limit: int = 20,
    ) -> dict[str, Any]:
        return {
            "countries": self.canonical_repository.get_country_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
            "attack_categories": self.canonical_repository.get_attack_category_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
            "institution_types": self.canonical_repository.get_institution_type_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
            "severities": self.canonical_repository.get_severity_facets(
                session,
                statuses=statuses,
                search=search,
                country_code=country_code,
                attack_category=attack_category,
                institution_type=institution_type,
                severity=severity,
                is_education_related=is_education_related,
                has_vendor=has_vendor,
                date_from=date_from,
                date_to=date_to,
                limit=facet_limit,
            ),
        }

    def get_analytics_breakdowns(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        breakdown_limit: int = 20,
    ) -> dict[str, Any]:
        return self.get_incident_facets(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            facet_limit=breakdown_limit,
        )

    def get_country_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = _DASHBOARD_COUNTRY_LIMIT,
    ) -> dict[str, Any]:
        data = _to_count_by_category(
            self.canonical_repository.get_country_breakdown(session, statuses=statuses, limit=limit),
            label_key="country",
            country_code_key="country_code",
        )
        return {"data": data, "total": sum(item["count"] for item in data)}

    def get_attack_type_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 15,
    ) -> dict[str, Any]:
        data = _to_count_by_category(
            self.canonical_repository.get_attack_breakdown(session, statuses=statuses, limit=limit),
            label_key="attack_category",
        )
        return {"data": data, "total": sum(item["count"] for item in data)}

    def get_ransomware_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 15,
    ) -> dict[str, Any]:
        data = _to_count_by_category(
            self.canonical_repository.get_ransomware_breakdown(session, statuses=statuses, limit=limit),
            label_key="ransomware_family",
        )
        return {"data": data, "total": sum(item["count"] for item in data)}

    def get_mitre_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        technique_limit: int = 20,
        per_tactic_limit: int = 5,
    ) -> dict[str, Any]:
        rows = session.execute(
            select(CanonicalEnrichment.canonical_projection)
            .join(
                CanonicalIncident,
                CanonicalIncident.id == CanonicalEnrichment.canonical_incident_id,
            )
            .where(CanonicalIncident.status.in_(tuple(statuses)))
        ).all()

        total_incidents = int(self.canonical_repository.count_recent(session, statuses=statuses) or 0)
        incidents_with_mitre = 0
        technique_count_total = 0
        tactic_incident_counts: Counter[str] = Counter()
        tactic_technique_counts: Counter[str] = Counter()
        technique_counts: Counter[tuple[str, str, str]] = Counter()

        for row in rows:
            projection = row[0] if isinstance(row, tuple) else getattr(row, "canonical_projection", None)
            if not isinstance(projection, dict):
                continue
            techniques = _as_list(projection.get("mitre_attack_techniques"))
            if not techniques:
                continue

            incidents_with_mitre += 1
            technique_count_total += len(techniques)
            seen_tactics: set[str] = set()

            for entry in techniques:
                if not isinstance(entry, dict):
                    continue
                tactic = _canonical_tactic(entry.get("tactic"))
                technique_id = str(entry.get("technique_id") or "Unknown")
                technique_name = str(entry.get("technique_name") or technique_id)
                key = (tactic, technique_id, technique_name)
                technique_counts[key] += 1
                tactic_technique_counts[tactic] += 1
                seen_tactics.add(tactic)

            for tactic in seen_tactics:
                tactic_incident_counts[tactic] += 1

        denominator = max(total_incidents, 1)
        tactics = [
            {
                "tactic": tactic,
                "incident_count": count,
                "incident_percentage": count / denominator * 100.0,
                "technique_count": tactic_technique_counts[tactic],
            }
            for tactic, count in tactic_incident_counts.most_common()
        ]
        techniques = [
            {
                "tactic": tactic,
                "technique_id": technique_id,
                "technique_name": technique_name,
                "count": count,
                "percentage": count / max(technique_count_total, 1) * 100.0,
            }
            for (tactic, technique_id, technique_name), count in technique_counts.most_common(technique_limit)
        ]

        top_techniques_by_tactic: list[dict[str, Any]] = []
        for tactic, _count in tactic_incident_counts.most_common():
            tactic_items = [
                {
                    "technique_id": technique_id,
                    "technique_name": technique_name,
                    "count": count,
                    "percentage": count / max(tactic_technique_counts[tactic], 1) * 100.0,
                }
                for (item_tactic, technique_id, technique_name), count in technique_counts.most_common()
                if item_tactic == tactic
            ][:per_tactic_limit]
            top_techniques_by_tactic.append(
                {
                    "tactic": tactic,
                    "techniques": tactic_items,
                }
            )

        return {
            "overview": {
                "total_incidents": total_incidents,
                "incidents_with_mitre": incidents_with_mitre,
                "incidents_with_mitre_share": incidents_with_mitre / denominator * 100.0,
                "technique_count_total": technique_count_total,
                "unique_tactic_count": len(tactic_incident_counts),
                "unique_technique_count": len(technique_counts),
            },
            "tactics": tactics,
            "techniques": techniques,
            "top_techniques_by_tactic": top_techniques_by_tactic,
        }

    def get_incident_trend(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        bucket: str = "month",
        limit: int = 24,
    ) -> list[dict[str, Any]]:
        return self.canonical_repository.get_incident_trend(
            session,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            bucket=bucket,
            limit=limit,
        )

    def get_kpi_trends(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        months: int = 12,
    ) -> dict[str, Any]:
        """Per-KPI monthly sparkline series + current value and period-over-period delta.

        Powers the dashboard KPI tiles whose headline number is accompanied by a
        small trend line. Each series is oldest → newest so the sparkline reads
        left-to-right.
        """
        metrics = ("incidents", "ransomware", "breaches", "actors", "supply_chain", "countries")
        result: dict[str, Any] = {}
        for metric in metrics:
            series = _to_time_series(
                self.canonical_repository.get_kpi_trend(
                    session,
                    metric=metric,
                    statuses=statuses,
                    bucket="month",
                    limit=months,
                )
            )
            counts = [point["count"] for point in series]
            total = sum(counts)
            # Trend direction: compare the recent half of the window against the
            # prior half. The final bucket is the current (partial) month, so it
            # is excluded from the delta math to avoid an artificial drop — it
            # still appears in the sparkline series.
            stable = counts[:-1] if len(counts) >= 4 else counts
            delta_pct: float | None = None
            if len(stable) >= 2:
                half = len(stable) // 2
                prior = sum(stable[:half])
                recent = sum(stable[half:])
                if prior > 0:
                    delta_pct = round((recent - prior) / prior * 100, 1)
            result[metric] = {
                "series": series,
                "values": counts,
                "total": total,
                "current": counts[-1] if counts else 0,
                "previous": counts[-2] if len(counts) >= 2 else 0,
                "delta_pct": delta_pct,
            }
        return result

    def get_timeline_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        months: int = 24,
    ) -> dict[str, Any]:
        items = _to_time_series(
            self.canonical_repository.get_incident_trend(
                session,
                statuses=statuses,
                bucket="month",
                limit=months,
            )
        )
        return {"data": items, "total": sum(item["count"] for item in items)}

    def get_threat_actor_analytics(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 20,
    ) -> dict[str, Any]:
        return self.canonical_repository.get_threat_actor_breakdown(
            session,
            statuses=statuses,
            limit=limit,
        )

    def get_filter_options(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, Any]:
        return self.canonical_repository.get_filter_options(
            session,
            statuses=statuses,
        )

    def get_feed_health(self, session: Session, *, limit: int = 50) -> dict[str, Any]:
        """Per-source ingestion health for the Intel Feeds page.

        Aggregates the raw ``source_incidents`` collection layer by feed:
        lifetime + trailing-30d volume, the most recent collection/publish
        timestamps, and a freshness status derived from how long it has been
        since the feed last delivered an event.
        """
        from sqlalchemy import func as _func  # local import keeps module header tidy

        now = datetime.now(timezone.utc)
        cutoff_30d = now - timedelta(days=30)

        rows = session.execute(
            select(
                SourceIncident.source_name,
                SourceIncident.source_group,
                _func.count(SourceIncident.id).label("events_total"),
                _func.count(SourceIncident.id)
                .filter(SourceIncident.collected_at >= cutoff_30d)
                .label("events_30d"),
                _func.max(SourceIncident.collected_at).label("last_collected_at"),
                _func.max(SourceIncident.source_published_at).label("last_published_at"),
            )
            .where(SourceIncident.is_deleted.is_(False))
            .group_by(SourceIncident.source_name, SourceIncident.source_group)
            .order_by(_func.count(SourceIncident.id).desc())
            .limit(limit)
        ).all()

        feeds: list[dict[str, Any]] = []
        groups: Counter[str] = Counter()
        healthy = stale = offline = 0
        events_24h_total = 0
        cutoff_24h = now - timedelta(hours=24)
        for row in rows:
            last_collected = row.last_collected_at
            age_days = (now - last_collected).total_seconds() / 86400 if last_collected else None
            if age_days is None or age_days > 30:
                status = "offline"
                offline += 1
            elif age_days > 7:
                status = "stale"
                stale += 1
            else:
                status = "healthy"
                healthy += 1
            events_24h = (
                int(row.events_30d) if last_collected and last_collected >= cutoff_24h and age_days and age_days < 1 else 0
            )
            events_24h_total += events_24h
            group = row.source_group or "other"
            groups[group] += int(row.events_total)
            feeds.append(
                {
                    "source": row.source_name,
                    "group": group,
                    "events_total": int(row.events_total),
                    "events_30d": int(row.events_30d or 0),
                    "last_collected_at": last_collected.isoformat() if last_collected else None,
                    "last_published_at": row.last_published_at.isoformat() if row.last_published_at else None,
                    "age_days": round(age_days, 1) if age_days is not None else None,
                    "status": status,
                }
            )

        total_events = sum(f["events_total"] for f in feeds)
        return {
            "summary": {
                "feed_count": len(feeds),
                "healthy": healthy,
                "stale": stale,
                "offline": offline,
                "events_total": total_events,
                "events_30d": sum(f["events_30d"] for f in feeds),
            },
            "by_group": [{"group": g, "events": c} for g, c in groups.most_common()],
            "feeds": feeds,
        }
