"""Build the star-schema analytical layer from operational canonical data.

``build_fact_for_canonical`` reads a canonical incident plus its
``canonical_enrichments.canonical_projection`` JSONB and upserts the matching
``fact_incident`` row together with its dimension keys and multi-valued CTI
bridges (data categories, system impact, MITRE techniques, CVEs, actors, IOCs).
It runs from persisted data only, so the full backfill needs no LLM calls and is
re-runnable (idempotent upsert keyed on the canonical incident id).

The same entry points are used for incremental refresh (per canonical, wired into
the ``refresh_analytics`` task) and for the one-shot ``eduthreat-v2-build-star
--backfill`` CLI over the whole corpus.
"""

from __future__ import annotations

import argparse
import re
from datetime import date, datetime
from typing import Any, Iterable, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.edu_cti_v2 import controlled_vocab as cv
from src.edu_cti_v2.models import (
    BridgeIncidentActor,
    BridgeIncidentCve,
    BridgeIncidentCwe,
    BridgeIncidentDataCategory,
    BridgeIncidentMitreTechnique,
    BridgeIncidentSystemImpact,
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    DimAttackCategory,
    DimAttackVector,
    DimCountry,
    DimCve,
    DimCwe,
    DimDataCategory,
    DimInstitutionType,
    DimMitreTactic,
    DimMitreTechnique,
    DimRansomwareFamily,
    DimSeverity,
    DimSystemImpact,
    DimThreatActor,
    FactIncident,
    IncidentIoc,
)
from src.edu_cti_v2.normalization import (
    normalize_ransomware_family,
    normalize_threat_actor_name,
)

_CVE_RE = re.compile(r"CVE-\d{4}-\d{4,7}", re.IGNORECASE)
_CWE_RE = re.compile(r"CWE-\d{1,5}", re.IGNORECASE)
_IPV4_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_DOMAIN_RE = re.compile(r"^(?:[a-z0-9-]+\.)+[a-z]{2,}$", re.IGNORECASE)
_HASH_LENGTHS = {32: "md5", 40: "sha1", 64: "sha256", 128: "sha512"}

_SEVERITY_RANK = {"critical": 5, "high": 4, "medium": 3, "low": 2, "informational": 1}


# ── small JSONB helpers ───────────────────────────────────────────────────────

def _d(obj: Any, *keys: str) -> Any:
    """Safe nested dict get tolerant of None at any level."""
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = re.sub(r"[^0-9.]", "", value)
        if cleaned and cleaned.count(".") <= 1:
            try:
                return float(cleaned)
            except ValueError:
                return None
    return None


def _to_int(value: Any) -> Optional[int]:
    n = _to_number(value)
    return int(n) if n is not None else None


def _to_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return datetime.strptime(value[: len(fmt) + 2], fmt).date()
            except ValueError:
                continue
    return None


# ── dimension upsert (surrogate-key slug dims) ────────────────────────────────

class _SlugDimCache:
    """Caches slug -> surrogate id per dimension within one run."""

    def __init__(self, session: Session):
        self.session = session
        self._cache: dict[tuple[str, str], Any] = {}

    def get_or_create(self, model, slug: Optional[str], *, label: Optional[str] = None,
                      in_vocab: bool = True, extra: Optional[dict] = None) -> Optional[Any]:
        if not slug:
            return None
        key = (model.__tablename__, slug)
        if key in self._cache:
            return self._cache[key]
        values = {"slug": slug, "label": label or slug.replace("_", " ").title(),
                  "in_vocabulary": in_vocab}
        if extra:
            values.update(extra)
        stmt = pg_insert(model).values(**values).on_conflict_do_nothing(index_elements=["slug"])
        self.session.execute(stmt)
        row_id = self.session.execute(
            select(model.id).where(model.slug == slug)
        ).scalar_one()
        self._cache[key] = row_id
        return row_id


def seed_dimensions(session: Session) -> None:
    """Upsert the controlled-vocabulary dimension members and MITRE tactics."""
    cache = _SlugDimCache(session)
    for slug in sorted(cv.INSTITUTION_TYPES):
        cache.get_or_create(DimInstitutionType, slug)
    for slug in sorted(cv.ATTACK_CATEGORIES):
        cache.get_or_create(DimAttackCategory, slug, extra={"family": _attack_family(slug)})
    for slug in sorted(cv.ATTACK_VECTORS):
        cache.get_or_create(DimAttackVector, slug)
    for slug in sorted(cv.SEVERITIES):
        cache.get_or_create(DimSeverity, slug, extra={"rank": _SEVERITY_RANK.get(slug)})
    for ordinal, slug in enumerate(_MITRE_TACTIC_ORDER, start=1):
        stmt = pg_insert(DimMitreTactic).values(
            slug=slug, name=slug.replace("_", " ").title(), ordinal=ordinal
        ).on_conflict_do_nothing(index_elements=["slug"])
        session.execute(stmt)
    session.flush()


_MITRE_TACTIC_ORDER = [
    "reconnaissance", "resource_development", "initial_access", "execution", "persistence",
    "privilege_escalation", "defense_evasion", "credential_access", "discovery",
    "lateral_movement", "collection", "command_and_control", "exfiltration", "impact",
]


def _attack_family(slug: str) -> Optional[str]:
    for prefix, family in (
        ("ransomware", "ransomware"), ("phishing", "phishing"), ("spear", "phishing"),
        ("data_breach", "data_breach"), ("data_exposure", "data_breach"),
        ("data_leak", "data_breach"), ("ddos", "ddos"), ("malware", "malware"),
        ("supply_chain", "supply_chain"), ("insider", "insider"),
    ):
        if slug.startswith(prefix):
            return family
    return "other"


# ── IOC extraction ────────────────────────────────────────────────────────────

def _classify_ioc(value: str) -> Optional[str]:
    v = value.strip()
    if not v or len(v) > 256:
        return None
    if _IPV4_RE.match(v):
        return "ipv4"
    if _EMAIL_RE.match(v):
        return "email"
    low = v.lower()
    if all(ch in "0123456789abcdef" for ch in low) and len(low) in _HASH_LENGTHS:
        return _HASH_LENGTHS[len(low)]
    if v.startswith(("http://", "https://")):
        return "url"
    if ":" in v and v.count(":") >= 2:
        return "ipv6"
    if _DOMAIN_RE.match(v):
        return "domain"
    return None


def _collect_iocs(projection: dict) -> list[tuple[str, str]]:
    found: dict[tuple[str, str], None] = {}
    candidates: list[str] = []
    # timeline event indicators
    for event in projection.get("timeline") or []:
        inds = event.get("indicators") if isinstance(event, dict) else None
        if isinstance(inds, list):
            candidates.extend(str(x) for x in inds)
        elif isinstance(inds, str):
            candidates.append(inds)
    # any top-level/nested list named like indicators/iocs
    for key in ("iocs", "indicators", "indicators_of_compromise"):
        val = projection.get(key)
        if isinstance(val, list):
            candidates.extend(str(x) for x in val)
    for raw in candidates:
        kind = _classify_ioc(raw)
        if kind:
            found[(kind, raw.strip())] = None
    return list(found.keys())


# ── fact + bridge build for one canonical ────────────────────────────────────

def _replace_bridge(session: Session, model, canonical_id, rows: Iterable[dict]) -> None:
    session.execute(delete(model).where(model.canonical_incident_id == canonical_id))
    rows = list(rows)
    if rows:
        session.execute(pg_insert(model).values(rows).on_conflict_do_nothing())


def build_fact_for_canonical(
    session: Session,
    canonical: CanonicalIncident,
    projection: Optional[dict],
    completeness_score: Optional[float],
    source_count: int,
    cache: Optional[_SlugDimCache] = None,
) -> None:
    cache = cache or _SlugDimCache(session)
    proj = projection or {}
    cid = canonical.id

    # Single-valued dimensions (write-time normalized slugs)
    inst_slug = cv.normalize_institution_type(canonical.institution_type)
    cat_slug = cv.normalize_attack_category(canonical.attack_category)
    vec_slug = cv.normalize_attack_vector(_d(proj, "attack_dynamics", "attack_vector") or canonical.attack_vector)
    sev_slug = cv.normalize_severity(canonical.severity)
    actor_label = normalize_threat_actor_name(canonical.threat_actor_name)
    family_label = normalize_ransomware_family(canonical.ransomware_family)

    inst_id = cache.get_or_create(DimInstitutionType, inst_slug, in_vocab=cv.is_in_vocabulary("institution_type", inst_slug)) if inst_slug else None
    cat_id = cache.get_or_create(DimAttackCategory, cat_slug, in_vocab=cv.is_in_vocabulary("attack_category", cat_slug), extra={"family": _attack_family(cat_slug)} if cat_slug else None) if cat_slug else None
    vec_id = cache.get_or_create(DimAttackVector, vec_slug, in_vocab=cv.is_in_vocabulary("attack_vector", vec_slug)) if vec_slug else None
    sev_id = cache.get_or_create(DimSeverity, sev_slug, in_vocab=cv.is_in_vocabulary("severity", sev_slug), extra={"rank": _SEVERITY_RANK.get(sev_slug)}) if sev_slug else None
    actor_id = cache.get_or_create(DimThreatActor, cv.slugify(actor_label), label=actor_label) if actor_label else None
    family_id = cache.get_or_create(DimRansomwareFamily, cv.slugify(family_label), label=family_label) if family_label else None

    # Country dimension (seed lazily from observed code+name)
    if canonical.country_code:
        session.execute(
            pg_insert(DimCountry)
            .values(country_code=canonical.country_code, name=canonical.country or canonical.country_code)
            .on_conflict_do_nothing(index_elements=["country_code"])
        )

    # Dates and timing
    incident_d = canonical.incident_date
    disclosure_d = canonical.source_published_at.date() if canonical.source_published_at else None
    # The extraction schema names the detection field discovery_date; accept either.
    detection_d = _to_date(proj.get("detection_date") or proj.get("discovery_date"))
    disclosure_lag = (disclosure_d - incident_d).days if (incident_d and disclosure_d and disclosure_d >= incident_d) else None
    dwell = _to_int(proj.get("dwell_time_days"))
    recovery_days = _to_int(_d(proj, "recovery_metrics", "recovery_timeframe_days") or _d(proj, "attack_dynamics", "recovery_timeframe_days"))
    downtime = _to_int(_d(proj, "operational_impact_metrics", "downtime_days"))

    di = proj.get("data_impact") or {}
    ad = proj.get("attack_dynamics") or {}
    oi = proj.get("operational_impact_metrics") or {}
    si = proj.get("system_impact") or {}

    fact_values = dict(
        canonical_incident_id=cid,
        institution_name=canonical.institution_name,
        vendor_name=canonical.vendor_name or _d(si, "vendor_name"),
        region=canonical.region,
        city=canonical.city,
        institution_type_id=inst_id,
        attack_category_id=cat_id,
        attack_vector_id=vec_id,
        severity_id=sev_id,
        primary_actor_id=actor_id,
        ransomware_family_id=family_id,
        country_code=canonical.country_code,
        incident_date=incident_d,
        detection_date=detection_d,
        disclosure_date=disclosure_d,
        incident_year=incident_d.year if incident_d else None,
        incident_quarter=((incident_d.month - 1) // 3 + 1) if incident_d else None,
        dwell_time_days=dwell,
        disclosure_lag_days=disclosure_lag,
        recovery_days=recovery_days,
        downtime_days=downtime,
        mttd_hours=_to_number(_d(proj, "recovery_metrics", "mttd_hours")),
        mttr_hours=_to_number(_d(proj, "recovery_metrics", "mttr_hours")),
        records_affected_exact=_to_int(di.get("records_affected_exact")),
        records_affected_min=_to_int(di.get("records_affected_min")),
        records_affected_max=_to_int(di.get("records_affected_max")),
        ransom_demanded_usd=_to_number(ad.get("ransom_demanded") or ad.get("ransom_amount")),
        ransom_paid_usd=_to_number(ad.get("ransom_paid")),
        data_exfiltrated=_as_bool(di.get("data_exfiltrated")),
        data_encrypted=_as_bool(di.get("data_encrypted")),
        is_vendor_breach=bool(canonical.vendor_name or _d(si, "vendor_name") or si.get("third_party_vendor_impact")),
        teaching_disrupted=_as_bool(oi.get("teaching_disrupted")),
        research_disrupted=_as_bool(oi.get("research_disrupted")),
        attribution_confidence=proj.get("attribution_confidence") or _d(proj, "attack_dynamics", "attribution_confidence"),
        source_reliability=proj.get("source_reliability") or _d(proj, "attack_dynamics", "source_reliability"),
        completeness_score=completeness_score,
        source_count=source_count,
        projection_version="star-v1",
    )

    update_cols = {k: v for k, v in fact_values.items() if k != "canonical_incident_id"}
    session.execute(
        pg_insert(FactIncident).values(**fact_values)
        .on_conflict_do_update(index_elements=["canonical_incident_id"], set_=update_cols)
    )

    # Bridges (delete + reinsert; idempotent)
    data_cats = _collect_data_categories(di)
    _replace_bridge(session, BridgeIncidentDataCategory, cid, [
        {"canonical_incident_id": cid, "data_category_id": cache.get_or_create(DimDataCategory, slug, in_vocab=False, extra={"is_pii": _is_pii(slug)})}
        for slug in data_cats
    ])

    systems = [cv.slugify(s) for s in (si.get("systems_affected") or []) if cv.slugify(s)]
    _replace_bridge(session, BridgeIncidentSystemImpact, cid, [
        {"canonical_incident_id": cid, "system_impact_id": cache.get_or_create(DimSystemImpact, slug, in_vocab=False)}
        for slug in dict.fromkeys(systems)
    ])

    _replace_bridge(session, BridgeIncidentMitreTechnique, cid, _build_mitre_rows(session, cid, proj))
    _replace_bridge(session, BridgeIncidentCve, cid, _build_cve_rows(session, cid, proj))
    _replace_bridge(session, BridgeIncidentCwe, cid, _build_cwe_rows(session, cid, proj))

    if actor_id:
        _replace_bridge(session, BridgeIncidentActor, cid, [
            {"canonical_incident_id": cid, "actor_id": actor_id, "is_primary": True}
        ])
    else:
        session.execute(delete(BridgeIncidentActor).where(BridgeIncidentActor.canonical_incident_id == cid))

    iocs = _collect_iocs(proj)
    session.execute(delete(IncidentIoc).where(IncidentIoc.canonical_incident_id == cid))
    if iocs:
        session.execute(pg_insert(IncidentIoc).values([
            {"canonical_incident_id": cid, "ioc_type": kind, "value": val} for kind, val in iocs
        ]).on_conflict_do_nothing())


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ("true", "yes", "1"):
            return True
        if value.lower() in ("false", "no", "0"):
            return False
    return None


_PII_CATEGORIES = {"student_data", "faculty_data", "alumni_data", "personal_information",
                   "financial_data", "medical_records", "social_security_numbers", "health_data"}


def _is_pii(slug: str) -> bool:
    return slug in _PII_CATEGORIES


def _collect_data_categories(data_impact: dict) -> list[str]:
    cats: dict[str, None] = {}
    for raw in data_impact.get("data_types_affected") or []:
        slug = cv.slugify(raw)
        if slug:
            cats[slug] = None
    # promote boolean-true category flags
    for flag in ("student_data", "faculty_data", "alumni_data", "research_data",
                 "financial_data", "medical_records", "personal_information",
                 "administrative_data", "intellectual_property"):
        if data_impact.get(flag) is True:
            cats[flag] = None
    return list(cats.keys())


def _build_mitre_rows(session: Session, cid, proj: dict) -> list[dict]:
    rows: dict[str, dict] = {}
    for tech in proj.get("mitre_attack_techniques") or []:
        if not isinstance(tech, dict):
            continue
        tid = (tech.get("technique_id") or "").strip().upper()
        if not re.match(r"^T\d{4}(\.\d{3})?$", tid):
            continue
        tactic = cv.normalize_mitre_tactic(tech.get("tactic"))
        session.execute(
            pg_insert(DimMitreTechnique).values(
                technique_id=tid, name=tech.get("technique_name"), tactic_slug=tactic,
                is_sub_technique="." in tid,
                parent_technique_id=tid.split(".")[0] if "." in tid else None,
            ).on_conflict_do_nothing(index_elements=["technique_id"])
        )
        rows[tid] = {"canonical_incident_id": cid, "technique_id": tid, "tactic_slug": tactic}
    return list(rows.values())


def _build_cve_rows(session: Session, cid, proj: dict) -> list[dict]:
    text_blobs: list[str] = []
    # Structured CVE fields (often null in practice).
    for path in (("attack_dynamics", "cve_references"), ("vulnerabilities_exploited",),
                 ("cve_references",)):
        val = _d(proj, *path) if len(path) > 1 else proj.get(path[0])
        if isinstance(val, list):
            text_blobs.extend(str(x) for x in val)
        elif isinstance(val, str):
            text_blobs.append(val)
    # CVEs are most often embedded in free-text fields rather than the structured
    # field, so scan the high-signal narrative fields as well.
    for key in ("enriched_summary", "attack_campaign_name", "initial_access_description"):
        val = proj.get(key)
        if isinstance(val, str):
            text_blobs.append(val)
    for event in proj.get("timeline") or []:
        if isinstance(event, dict):
            text_blobs.append(str(event.get("event_description") or ""))
    for tech in proj.get("mitre_attack_techniques") or []:
        if isinstance(tech, dict):
            text_blobs.append(str(tech.get("description") or ""))
    cves = {m.group(0).upper() for blob in text_blobs for m in _CVE_RE.finditer(blob)}
    rows = []
    for cve in sorted(cves):
        year = int(cve.split("-")[1]) if cve.count("-") >= 2 and cve.split("-")[1].isdigit() else None
        session.execute(pg_insert(DimCve).values(cve_id=cve, year=year).on_conflict_do_nothing(index_elements=["cve_id"]))
        rows.append({"canonical_incident_id": cid, "cve_id": cve})
    return rows


def _build_cwe_rows(session: Session, cid, proj: dict) -> list[dict]:
    blobs: list[str] = []
    val = proj.get("cwe_references")
    if isinstance(val, list):
        blobs.extend(str(x) for x in val)
    elif isinstance(val, str):
        blobs.append(val)
    # CWE is most often carried per-vulnerability in vulnerabilities_exploited.
    for vuln in proj.get("vulnerabilities_exploited") or []:
        if isinstance(vuln, dict) and vuln.get("cwe_id"):
            blobs.append(str(vuln["cwe_id"]))
    cwes = {m.group(0).upper() for blob in blobs for m in _CWE_RE.finditer(blob)}
    rows = []
    for cwe in sorted(cwes):
        session.execute(pg_insert(DimCwe).values(cwe_id=cwe).on_conflict_do_nothing(index_elements=["cwe_id"]))
        rows.append({"canonical_incident_id": cid, "cwe_id": cwe})
    return rows


# ── backfill driver ───────────────────────────────────────────────────────────

def backfill_all(
    session: Session,
    *,
    batch_size: int = 50,
    only_open: bool = True,
    progress: bool = False,
) -> dict[str, int]:
    seed_dimensions(session)
    session.commit()
    cache = _SlugDimCache(session)

    member_counts = dict(session.execute(
        select(CanonicalMembership.canonical_incident_id, func.count())
        .group_by(CanonicalMembership.canonical_incident_id)
    ).all())

    q = select(CanonicalIncident, CanonicalEnrichment.canonical_projection,
               CanonicalEnrichment.completeness_score).outerjoin(
        CanonicalEnrichment, CanonicalEnrichment.canonical_incident_id == CanonicalIncident.id)
    if only_open:
        q = q.where(CanonicalIncident.status == "open")

    pruned = 0
    if only_open:
        # Prune orphaned facts for canonicals that are no longer open (merged / folded
        # during dedup, superseded, excluded). build_fact_for_canonical only rebuilds the
        # canonicals it processes, so without this their stale fact rows linger and inflate
        # the export/analytics row count above the true open-canonical total.
        open_ids = select(CanonicalIncident.id).where(CanonicalIncident.status == "open")
        for _model in (FactIncident, BridgeIncidentActor, BridgeIncidentMitreTechnique,
                       BridgeIncidentCve, BridgeIncidentDataCategory, IncidentIoc):
            res = session.execute(delete(_model).where(_model.canonical_incident_id.not_in(open_ids)))
            if _model is FactIncident:
                pruned = res.rowcount or 0
        session.commit()

    processed = 0
    for canonical, projection, completeness in session.execute(q).all():
        build_fact_for_canonical(
            session, canonical, projection,
            float(completeness) if completeness is not None else None,
            int(member_counts.get(canonical.id, 1)), cache=cache,
        )
        processed += 1
        if processed % batch_size == 0:
            session.commit()
            if progress:
                print(f"  ... {processed} facts built", flush=True)
    session.commit()
    return {"facts_built": processed, "stale_facts_pruned": pruned}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the EduThreat star-schema analytical layer")
    parser.add_argument("--backfill", action="store_true", help="rebuild all facts from canonical projections")
    parser.add_argument("--all-statuses", action="store_true", help="include non-open canonicals")
    args = parser.parse_args()

    from src.edu_cti_v2.db import create_session_factory
    from src.edu_cti_v2.db.config import V2DatabaseSettings

    settings = V2DatabaseSettings.from_env()
    session_factory = create_session_factory(settings)
    with session_factory() as session:
        result = backfill_all(session, only_open=not args.all_statuses, progress=True)
    print(f"star backfill complete: {result}")


if __name__ == "__main__":
    main()
