"""Read-only campaign correlation workflow for EduThreat-CTI.

This module builds candidate campaign groupings above canonical incidents. It
does not mutate the production database: canonical incidents remain
victim/event-level records and campaign membership is exported as reviewable
analysis artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row


DEFAULT_OUTPUT_DIR = Path("paper/EDU_Attack/analysis/outputs/campaign")

DATE_WINDOW_DEFAULT_DAYS = 120
VENDOR_NAME_WINDOW_DAYS = 180
EDGE_THRESHOLD = 0.55


@dataclass(frozen=True)
class PlatformIndicator:
    """A high-signal shared system, vendor, or exploitation surface."""

    key: str
    vendor: str
    platform: str
    aliases: tuple[str, ...]
    campaign_type: str = "shared_vendor_incident"
    date_window_days: int = DATE_WINDOW_DEFAULT_DAYS


# ── The vendor/platform canonical registry ──────────────────────────────────
# THIS is the single place to fix vendor/platform confusion. Each entry maps a
# real supply-chain product to its canonical (vendor=company, platform=product)
# pair plus the text/vendor_name aliases that should resolve to it. A campaign's
# vendor/platform lists are normalised through this registry (see
# _canonicalize_vendors_platforms), so e.g. "Canvas" / "Canvas (Instructure)"
# collapse to vendor "Instructure" + platform "Canvas". To fix a NEW case: hit
# GET /api/admin/v2/data-quality/unrecognized-vendors to see vendor strings that
# match nothing here, then add one PlatformIndicator line below. Do NOT add
# downstream affected orgs (National Student Clearinghouse, TIAA, CDW, ...) —
# they are victims, not platforms, and must remain plain vendor strings.
PLATFORM_INDICATORS: tuple[PlatformIndicator, ...] = (
    PlatformIndicator(
        key="instructure_canvas",
        vendor="Instructure",
        platform="Canvas",
        aliases=(
            "instructure",
            "canvas learning management",
            "canvas lms",
            "canvas platform",
            "canvas access",
        ),
        campaign_type="shared_vendor_incident",
        date_window_days=75,
    ),
    PlatformIndicator(
        key="powerschool",
        vendor="PowerSchool",
        platform="PowerSchool",
        aliases=("powerschool", "powerschool sis", "powerschool student information system"),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="moveit",
        vendor="Progress Software",
        platform="MOVEit",
        aliases=("moveit", "progress moveit", "moveit transfer", "progress", "progress software"),
        campaign_type="mass_exploitation",
        date_window_days=420,
    ),
    PlatformIndicator(
        key="snowflake",
        vendor="Snowflake",
        platform="Snowflake",
        aliases=("snowflake",),
        campaign_type="mass_exploitation",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="blackbaud",
        vendor="Blackbaud",
        platform="Blackbaud",
        aliases=("blackbaud",),
        campaign_type="shared_vendor_incident",
        date_window_days=420,
    ),
    PlatformIndicator(
        key="illuminate_education",
        vendor="Illuminate Education",
        platform="Illuminate Education",
        aliases=("illuminate education",),
        campaign_type="shared_vendor_incident",
        date_window_days=420,
    ),
    PlatformIndicator(
        key="finalsite",
        vendor="Finalsite",
        platform="Finalsite",
        aliases=("finalsite",),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="ellucian_banner",
        vendor="Ellucian",
        platform="Banner",
        aliases=("ellucian", "banner student", "ellucian banner"),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="microsoft_365",
        vendor="Microsoft",
        platform="Microsoft 365",
        aliases=("microsoft 365", "office 365", "exchange online"),
        campaign_type="actor_activity_wave",
        date_window_days=90,
    ),
    # ── Supply-chain vendors/platforms observed in the corpus ────────────────
    PlatformIndicator(
        key="gti_careerconnect",
        vendor="Group GTI",
        platform="CareerConnect",
        aliases=("careerconnect", "career connect", "group gti", "gti"),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="accellion_fta",
        vendor="Accellion",
        platform="Accellion FTA",
        aliases=(
            "accellion",
            "accellion fta",
            "file transfer appliance",
            "fta",
            "kiteworks",
        ),
        campaign_type="mass_exploitation",
        date_window_days=420,
    ),
    PlatformIndicator(
        key="oracle_ebs",
        vendor="Oracle",
        platform="E-Business Suite",
        aliases=(
            "oracle e business suite",
            "oracle ebs",
            "e business suite",
            "ebs",
        ),
        campaign_type="mass_exploitation",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="cleo_mft",
        vendor="Cleo",
        platform="Cleo MFT",
        aliases=("cleo", "cleo harmony", "cleo vltrader", "cleo lexicom"),
        campaign_type="mass_exploitation",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="pearson_aimsweb",
        vendor="Pearson",
        platform="AIMSweb",
        aliases=("aimsweb", "aimsweb plus"),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="powerschool_naviance",
        vendor="PowerSchool",
        platform="Naviance",
        aliases=("naviance",),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="follett",
        vendor="Follett",
        platform="Aspen",
        aliases=("follett", "follett aspen", "follett destiny", "aspen sis", "destiny"),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="anthology_blackboard",
        vendor="Anthology",
        platform="Blackboard",
        aliases=("blackboard", "blackboard learn", "anthology"),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="verkada",
        vendor="Verkada",
        platform="Verkada",
        aliases=("verkada",),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="mobile_guardian",
        vendor="Mobile Guardian",
        platform="Mobile Guardian",
        aliases=("mobile guardian",),
        campaign_type="shared_vendor_incident",
        date_window_days=180,
    ),
    PlatformIndicator(
        key="zoom",
        vendor="Zoom",
        platform="Zoom",
        aliases=("zoom video", "zoom communications"),
        campaign_type="shared_vendor_incident",
        date_window_days=90,
    ),
)

PLATFORM_BY_KEY = {indicator.key: indicator for indicator in PLATFORM_INDICATORS}


ACTOR_ALIASES: dict[str, tuple[str, ...]] = {
    "ShinyHunters": ("shinyhunters", "shiny hunters"),
    "Cl0p": ("cl0p", "clop"),
    "LockBit": ("lockbit",),
    "Akira": ("akira",),
    "Vice Society": ("vice society",),
    "Rhysida": ("rhysida",),
    "Medusa": ("medusa",),
    "BlackCat/ALPHV": ("blackcat", "alphv", "blackcat alphv"),
    "BlackSuit": ("blacksuit", "black suit"),
    "Hive": ("hive ransomware",),
    "NetWalker": ("netwalker",),
    "RansomHub": ("ransomhub",),
    "Qilin": ("qilin",),
    "BianLian": ("bianlian", "bian lian"),
    "Play": ("play ransomware", "play group"),
}

# Generic, non-attributive actor labels dropped from campaign actor lists. Mirrors
# `_UNKNOWN_THREAT_ACTOR_VALUES` / `_GENERIC_ACTOR_SUBSTRINGS` in
# `src/edu_cti_v2/normalization.py` (kept as a local copy to avoid an
# edu_cti -> edu_cti_v2 reverse import). Keep the two in sync.
GENERIC_ACTOR_VALUES = {
    "hacking",
    "unauthorized actor",
    "unauthorised actor",
    "unknown actor",
    "unknown criminal actors",
    "cybercriminals",
    "cybercriminal",
    "cyber criminal",
    "cyber criminals",
    "criminal",
    "criminals",
    "attacker",
    "attackers",
    "threat actors",
    "threat actor",
    "cyber extortion",
    "cyber extortionist",
    "cyber extortionists",
    "extortion",
    "extortion group",
    "extortion gang",
    "extortionist",
    "extortionists",
    "ransomware group",
    "ransomware gang",
    "ransomware operator",
    "ransomware operators",
    "unidentified",
    "unidentified actor",
    "unidentified actors",
    "unnamed",
    "unnamed actor",
}

# Generic word tokens stripped when deciding whether an actor label is purely
# descriptive. After removing these, a label with NOTHING left is generic; one with a
# real name surviving ("Clop" in "Clop cybercriminal") is kept. Mirrors
# `_GENERIC_ACTOR_TOKENS` in `src/edu_cti_v2/normalization.py`.
_GENERIC_ACTOR_TOKENS = {
    "ransomware", "ransom", "extortion", "gang", "group", "operation", "operations",
    "operator", "operators", "collective", "crew", "hacker", "hackers", "hacking",
    "affiliate", "affiliates", "criminal", "criminals", "cybercriminal", "cybercriminals",
    "cybercrime", "cybercrimes", "syndicate", "actor", "actors", "cyber", "cyberattack",
    "threat", "malicious", "unknown", "unidentified", "unnamed", "suspected", "foreign",
    "pro", "unauthorized", "unauthorised", "attacker", "attackers",
    "china", "chinese", "iran", "iranian", "north", "korea", "korean", "russia",
    "russian", "state", "backed",
}


GENERIC_NEGATIVE_TERMS = (
    "best practice",
    "best practices",
    "state of cybersecurity",
    "wake-up call",
    "trend report",
    "annual report",
    "how to",
    "tips for",
)


SUPPLY_CHAIN_CATEGORIES = {
    "third_party_compromise",
    "supply_chain_software",
    "software_supply_chain",
}

GENERIC_FEATURE_VALUES = {
    "other",
    "unknown",
    "email_system",
    "student_portal",
    "cloud_services",
    "network_infrastructure",
    "file_servers",
    "backup_systems",
    "payroll_system",
    "financial_systems",
    "hospital_systems",
    "web_servers",
    "learning_management_system",
}


@dataclass
class CampaignEvidenceItem:
    evidence_item_id: str
    canonical_incident_id: str
    canonical_status: str
    source_incident_id: str | None
    article_document_id: str | None
    victim_name: str | None
    institution_type: str | None
    country: str | None
    country_code: str | None
    incident_date: str | None
    publication_date: str | None
    source_name: str | None
    source_group: str | None
    source_title: str | None
    article_title: str | None
    source_url: str | None
    attack_category: str | None
    attack_vector: str | None
    threat_actor: str | None
    ransomware_family: str | None
    vendors: list[str] = field(default_factory=list)
    platforms: list[str] = field(default_factory=list)
    affected_systems: list[str] = field(default_factory=list)
    platform_keys: list[str] = field(default_factory=list)
    actors: list[str] = field(default_factory=list)
    cves: list[str] = field(default_factory=list)
    campaign_names: list[str] = field(default_factory=list)
    mitre_tactics: list[str] = field(default_factory=list)
    records_affected_exact: int | None = None
    records_affected_min: int | None = None
    records_affected_max: int | None = None
    data_categories: list[str] = field(default_factory=list)
    evidence_quotes: list[str] = field(default_factory=list)
    negative_flags: list[str] = field(default_factory=list)
    manual_review_required: bool = False
    manual_review_reason: str | None = None
    content_hash: str | None = None


@dataclass
class CampaignProfile:
    canonical_incident_id: str
    canonical_status: str
    victim_name: str | None
    institution_type: str | None
    country: str | None
    country_code: str | None
    representative_date: str | None
    attack_categories: set[str] = field(default_factory=set)
    attack_vectors: set[str] = field(default_factory=set)
    vendors: set[str] = field(default_factory=set)
    platforms: set[str] = field(default_factory=set)
    affected_systems: set[str] = field(default_factory=set)
    platform_keys: set[str] = field(default_factory=set)
    actors: set[str] = field(default_factory=set)
    ransomware_families: set[str] = field(default_factory=set)
    cves: set[str] = field(default_factory=set)
    campaign_names: set[str] = field(default_factory=set)
    mitre_tactics: set[str] = field(default_factory=set)
    article_hashes: set[str] = field(default_factory=set)
    article_titles: set[str] = field(default_factory=set)
    source_incident_ids: set[str] = field(default_factory=set)
    article_document_ids: set[str] = field(default_factory=set)
    evidence_quotes: list[str] = field(default_factory=list)
    manual_review_required: bool = False
    manual_review_reasons: set[str] = field(default_factory=set)
    evidence_items: list[CampaignEvidenceItem] = field(default_factory=list)


@dataclass
class CampaignEdge:
    from_canonical_incident_id: str
    to_canonical_incident_id: str
    confidence: float
    reasons: list[str]
    shared_vendors: list[str]
    shared_platforms: list[str]
    shared_platform_keys: list[str]
    shared_actors: list[str]
    shared_cves: list[str]
    shared_campaign_names: list[str]
    shared_article_hashes: list[str]
    date_gap_days: int | None


@dataclass
class CampaignCandidate:
    campaign_id: str
    campaign_name: str
    campaign_type: str
    first_seen_date: str | None
    last_seen_date: str | None
    actors: list[str]
    vendors: list[str]
    platforms: list[str]
    cves: list[str]
    campaign_names: list[str]
    attack_categories: list[str]
    member_count: int
    confirmed_member_count: int
    evidence_only_member_count: int
    confidence: float
    analyst_summary: str
    # Campaign-family grouping: fragments of one real campaign (e.g. the actor
    # "wave" view and the CVE "exposure" view of the same event, or two split
    # platform components) share a family_id so the UI can present them as one
    # related family instead of duplicates. Members are NOT merged.
    family_id: str | None = None
    related_campaign_ids: list[str] = field(default_factory=list)
    is_primary_in_family: bool = False


@dataclass
class CampaignMembership:
    campaign_id: str
    canonical_incident_id: str
    role: str
    confidence: float
    evidence_article_ids: list[str]
    evidence_source_incident_ids: list[str]
    evidence_quotes: list[str]
    review_status: str
    victim_name: str | None
    canonical_status: str


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value).strip() or None


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                return [stripped]
            return parsed if isinstance(parsed, list) else [parsed]
        return [stripped]
    return [value]


def _dedupe(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _as_text(value)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _dedupe_non_generic(values: Iterable[Any]) -> list[str]:
    return [
        value
        for value in _dedupe(values)
        if _normalize_for_match(value) not in GENERIC_FEATURE_VALUES
    ]


def _json_get(payload: Mapping[str, Any] | None, *path: str) -> Any:
    current: Any = payload or {}
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _coalesce_json(payload: Mapping[str, Any] | None, *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _json_get(payload, *path)
        if value is not None:
            return value
    return None


def _normalize_for_match(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _build_vendor_name_to_indicator() -> dict[str, str]:
    """Map a normalized ``vendor_name`` (the structured field, not free text) to a
    platform indicator key. This lets a canonical incident that *carries*
    ``vendor_name = Instructure`` fan out to the shared-vendor campaign even when
    its article body never literally says "Instructure"/"Canvas" — the text-token
    path (``_extract_platform_indicators``) alone misses those."""

    # First indicator to claim a normalized name keeps it (``setdefault``), so a
    # secondary product that re-uses a primary vendor's company name — PowerSchool
    # appears as both the SIS (``powerschool``) and Naviance (``powerschool_naviance``)
    # indicators — does not clobber the bare-company → primary mapping. Order the
    # registry with the primary platform first for any shared vendor.
    mapping: dict[str, str] = {}
    for indicator in PLATFORM_INDICATORS:
        mapping.setdefault(_normalize_for_match(indicator.vendor), indicator.key)
        mapping.setdefault(_normalize_for_match(indicator.platform), indicator.key)
        for alias in indicator.aliases:
            mapping.setdefault(_normalize_for_match(alias), indicator.key)
    mapping.pop("", None)
    return mapping


VENDOR_NAME_TO_INDICATOR = _build_vendor_name_to_indicator()


def _indicator_key_for_vendor(value: str) -> str | None:
    """Match a (possibly messy) vendor string to a platform indicator.

    Tries the whole normalized string, then each parenthesis/slash/comma-separated
    part, so product-name-as-vendor strings the LLM produces — ``"Canvas"`` or
    ``"Canvas (Instructure)"`` — resolve to the Instructure/Canvas indicator."""
    norm = _normalize_for_match(value)
    if norm in VENDOR_NAME_TO_INDICATOR:
        return VENDOR_NAME_TO_INDICATOR[norm]
    for part in re.split(r"[()/,]", value or ""):
        pn = _normalize_for_match(part)
        if pn and pn in VENDOR_NAME_TO_INDICATOR:
            return VENDOR_NAME_TO_INDICATOR[pn]
    return None


def _split_vendor_entities(value: str) -> list[str]:
    """Split a raw vendor string into individual entities on top-level commas.

    ``vendor_name`` is built by comma-joining the LLM's ``third_parties_involved``
    list (``json_to_schema_mapper.py``), so a single field routinely mixes the
    breached vendor with downstream victims — ``"MOVEit (Progress), NSC, TIAA"``.
    Splitting first lets each entity be canonicalised (or kept) on its own, instead
    of one product match (``moveit``) swallowing the whole string and dropping NSC /
    TIAA. Commas *inside* parentheses are not split points (``"Acme (a, b) Corp"``
    stays one entity)."""
    if not value:
        return []
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in value:
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    parts.append("".join(buf).strip())
    return [p for p in parts if p]


def _strip_descriptive_parenthetical(entity: str) -> str:
    """Drop a trailing ``(...)`` that merely *describes* an unknown vendor.

    ``"SureFire (cybersecurity contractor)"`` → ``"SureFire"``. Only applied to
    entities that did not resolve to a registry indicator, and never when the
    parenthetical names a *known* company (so ``"Foo (Instructure)"`` is left for
    the registry to handle rather than mangled)."""
    match = re.search(r"\s*\(([^()]*)\)\s*$", entity)
    if not match:
        return entity.strip()
    inner = match.group(1)
    if _normalize_for_match(inner) in VENDOR_NAME_TO_INDICATOR:
        return entity.strip()
    return entity[: match.start()].strip()


def _canonicalize_vendors_platforms(
    vendors: Sequence[str], platforms: Sequence[str]
) -> tuple[list[str], list[str]]:
    """Normalise a campaign's vendor/platform lists through the indicator registry.

    Each raw vendor string is first **split** into individual entities (a single
    field often comma-joins the breached vendor with downstream victims), then each
    entity is canonicalised independently: a known platform's *product* name (e.g.
    Canvas) or a parenthetical alias (``Canvas (Instructure)``) collapses to the
    canonical company (Instructure) in ``vendors`` and the product (Canvas) in
    ``platforms`` — so a campaign no longer lists Canvas / Canvas (Instructure) /
    Instructure as three vendors, and ``"MOVEit (Progress), NSC, TIAA"`` becomes
    ``Progress Software`` (vendor) + ``MOVEit`` (platform) while NSC / TIAA survive
    as their own plain vendor strings. Unknown vendors are kept verbatim apart from
    a stripped *descriptive* parenthetical."""
    out_vendors: list[str] = []
    out_platforms: list[str] = list(platforms)
    for raw in vendors:
        for entity in _split_vendor_entities(raw):
            key = _indicator_key_for_vendor(entity)
            if key is not None:
                indicator = PLATFORM_BY_KEY[key]
                out_vendors.append(indicator.vendor)
                out_platforms.append(indicator.platform)
            else:
                out_vendors.append(_strip_descriptive_parenthetical(entity))
    return _dedupe(out_vendors), _dedupe(out_platforms)


def _parse_date(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _date_gap_days(left: str | None, right: str | None) -> int | None:
    left_date = _parse_date(left)
    right_date = _parse_date(right)
    if left_date is None or right_date is None:
        return None
    return abs((left_date - right_date).days)


def _date_score(gap_days: int | None) -> float:
    if gap_days is None:
        return 0.0
    if gap_days <= 14:
        return 0.25
    if gap_days <= 45:
        return 0.20
    if gap_days <= 120:
        return 0.15
    if gap_days <= 365:
        return 0.08
    return 0.0


_CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.I)


def _normalize_cve(raw: str | None) -> str | None:
    """Return a canonical ``CVE-YYYY-NNNN..`` id, or None if malformed.

    Enforces the canonical shape and a sane year so broken ids that leak from
    free text / structured LLM fields don't pollute a campaign's CVE list.
    Format-valid but spurious ids (e.g. a one-off concatenation) are filtered
    separately by the >=2-member consensus rule in ``build_campaign_outputs``.
    """
    if not raw:
        return None
    text = str(raw).strip().upper()
    match = re.fullmatch(r"CVE-(\d{4})-(\d{4,7})", text)
    if not match:
        return None
    year = int(match.group(1))
    if year < 1999 or year > date.today().year + 1:
        return None
    return text


def _extract_cves(text: str) -> list[str]:
    return _dedupe(cve for cve in (_normalize_cve(m) for m in _CVE_RE.findall(text)) if cve)


def _extract_platform_indicators(text: str) -> tuple[list[str], list[str], list[str]]:
    normalized = _normalize_for_match(text)
    vendors: list[str] = []
    platforms: list[str] = []
    keys: list[str] = []
    for indicator in PLATFORM_INDICATORS:
        for alias in indicator.aliases:
            if re.search(rf"\b{re.escape(_normalize_for_match(alias))}\b", normalized):
                vendors.append(indicator.vendor)
                platforms.append(indicator.platform)
                keys.append(indicator.key)
                break
    return _dedupe(vendors), _dedupe(platforms), _dedupe(keys)


_KNOWN_ACTOR_NORMALIZED = (
    {_normalize_for_match(name) for name in ACTOR_ALIASES}
    | {_normalize_for_match(alias) for aliases in ACTOR_ALIASES.values() for alias in aliases}
) - {""}


def _is_generic_actor(value: str) -> bool:
    """Generic (non-attributive) actor label — dropped from campaign actor lists.

    A known actor (a canonical `ACTOR_ALIASES` name) is never generic. Otherwise the
    label is generic if it matches an enumerated junk value or, for an unattributed
    label, contains a generic marker substring (so "russian cyber-extortion group" is
    dropped even though its exact form isn't enumerated)."""
    norm = _normalize_for_match(value)
    if not norm:
        return True
    if norm in _KNOWN_ACTOR_NORMALIZED:
        return False
    if norm in GENERIC_ACTOR_VALUES:
        return True
    core = [t for t in norm.split() if t not in _GENERIC_ACTOR_TOKENS]
    return not core


def _extract_actors(text: str, explicit_values: Sequence[Any]) -> list[str]:
    actors = list(explicit_values)
    normalized = _normalize_for_match(text)
    for actor, aliases in ACTOR_ALIASES.items():
        for alias in aliases:
            if re.search(rf"\b{re.escape(_normalize_for_match(alias))}\b", normalized):
                actors.append(actor)
                break
    return [actor for actor in _dedupe(actors) if not _is_generic_actor(actor)]


def _extract_negative_flags(text: str) -> list[str]:
    normalized = _normalize_for_match(text)
    return [
        term
        for term in GENERIC_NEGATIVE_TERMS
        if re.search(rf"\b{re.escape(_normalize_for_match(term))}\b", normalized)
    ]


def _split_sentences(text: str) -> list[str]:
    compact = re.sub(r"\s+", " ", text or "").strip()
    if not compact:
        return []
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", compact) if part.strip()]


def _evidence_quotes(text: str, terms: Sequence[str], *, limit: int = 3) -> list[str]:
    if not text:
        return []
    normalized_terms = [_normalize_for_match(term) for term in terms if term]
    quotes: list[str] = []
    for sentence in _split_sentences(text):
        sentence_norm = _normalize_for_match(sentence)
        if any(term and term in sentence_norm for term in normalized_terms):
            quotes.append(sentence[:360])
        if len(quotes) >= limit:
            break
    return _dedupe(quotes)


def _mitre_tactics_from_projection(projection: Mapping[str, Any] | None) -> list[str]:
    tactics: list[str] = []
    for technique in _as_list(_json_get(projection, "mitre_attack_techniques")):
        if isinstance(technique, Mapping):
            tactics.extend(_as_list(technique.get("tactic")))
            tactics.extend(_as_list(technique.get("tactics")))
    return _dedupe(tactics)


def _data_categories_from_projection(projection: Mapping[str, Any] | None) -> list[str]:
    return _dedupe(
        _as_list(_coalesce_json(projection, ("data_impact", "data_categories"), ("data_categories",)))
    )


def _source_title_is_supported(row: Mapping[str, Any]) -> bool:
    """Avoid stale search/RSS titles driving campaign edges by themselves."""

    raw_title = _normalize_for_match(_as_text(row.get("raw_title")))
    article_title = _normalize_for_match(_as_text(row.get("article_title")))
    if not raw_title:
        return False
    if not article_title:
        return not _as_text(row.get("content_text"))
    if raw_title == article_title:
        return True
    return bool(raw_title and article_title and (raw_title in article_title or article_title in raw_title))


def _row_text(row: Mapping[str, Any], *, include_content: bool = True) -> str:
    parts = [
        row.get("raw_title") if _source_title_is_supported(row) else None,
        row.get("raw_notes"),
        row.get("article_title"),
    ]
    if include_content:
        parts.append(row.get("content_text"))
    return " ".join(str(part) for part in parts if part)


def build_evidence_items(rows: Iterable[Mapping[str, Any]]) -> list[CampaignEvidenceItem]:
    """Build normalized evidence items from canonical/source/article rows."""

    items: list[CampaignEvidenceItem] = []
    for index, row in enumerate(rows, start=1):
        canonical_id = _as_text(row.get("canonical_incident_id"))
        if not canonical_id:
            continue
        projection = row.get("canonical_projection") or {}
        text = _row_text(row)
        explicit_vendor = _coalesce_json(
            projection,
            ("system_impact", "vendor_name"),
            ("vendor_name",),
        )
        explicit_affected_systems = _coalesce_json(
            projection,
            ("system_impact", "systems_affected"),
            ("systems_affected",),
        )
        vendors, platforms, platform_keys = _extract_platform_indicators(text)
        # Seed an indicator from the *structured* vendor_name field too, so an
        # incident that carries vendor_name = Instructure (a known platform
        # vendor) fans out to the shared-vendor campaign even if its article body
        # never names the vendor. This is what links third-party victim records.
        for vendor_value in (row.get("vendor_name"), explicit_vendor):
            mapped_key = VENDOR_NAME_TO_INDICATOR.get(_normalize_for_match(_as_text(vendor_value)))
            if mapped_key:
                indicator = PLATFORM_BY_KEY[mapped_key]
                platform_keys = _dedupe([*platform_keys, indicator.key])
                platforms = _dedupe([*platforms, indicator.platform])
                vendors = _dedupe([*vendors, indicator.vendor])
        vendors = _dedupe_non_generic([row.get("vendor_name"), explicit_vendor, *vendors])
        platforms = _dedupe_non_generic(platforms)
        affected_systems = _dedupe_non_generic(_as_list(explicit_affected_systems))

        explicit_actors = [
            row.get("threat_actor_name"),
            _json_get(projection, "threat_actor"),
            *_as_list(_json_get(projection, "threat_actor_aliases")),
        ]
        actors = _extract_actors(text, explicit_actors)
        cves = _extract_cves(text)
        quote_terms = [*vendors, *platforms, *actors, *cves]
        quotes = _evidence_quotes(text, quote_terms)

        article_id = _as_text(row.get("article_document_id"))
        source_id = _as_text(row.get("source_incident_id"))
        evidence_key = f"{canonical_id}:{source_id or 'source'}:{article_id or index}"
        evidence_id = hashlib.sha1(evidence_key.encode("utf-8")).hexdigest()[:16]

        item = CampaignEvidenceItem(
            evidence_item_id=evidence_id,
            canonical_incident_id=canonical_id,
            canonical_status=_as_text(row.get("canonical_status")) or "unknown",
            source_incident_id=source_id,
            article_document_id=article_id,
            victim_name=_as_text(row.get("institution_name") or row.get("vendor_name")),
            institution_type=_as_text(row.get("institution_type")),
            country=_as_text(row.get("country")),
            country_code=_as_text(row.get("country_code")),
            incident_date=_as_text(row.get("incident_date")),
            publication_date=_as_text(row.get("article_publish_date") or row.get("source_published_at")),
            source_name=_as_text(row.get("source_name")),
            source_group=_as_text(row.get("source_group")),
            source_title=_as_text(row.get("raw_title")),
            article_title=_as_text(row.get("article_title")),
            source_url=_as_text(row.get("article_url")),
            attack_category=_as_text(row.get("attack_category")),
            attack_vector=_as_text(row.get("attack_vector")),
            threat_actor=_as_text(row.get("threat_actor_name")),
            ransomware_family=_as_text(row.get("ransomware_family")),
            vendors=vendors,
            platforms=platforms,
            affected_systems=affected_systems,
            platform_keys=platform_keys,
            actors=actors,
            cves=cves,
            campaign_names=_dedupe([_json_get(projection, "attack_campaign_name")]),
            mitre_tactics=_mitre_tactics_from_projection(projection),
            records_affected_exact=_as_int(
                _coalesce_json(
                    projection,
                    ("data_impact", "records_affected_exact"),
                    ("records_affected_exact",),
                )
            ),
            records_affected_min=_as_int(
                _coalesce_json(
                    projection,
                    ("data_impact", "records_affected_min"),
                    ("records_affected_min",),
                )
            ),
            records_affected_max=_as_int(
                _coalesce_json(
                    projection,
                    ("data_impact", "records_affected_max"),
                    ("records_affected_max",),
                )
            ),
            data_categories=_data_categories_from_projection(projection),
            evidence_quotes=quotes,
            negative_flags=_extract_negative_flags(text),
            manual_review_required=bool(row.get("manual_review_required")),
            manual_review_reason=_as_text(row.get("manual_review_reason")),
            content_hash=_as_text(row.get("content_hash")),
        )
        items.append(item)
    return items


def build_profiles(items: Iterable[CampaignEvidenceItem]) -> dict[str, CampaignProfile]:
    profiles: dict[str, CampaignProfile] = {}
    for item in items:
        profile = profiles.get(item.canonical_incident_id)
        if profile is None:
            profile = CampaignProfile(
                canonical_incident_id=item.canonical_incident_id,
                canonical_status=item.canonical_status,
                victim_name=item.victim_name,
                institution_type=item.institution_type,
                country=item.country,
                country_code=item.country_code,
                representative_date=item.incident_date or item.publication_date,
            )
            profiles[item.canonical_incident_id] = profile

        profile.evidence_items.append(item)
        profile.attack_categories.update(_dedupe([item.attack_category]))
        profile.attack_vectors.update(_dedupe([item.attack_vector]))
        profile.vendors.update(item.vendors)
        profile.platforms.update(item.platforms)
        profile.affected_systems.update(item.affected_systems)
        profile.platform_keys.update(item.platform_keys)
        profile.actors.update(item.actors)
        profile.ransomware_families.update(_dedupe([item.ransomware_family]))
        profile.cves.update(item.cves)
        profile.campaign_names.update(item.campaign_names)
        profile.mitre_tactics.update(item.mitre_tactics)
        profile.article_hashes.update(_dedupe([item.content_hash]))
        profile.article_titles.update(_dedupe([item.article_title]))
        profile.source_incident_ids.update(_dedupe([item.source_incident_id]))
        profile.article_document_ids.update(_dedupe([item.article_document_id]))
        profile.evidence_quotes = _dedupe([*profile.evidence_quotes, *item.evidence_quotes])[:5]
        profile.manual_review_required = profile.manual_review_required or item.manual_review_required
        if item.manual_review_reason:
            profile.manual_review_reasons.add(item.manual_review_reason)
        if profile.representative_date is None:
            profile.representative_date = item.incident_date or item.publication_date
    return profiles


def _indicator_window_days(platform_keys: Iterable[str]) -> int:
    windows = [
        indicator.date_window_days
        for key in platform_keys
        for indicator in PLATFORM_INDICATORS
        if indicator.key == key
    ]
    return max(windows) if windows else DATE_WINDOW_DEFAULT_DAYS


def build_candidate_edges(profiles: Mapping[str, CampaignProfile]) -> list[CampaignEdge]:
    """Build deterministic candidate campaign edges between canonical incidents."""

    edges: list[CampaignEdge] = []
    for left, right in combinations(profiles.values(), 2):
        shared_platform_keys = sorted(left.platform_keys & right.platform_keys)
        shared_platforms = sorted(left.platforms & right.platforms)
        shared_vendors = sorted(left.vendors & right.vendors)
        shared_actors = sorted(left.actors & right.actors)
        shared_cves = sorted(left.cves & right.cves)
        shared_campaign_names = sorted(left.campaign_names & right.campaign_names)
        shared_hashes = sorted(left.article_hashes & right.article_hashes)
        date_gap = _date_gap_days(left.representative_date, right.representative_date)
        date_component = _date_score(date_gap)

        score = 0.0
        reasons: list[str] = []

        if shared_hashes:
            score += 0.90
            reasons.append("same_selected_article_content")

        if shared_campaign_names:
            score += 0.85
            reasons.append("shared_campaign_name")

        if shared_cves:
            score += 0.55 + date_component
            reasons.append("shared_cve")

        if shared_platform_keys:
            window_days = _indicator_window_days(shared_platform_keys)
            if date_gap is None or date_gap <= window_days:
                score += 0.45 + date_component
                reasons.append("shared_vendor_or_platform")
        elif shared_vendors:
            # Generic shared-vendor fan-out for named vendors that don't have a
            # predefined platform indicator: two incidents naming the same
            # (non-generic) vendor within a vendor-sized window are very likely
            # the same upstream supply-chain event.
            if date_gap is None or date_gap <= VENDOR_NAME_WINDOW_DAYS:
                score += 0.45 + date_component
                reasons.append("shared_vendor_name")

        if shared_actors and date_component >= 0.15:
            score += 0.35 + date_component
            reasons.append("shared_actor_time_window")

        if left.ransomware_families & right.ransomware_families and date_component >= 0.15:
            score += 0.25 + date_component
            reasons.append("shared_ransomware_family_time_window")

        if left.attack_categories & right.attack_categories:
            shared_categories = left.attack_categories & right.attack_categories
            if shared_categories & SUPPLY_CHAIN_CATEGORIES and shared_platform_keys:
                score += 0.12
                reasons.append("shared_supply_chain_category")
            elif shared_actors or shared_cves:
                score += 0.08
                reasons.append("shared_attack_category")

        if score < EDGE_THRESHOLD:
            continue

        edges.append(
            CampaignEdge(
                from_canonical_incident_id=left.canonical_incident_id,
                to_canonical_incident_id=right.canonical_incident_id,
                confidence=round(min(score, 1.0), 3),
                reasons=_dedupe(reasons),
                shared_vendors=shared_vendors,
                shared_platforms=shared_platforms,
                shared_platform_keys=shared_platform_keys,
                shared_actors=shared_actors,
                shared_cves=shared_cves,
                shared_campaign_names=shared_campaign_names,
                shared_article_hashes=shared_hashes,
                date_gap_days=date_gap,
            )
        )
    edges.sort(key=lambda edge: (-edge.confidence, edge.from_canonical_incident_id, edge.to_canonical_incident_id))
    return edges


def _connected_components(profile_ids: Iterable[str], edges: Iterable[CampaignEdge]) -> list[set[str]]:
    parent = {profile_id: profile_id for profile_id in profile_ids}

    def find(node: str) -> str:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: str, right: str) -> None:
        root_left = find(left)
        root_right = find(right)
        if root_left != root_right:
            parent[root_right] = root_left

    edge_count = 0
    for edge in edges:
        edge_count += 1
        union(edge.from_canonical_incident_id, edge.to_canonical_incident_id)
    if edge_count == 0:
        return []

    components: dict[str, set[str]] = defaultdict(set)
    for profile_id in profile_ids:
        components[find(profile_id)].add(profile_id)
    return [component for component in components.values() if len(component) >= 2]


def _edge_bucket_keys(edge: CampaignEdge) -> list[tuple[str, str]]:
    keys: list[tuple[str, str]] = []
    keys.extend(("cve", value) for value in edge.shared_cves)
    keys.extend(("platform", value) for value in edge.shared_platform_keys)
    if not keys:
        keys.extend(("campaign_name", value) for value in edge.shared_campaign_names)
    if not keys:
        keys.extend(("vendor", value) for value in edge.shared_vendors)
    if not keys:
        keys.extend(("actor", value) for value in edge.shared_actors)
    return keys


def _bucketed_components(
    profiles: Mapping[str, CampaignProfile],
    edges: Sequence[CampaignEdge],
) -> list[tuple[str, str, set[str], list[CampaignEdge]]]:
    buckets: dict[tuple[str, str], list[CampaignEdge]] = defaultdict(list)
    for edge in edges:
        for key in _edge_bucket_keys(edge):
            buckets[key].append(edge)

    components: list[tuple[str, str, set[str], list[CampaignEdge]]] = []
    seen: set[tuple[str, str, tuple[str, ...]]] = set()
    for (kind, value), bucket_edges in sorted(buckets.items()):
        profile_ids: set[str] = set()
        for edge in bucket_edges:
            profile_ids.add(edge.from_canonical_incident_id)
            profile_ids.add(edge.to_canonical_incident_id)
        for component in _connected_components(profile_ids, bucket_edges):
            signature = (kind, value, tuple(sorted(component)))
            if signature in seen:
                continue
            seen.add(signature)
            component_edges = [
                edge
                for edge in bucket_edges
                if edge.from_canonical_incident_id in component
                and edge.to_canonical_incident_id in component
            ]
            components.append((kind, value, component, component_edges))
    components.sort(key=lambda item: (-len(item[2]), item[0], item[1]))
    return components


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug[:48] or "campaign"


def _date_range(profiles: Iterable[CampaignProfile]) -> tuple[str | None, str | None]:
    dates = sorted(_as_text(profile.representative_date) for profile in profiles if profile.representative_date)
    if not dates:
        return None, None
    return dates[0], dates[-1]


def _top_values(profiles: Iterable[CampaignProfile], attr: str) -> list[str]:
    counter: Counter[str] = Counter()
    for profile in profiles:
        values = getattr(profile, attr)
        counter.update(values)
    return [value for value, _count in counter.most_common()]


def _consensus_values(profiles: Iterable[CampaignProfile], attr: str, min_count: int = 2) -> list[str]:
    """Like ``_top_values`` but keeps only values attested by >= ``min_count``
    members. Used for a campaign's CVE list so a single mis-attributed CVE on one
    member does not pollute the whole campaign."""
    counter: Counter[str] = Counter()
    for profile in profiles:
        counter.update(getattr(profile, attr))
    return [value for value, count in counter.most_common() if count >= min_count]


def _evidence_cve_consensus(
    items: Sequence["CampaignEvidenceItem"], component: set[str], min_count: int = 2
) -> list[str]:
    """Campaign CVEs attested by >= ``min_count`` *evidence items* in the cluster.

    Counting at the evidence-item level (not the member level) keeps a real,
    multiply-reported CVE that few victim records mention in their structured
    fields -- e.g. the 2023 MOVEit zero-day CVE-2023-34362 appears in several
    article evidence items even though most affected schools' projections only
    say "MOVEit incident" -- while still dropping one-off mis-attributions (the
    off-event GitLab/Ollama CVEs that each surface in a single evidence item)."""
    counter: Counter[str] = Counter()
    for item in items:
        if item.canonical_incident_id in component:
            counter.update(item.cves)
    return [value for value, count in counter.most_common() if count >= min_count]


def _dominant_year(profiles: Iterable[CampaignProfile]) -> str | None:
    """Most common member year (ties broken toward the later year).

    Robust to a single mis-dated outlier, unlike ``min(member date)`` — a 2023
    MOVEit wave with one stray 2022 record is named 2023, not 2022."""
    counter: Counter[str] = Counter()
    for profile in profiles:
        parsed = _parse_date(profile.representative_date)
        if parsed is not None:
            counter[str(parsed.year)] += 1
    if not counter:
        return None
    return max(counter.items(), key=lambda kv: (kv[1], kv[0]))[0]


def _trim_incoherent_members(
    component: set[str],
    profiles: Mapping[str, CampaignProfile],
    platform_keys: Iterable[str],
) -> set[str]:
    """Drop members whose incident date falls far outside the cluster's core
    time window.

    A real campaign is temporally coherent. Wide platform date windows plus
    transitive union-find chaining can otherwise pull off-event incidents into a
    cluster (e.g. 2025 records merged into the 2023 MOVEit wave), inflating
    member counts and injecting unrelated CVEs. A member is kept when it is
    within ``window`` days of the median member date; undated members are kept
    (cannot judge). Never trims below two members."""
    dated = [(pid, _parse_date(profiles[pid].representative_date)) for pid in component]
    dates = sorted(parsed for _pid, parsed in dated if parsed is not None)
    if len(dates) < 3:
        return set(component)
    median = dates[len(dates) // 2]
    window = _indicator_window_days(platform_keys)
    kept = {pid for pid, parsed in dated if parsed is None or abs((parsed - median).days) <= window}
    return kept if len(kept) >= 2 else set(component)


def _campaign_type(kind: str, value: str, platform_keys: list[str], cves: list[str], actors: list[str]) -> str:
    if kind == "campaign_name":
        return "same_campaign"
    if kind == "cve":
        return "mass_exploitation"
    if kind == "actor":
        return "actor_activity_wave"
    if kind == "vendor":
        return "shared_vendor_incident"
    if kind == "platform" and value in PLATFORM_BY_KEY:
        return PLATFORM_BY_KEY[value].campaign_type
    indicator_types = {
        indicator.campaign_type
        for indicator in PLATFORM_INDICATORS
        if indicator.key in platform_keys
    }
    if cves or "mass_exploitation" in indicator_types:
        return "mass_exploitation"
    if "shared_vendor_incident" in indicator_types:
        return "shared_vendor_incident"
    if actors:
        return "actor_activity_wave"
    return "same_campaign"


def _campaign_name(
    kind: str,
    value: str,
    campaign_type: str,
    platforms: list[str],
    actors: list[str],
    cves: list[str],
    campaign_names: list[str],
    year: str | None,
) -> str:
    suffix = f" {year}" if year else ""
    if kind == "campaign_name":
        return value
    if kind == "platform" and value in PLATFORM_BY_KEY:
        return f"{PLATFORM_BY_KEY[value].platform}{suffix} education impact"
    if kind == "cve":
        return f"{value}{suffix} education exposure"
    if kind == "actor":
        return f"{value}{suffix} education activity wave"
    if kind == "vendor":
        return f"{value}{suffix} education impact"
    if campaign_names:
        return campaign_names[0]
    if platforms:
        return f"{platforms[0]}{suffix} education impact"
    if cves:
        return f"{cves[0]}{suffix} education exposure"
    if actors:
        return f"{actors[0]}{suffix} education activity wave"
    return f"{campaign_type.replace('_', ' ').title()}{suffix}"


def _campaign_id(name: str, member_ids: Iterable[str]) -> str:
    seed = "|".join(sorted(member_ids))
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    return f"campaign_{_slug(name)}_{digest}"


def _assign_families(
    candidates: Sequence[CampaignCandidate],
    memberships: Sequence[CampaignMembership],
) -> None:
    """Group candidate campaigns that describe one real campaign into a family.

    Two candidates join the same family when EITHER:

    1. they share their **top threat actor within the same year** — links the
       alternate views of one actor's wave (the CVE "exposure" and vendor
       "impact" clusters list the responsible actor as their top actor, so
       "Cl0p 2025 activity wave" groups with "CVE-2025-61882 2025 exposure"); or
    2. their member sets **strongly overlap** (shared >= 3 incidents AND
       Jaccard >= 0.5) — links the per-signal "views" of one event that share no
       single actor. The deterministic engine emits one component per signal
       (platform, CVE, campaign-name, vendor), so the 2023 MOVEit wave appears as
       a MOVEit-platform cluster, a CVE-2023-34362 cluster and a "MOVEit"
       name cluster over largely the same incidents; actor-only keying left these
       as separate families (some with no actor at all). The high overlap
       threshold collapses these genuine fragments without transitively chaining
       distinct campaigns (a loose threshold would merge everything into a blob).

    Members are NOT merged across campaigns — this is a presentational grouping
    only; the family is the de-duplicated unit for analysis. Mutates each
    candidate's ``family_id`` / ``related_campaign_ids`` / ``is_primary_in_family``
    in place.
    """

    if not candidates:
        return

    parent: dict[str, str] = {candidate.campaign_id: candidate.campaign_id for candidate in candidates}

    def find(node: str) -> str:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: str, right: str) -> None:
        root_left, root_right = find(left), find(right)
        if root_left != root_right:
            # Deterministic merge direction so family roots are stable across runs.
            lo, hi = sorted((root_left, root_right))
            parent[hi] = lo

    # Union campaigns that share their top threat actor within the same year.
    actor_year_owner: dict[tuple[str, str], str] = {}
    for candidate in candidates:
        if not candidate.actors:
            continue
        actor = _normalize_for_match(candidate.actors[0])
        if not actor:
            continue
        key = (actor, (candidate.first_seen_date or "")[:4])
        owner = actor_year_owner.get(key)
        if owner is None:
            actor_year_owner[key] = candidate.campaign_id
        else:
            union(owner, candidate.campaign_id)

    # Union campaigns that are per-signal fragments of one event, detected by
    # strong member-set overlap (shared >= 3 AND Jaccard >= 0.5).
    members_by_campaign: dict[str, set[str]] = defaultdict(set)
    for membership in memberships:
        members_by_campaign[membership.campaign_id].add(membership.canonical_incident_id)
    overlap_ids = [c.campaign_id for c in candidates if len(members_by_campaign.get(c.campaign_id, ())) >= 3]
    for index, left_id in enumerate(overlap_ids):
        left_members = members_by_campaign[left_id]
        for right_id in overlap_ids[index + 1 :]:
            right_members = members_by_campaign[right_id]
            shared = len(left_members & right_members)
            if shared >= 3 and shared / len(left_members | right_members) >= 0.5:
                union(left_id, right_id)

    families: dict[str, list[CampaignCandidate]] = defaultdict(list)
    for candidate in candidates:
        families[find(candidate.campaign_id)].append(candidate)

    for root, members in families.items():
        member_ids = sorted(member.campaign_id for member in members)
        family_id = "family_" + hashlib.sha1("|".join(member_ids).encode("utf-8")).hexdigest()[:10]
        # Primary = largest membership, then highest confidence, then stable id.
        primary = max(members, key=lambda c: (c.member_count, c.confidence, c.campaign_id))
        for member in members:
            member.family_id = family_id
            member.related_campaign_ids = [cid for cid in member_ids if cid != member.campaign_id]
            member.is_primary_in_family = member.campaign_id == primary.campaign_id


def build_campaign_outputs(
    items: Sequence[CampaignEvidenceItem],
    edges: Sequence[CampaignEdge],
) -> tuple[list[CampaignCandidate], list[CampaignMembership]]:
    profiles = build_profiles(items)
    components = _bucketed_components(profiles, edges)
    candidates: list[CampaignCandidate] = []
    memberships: list[CampaignMembership] = []

    for kind, value, component, component_edges in components:
        # Trim temporally-incoherent (off-event) members before deriving any
        # attribute, so dates / CVEs / member_count / memberships all reflect the
        # coherent core. Size the window from the cluster's platform type first.
        pre_platform_keys = _top_values([profiles[pid] for pid in component], "platform_keys")
        component = _trim_incoherent_members(component, profiles, pre_platform_keys)
        component_edges = [
            edge
            for edge in component_edges
            if edge.from_canonical_incident_id in component
            and edge.to_canonical_incident_id in component
        ]
        component_profiles = [profiles[profile_id] for profile_id in sorted(component)]
        first_seen, last_seen = _date_range(component_profiles)
        year = _dominant_year(component_profiles)
        platform_keys = _top_values(component_profiles, "platform_keys")
        platforms = _top_values(component_profiles, "platforms")
        vendors = _top_values(component_profiles, "vendors")
        actors = _top_values(component_profiles, "actors")
        # Campaign CVE list = CVEs attested by >=2 evidence items in the (trimmed)
        # cluster. Evidence-level consensus keeps a genuinely-reported CVE that few
        # victim projections carry (e.g. MOVEit's CVE-2023-34362) while still
        # dropping one-off off-event mis-attributions.
        cves = _evidence_cve_consensus(items, component, 2)
        campaign_names = _top_values(component_profiles, "campaign_names")
        attack_categories = _top_values(component_profiles, "attack_categories")
        if kind == "platform" and value in PLATFORM_BY_KEY:
            indicator = PLATFORM_BY_KEY[value]
            platform_keys = [indicator.key]
            platforms = [indicator.platform]
            vendors = [indicator.vendor]
        elif kind == "cve":
            cves = [value]
        elif kind == "actor":
            actors = [value]
        elif kind == "campaign_name":
            campaign_names = [value]
        # Collapse product-name-as-vendor strings (Canvas, "Canvas (Instructure)")
        # to the canonical company + platform so a campaign lists Instructure once,
        # not Canvas / Canvas (Instructure) / Instructure as separate vendors.
        vendors, platforms = _canonicalize_vendors_platforms(vendors, platforms)
        campaign_type = _campaign_type(kind, value, platform_keys, cves, actors)
        name = _campaign_name(kind, value, campaign_type, platforms, actors, cves, campaign_names, year)
        campaign_id = _campaign_id(name, component)
        confidence = (
            sum(edge.confidence for edge in component_edges) / len(component_edges)
            if component_edges
            else 0.0
        )
        confirmed = sum(
            1
            for profile in component_profiles
            if profile.canonical_status == "open" and not profile.manual_review_required
        )
        evidence_only = len(component_profiles) - confirmed
        summary_subject = platforms[0] if platforms else actors[0] if actors else cves[0] if cves else "shared evidence"
        analyst_summary = (
            f"Candidate {campaign_type.replace('_', ' ')} cluster around {summary_subject}; "
            f"{len(component_profiles)} canonical records connected by deterministic CTI evidence."
        )
        candidates.append(
            CampaignCandidate(
                campaign_id=campaign_id,
                campaign_name=name,
                campaign_type=campaign_type,
                first_seen_date=first_seen,
                last_seen_date=last_seen,
                actors=actors,
                vendors=vendors,
                platforms=platforms,
                cves=cves,
                campaign_names=campaign_names,
                attack_categories=attack_categories,
                member_count=len(component_profiles),
                confirmed_member_count=confirmed,
                evidence_only_member_count=evidence_only,
                confidence=round(confidence, 3),
                analyst_summary=analyst_summary,
            )
        )

        for profile in component_profiles:
            incident_edges = [
                edge.confidence
                for edge in component_edges
                if profile.canonical_incident_id
                in {edge.from_canonical_incident_id, edge.to_canonical_incident_id}
            ]
            membership_confidence = max(incident_edges) if incident_edges else confidence
            if profile.canonical_status != "open":
                role = "needs_review"
                review_status = "excluded_evidence_only"
            elif profile.manual_review_required:
                role = "needs_review"
                review_status = "manual_review_required"
            elif profile.platform_keys and (
                profile.attack_categories & SUPPLY_CHAIN_CATEGORIES or profile.vendors
            ):
                role = "affected_via_vendor"
                review_status = "candidate_unreviewed"
            else:
                role = "direct_victim"
                review_status = "candidate_unreviewed"

            memberships.append(
                CampaignMembership(
                    campaign_id=campaign_id,
                    canonical_incident_id=profile.canonical_incident_id,
                    role=role,
                    confidence=round(membership_confidence, 3),
                    evidence_article_ids=sorted(profile.article_document_ids),
                    evidence_source_incident_ids=sorted(profile.source_incident_ids),
                    evidence_quotes=profile.evidence_quotes[:3],
                    review_status=review_status,
                    victim_name=profile.victim_name,
                    canonical_status=profile.canonical_status,
                )
            )

    _assign_families(candidates, memberships)
    candidates.sort(key=lambda candidate: (-candidate.member_count, -candidate.confidence, candidate.campaign_name))
    memberships.sort(key=lambda membership: (membership.campaign_id, membership.victim_name or ""))
    return candidates, memberships


def build_signatures(candidates: Sequence[CampaignCandidate]) -> str:
    """Render high-confidence candidate signatures as dependency-free YAML."""

    lines = [
        "# Generated candidate campaign signatures.",
        "# Review before using in production heuristics.",
        "campaign_signatures:",
    ]
    for candidate in candidates:
        if candidate.member_count < 2 or candidate.confidence < 0.65:
            continue
        aliases = _dedupe([*candidate.vendors, *candidate.platforms, *candidate.actors, *candidate.cves])
        aliases = _dedupe([*aliases, *candidate.campaign_names])
        lines.extend(
            [
                f"  - campaign_id: {candidate.campaign_id}",
                f"    campaign_name: {json.dumps(candidate.campaign_name)}",
                f"    campaign_type: {candidate.campaign_type}",
                f"    confidence: {candidate.confidence}",
                f"    date_window:",
                f"      start_date: {candidate.first_seen_date or 'null'}",
                f"      end_date: {candidate.last_seen_date or 'null'}",
                f"    required_any_terms: {json.dumps(aliases)}",
                f"    vendors: {json.dumps(candidate.vendors)}",
                f"    platforms: {json.dumps(candidate.platforms)}",
                f"    actors: {json.dumps(candidate.actors)}",
                f"    cves: {json.dumps(candidate.cves)}",
                f"    campaign_names: {json.dumps(candidate.campaign_names)}",
                "    negative_terms: [\"trend report\", \"best practices\", \"state of cybersecurity\", \"roundup\"]",
            ]
        )
    if len(lines) == 3:
        lines.append("  []")
    return "\n".join(lines) + "\n"


def build_llm_review_packets(
    candidates: Sequence[CampaignCandidate],
    memberships: Sequence[CampaignMembership],
) -> list[dict[str, Any]]:
    by_campaign: dict[str, list[CampaignMembership]] = defaultdict(list)
    for membership in memberships:
        by_campaign[membership.campaign_id].append(membership)
    packets: list[dict[str, Any]] = []
    for candidate in candidates:
        packets.append(
            {
                "task": "campaign_adjudication",
                "allowed_labels": [
                    "same_campaign",
                    "shared_vendor_incident",
                    "mass_exploitation",
                    "actor_activity_wave",
                    "roundup_not_campaign",
                    "unrelated",
                ],
                "candidate": asdict(candidate),
                "members": [asdict(item) for item in by_campaign.get(candidate.campaign_id, [])],
                "instruction": (
                    "Adjudicate whether these victim-level incidents belong to the same "
                    "campaign or shared upstream event. Use only the evidence quotes and "
                    "structured fields. Do not merge victim identities."
                ),
            }
        )
    return packets


def build_review_sample(
    candidates: Sequence[CampaignCandidate],
    memberships: Sequence[CampaignMembership],
    *,
    sample_size: int = 30,
) -> list[dict[str, Any]]:
    """Build a small deterministic manual-review sample across candidates."""

    candidate_order = [candidate.campaign_id for candidate in candidates]
    by_campaign: dict[str, list[CampaignMembership]] = defaultdict(list)
    for membership in memberships:
        by_campaign[membership.campaign_id].append(membership)

    sample: list[dict[str, Any]] = []
    while len(sample) < sample_size:
        added = False
        for campaign_id in candidate_order:
            if not by_campaign[campaign_id]:
                continue
            membership = by_campaign[campaign_id].pop(0)
            row = asdict(membership)
            row["manual_label"] = ""
            row["manual_notes"] = ""
            sample.append(row)
            added = True
            if len(sample) >= sample_size:
                break
        if not added:
            break
    return sample


def _serialize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (list, dict)):
            serialized[key] = json.dumps(value, ensure_ascii=False)
        else:
            serialized[key] = value
    return serialized


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_serialize_row(row))


def write_outputs(
    output_dir: Path,
    *,
    evidence_items: Sequence[CampaignEvidenceItem],
    edges: Sequence[CampaignEdge],
    candidates: Sequence[CampaignCandidate],
    memberships: Sequence[CampaignMembership],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / "campaign_evidence_items.csv", [asdict(item) for item in evidence_items])
    _write_csv(output_dir / "campaign_edges.csv", [asdict(edge) for edge in edges])
    _write_csv(output_dir / "campaign_candidates.csv", [asdict(candidate) for candidate in candidates])
    _write_csv(output_dir / "campaign_memberships.csv", [asdict(membership) for membership in memberships])
    _write_csv(output_dir / "campaign_review_sample.csv", build_review_sample(candidates, memberships))
    (output_dir / "campaign_summaries.json").write_text(
        json.dumps([asdict(candidate) for candidate in candidates], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (output_dir / "campaign_signatures.yml").write_text(build_signatures(candidates), encoding="utf-8")
    packets = build_llm_review_packets(candidates, memberships)
    with (output_dir / "campaign_llm_review_packets.jsonl").open("w", encoding="utf-8") as handle:
        for packet in packets:
            handle.write(json.dumps(packet, ensure_ascii=False) + "\n")
    manifest = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "evidence_items": len(evidence_items),
        "edges": len(edges),
        "campaign_candidates": len(candidates),
        "campaign_memberships": len(memberships),
        "review_sample_size": min(30, len(memberships)),
        "read_only": True,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def fetch_campaign_rows(database_url: str, *, include_excluded: bool = True, limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch source/article/canonical rows in a read-only transaction."""

    status_filter = "" if include_excluded else "and ci.status = 'open'"
    limit_clause = "limit %(limit)s" if limit else ""
    query = f"""
        select
            ci.id::text as canonical_incident_id,
            ci.status as canonical_status,
            ci.institution_name,
            ci.institution_type,
            ci.vendor_name,
            ci.country,
            ci.country_code,
            ci.incident_date,
            ci.date_precision,
            ci.source_published_at,
            ci.attack_category,
            ci.attack_vector,
            ci.threat_actor_name,
            ci.ransomware_family,
            cm.source_incident_id::text as source_incident_id,
            cm.match_type,
            cm.is_primary_member,
            si.source_name,
            si.source_group,
            si.raw_title,
            si.raw_victim_name,
            si.raw_notes,
            si.source_published_at as source_row_published_at,
            se.manual_review_required,
            se.manual_review_reason,
            ad.id::text as article_document_id,
            ad.title as article_title,
            ad.publish_date as article_publish_date,
            left(ad.content_text, 8000) as content_text,
            ad.content_hash,
            ce.canonical_projection,
            ce.analytics_projection,
            (
                select string_agg(distinct coalesce(nullif(siu.resolved_url, ''), siu.url), ' | ')
                from source_incident_urls siu
                where siu.source_incident_id = si.id
            ) as article_url
        from canonical_incidents ci
        join canonical_memberships cm on cm.canonical_incident_id = ci.id
        join source_incidents si on si.id = cm.source_incident_id
        left join source_enrichments se on se.source_incident_id = si.id
        left join article_documents ad on ad.id = se.article_document_id
        left join canonical_enrichments ce on ce.canonical_incident_id = ci.id
        where ci.status in ('open', 'excluded')
          and coalesce(ci.is_education_related, true) is true
          {status_filter}
        order by ci.incident_date nulls last, ci.id, cm.is_primary_member desc
        {limit_clause}
    """
    with psycopg.connect(
        database_url,
        row_factory=dict_row,
        options="-c default_transaction_read_only=on -c statement_timeout=300000",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query, {"limit": limit})
            return list(cur.fetchall())


def run_campaign_analysis(
    database_url: str,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    include_excluded: bool = True,
    limit: int | None = None,
) -> dict[str, int]:
    rows = fetch_campaign_rows(database_url, include_excluded=include_excluded, limit=limit)
    evidence_items = build_evidence_items(rows)
    profiles = build_profiles(evidence_items)
    edges = build_candidate_edges(profiles)
    candidates, memberships = build_campaign_outputs(evidence_items, edges)
    write_outputs(
        output_dir,
        evidence_items=evidence_items,
        edges=edges,
        candidates=candidates,
        memberships=memberships,
    )
    return {
        "rows": len(rows),
        "evidence_items": len(evidence_items),
        "profiles": len(profiles),
        "edges": len(edges),
        "campaign_candidates": len(candidates),
        "campaign_memberships": len(memberships),
    }


def _database_url_from_env() -> str | None:
    return (
        os.getenv("EDU_CTI_V2_DATABASE_URL")
        or os.getenv("DATABASE_PUBLIC_URL")
        or os.getenv("DATABASE_URL")
        or _component_database_url()
    )


def _component_database_url() -> str | None:
    host = os.getenv("PGHOST")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    database = os.getenv("PGDATABASE")
    port = os.getenv("PGPORT", "5432")
    if not all((host, user, password, database)):
        return None
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate read-only campaign correlation outputs.")
    parser.add_argument("--database-url", default=None, help="Postgres URL. Defaults to EDU_CTI_V2_DATABASE_URL/DATABASE_URL.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--open-only", action="store_true", help="Exclude review/excluded canonical evidence rows.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit for smoke tests.")
    args = parser.parse_args(argv)

    database_url = args.database_url or _database_url_from_env()
    if not database_url:
        raise SystemExit(
            "Missing database URL. Set EDU_CTI_V2_DATABASE_URL, DATABASE_URL, or pass --database-url."
        )

    result = run_campaign_analysis(
        database_url,
        output_dir=args.output_dir,
        include_excluded=not args.open_only,
        limit=args.limit,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
