"""Controlled vocabularies and write-time categorical normalizers for v2.

The canonical slug sets below mirror the enumeration lists in the LLM extraction
schema (``src/edu_cti/pipeline/phase2/extraction/extraction_schema.py``). They are
the single source of truth for the categorical *dimension* values used by the
star-schema analytical layer (``models/star.py``) and by clean dataset exports.

The pipeline historically normalized categorical values at read time (in the
breakdown repositories and on the dashboard), which left the stored values mixed
(for example both ``"university"`` and ``"University"`` for the same type). These
helpers move that normalization to the *write* path so the canonical columns hold
one stable slug per concept, exports need no preprocessing, and analytics can use
plain ``GROUP BY``.

Threat-actor and ransomware-family normalization already exists in
``src/edu_cti_v2/normalization.py`` and is reused, not reimplemented.
"""

from __future__ import annotations

from typing import Optional

# ── Canonical slug vocabularies (mirror extraction_schema.py enum lists) ───────

INSTITUTION_TYPES: frozenset[str] = frozenset({
    "university", "community_college", "technical_college", "vocational_school",
    "k12_school", "school_district", "research_institute", "research_center",
    "medical_school", "university_hospital", "teaching_hospital", "online_university",
    "library", "tribal_college", "military_academy", "edtech_platform",
    "tutoring_service", "consortium", "education_department", "education_ministry",
    "student_loan_servicer", "education_nonprofit", "education_vendor", "unknown",
})

ATTACK_CATEGORIES: frozenset[str] = frozenset({
    "ransomware_encryption", "ransomware_double_extortion", "ransomware_triple_extortion",
    "ransomware_data_leak_only", "phishing_credential_harvest", "phishing_malware_delivery",
    "spear_phishing", "whaling", "business_email_compromise", "smishing", "vishing",
    "data_breach_external", "data_breach_internal", "data_exposure_misconfiguration",
    "data_leak_accidental", "ddos_volumetric", "ddos_application", "ddos_protocol",
    "malware_trojan", "malware_worm", "malware_backdoor", "malware_rootkit",
    "malware_cryptominer", "malware_infostealer", "malware_rat", "malware_botnet",
    "unauthorized_access", "privilege_escalation", "credential_stuffing", "brute_force",
    "password_spraying", "web_defacement", "sql_injection", "xss_attack", "api_abuse",
    "insider_malicious", "insider_negligent", "insider_compromised", "supply_chain_software",
    "supply_chain_hardware", "supply_chain_service_provider", "third_party_compromise",
    "social_engineering", "physical_breach", "account_takeover", "extortion_no_ransomware",
    "hacktivism", "espionage", "sabotage", "fraud", "unknown", "other",
})

ATTACK_VECTORS: frozenset[str] = frozenset({
    "phishing_email", "spear_phishing_email", "malicious_attachment", "malicious_link",
    "business_email_compromise", "stolen_credentials", "credential_stuffing", "brute_force",
    "password_spraying", "credential_phishing", "session_hijacking", "vulnerability_exploit_known",
    "vulnerability_exploit_zero_day", "unpatched_system", "misconfiguration", "default_credentials",
    "drive_by_download", "watering_hole", "malvertising", "sql_injection", "xss", "csrf", "ssrf",
    "path_traversal", "exposed_service", "exposed_rdp", "exposed_vpn", "exposed_ssh",
    "exposed_database", "exposed_api", "man_in_the_middle", "supply_chain_compromise",
    "third_party_vendor", "software_update_compromise", "trusted_relationship", "social_engineering",
    "pretexting", "baiting", "tailgating", "usb_drop", "insider_access", "former_employee",
    "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure", "dns_hijacking",
    "bgp_hijacking", "sim_swapping", "unknown", "other",
})

SEVERITIES: frozenset[str] = frozenset({
    "critical", "high", "medium", "low", "informational",
})

MITRE_TACTICS: frozenset[str] = frozenset({
    "reconnaissance", "resource_development", "initial_access", "execution", "persistence",
    "privilege_escalation", "defense_evasion", "credential_access", "discovery",
    "lateral_movement", "collection", "command_and_control", "exfiltration", "impact",
})

# Stragglers observed in production that are not exact enum slugs. Mapped to the
# closest canonical slug so historical records collapse cleanly on backfill.
_INSTITUTION_TYPE_ALIASES: dict[str, str] = {
    "school": "k12_school",
    "k_12_school": "k12_school",
    "k_12": "k12_school",
    "k12": "k12_school",
    "high_school": "k12_school",
    "elementary_school": "k12_school",
    "middle_school": "k12_school",
    "primary_school": "k12_school",
    "secondary_school": "k12_school",
    "college": "community_college",
    "polytechnic": "technical_college",
    "research_institution": "research_institute",
    "hospital": "university_hospital",
    "education_company": "education_vendor",
    "ed_tech": "edtech_platform",
    "edtech": "edtech_platform",
    "edtech_vendor": "edtech_platform",
    "nonprofit": "education_nonprofit",
    "ministry_of_education": "education_ministry",
    "department_of_education": "education_department",
}

_SEVERITY_ALIASES: dict[str, str] = {
    "info": "informational",
    "informative": "informational",
    "moderate": "medium",
    "med": "medium",
    "severe": "high",
    "catastrophic": "critical",
}


def slugify(value: Optional[str]) -> Optional[str]:
    """Lowercase, collapse separators to single underscores; the casing fix.

    ``"University"`` and ``"Research Institute"`` both become canonical slugs.
    """
    if value is None:
        return None
    slug = value.strip().lower().replace("/", "_").replace("-", "_").replace(" ", "_")
    slug = "_".join(part for part in slug.split("_") if part)
    return slug or None


def _normalize_in_vocab(
    value: Optional[str],
    vocab: frozenset[str],
    aliases: Optional[dict[str, str]] = None,
) -> Optional[str]:
    slug = slugify(value)
    if slug is None:
        return None
    if aliases and slug in aliases:
        return aliases[slug]
    if slug in vocab:
        return slug
    # Unrecognized value: keep the slug (still casing-normalized) so no signal is
    # silently dropped; dimension build records it as an out-of-vocabulary member.
    return slug


def normalize_institution_type(value: Optional[str]) -> Optional[str]:
    return _normalize_in_vocab(value, INSTITUTION_TYPES, _INSTITUTION_TYPE_ALIASES)


def normalize_attack_category(value: Optional[str]) -> Optional[str]:
    return _normalize_in_vocab(value, ATTACK_CATEGORIES)


def normalize_attack_vector(value: Optional[str]) -> Optional[str]:
    return _normalize_in_vocab(value, ATTACK_VECTORS)


def normalize_severity(value: Optional[str]) -> Optional[str]:
    return _normalize_in_vocab(value, SEVERITIES, _SEVERITY_ALIASES)


def normalize_mitre_tactic(value: Optional[str]) -> Optional[str]:
    return _normalize_in_vocab(value, MITRE_TACTICS)


def is_in_vocabulary(kind: str, slug: Optional[str]) -> bool:
    """Whether a slug is a recognized member of the named controlled vocabulary."""
    if slug is None:
        return False
    return slug in {
        "institution_type": INSTITUTION_TYPES,
        "attack_category": ATTACK_CATEGORIES,
        "attack_vector": ATTACK_VECTORS,
        "severity": SEVERITIES,
        "mitre_tactic": MITRE_TACTICS,
    }.get(kind, frozenset())
