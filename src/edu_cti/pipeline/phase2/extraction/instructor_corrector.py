"""
Instructor-based validation and self-correction layer for critical CTI extraction fields.

After the main GBNF-constrained extraction, this module detects null/invalid critical
fields and runs ONE targeted Instructor retry call to fix them. The retry loop is
powered by Pydantic validators — if the LLM outputs an invalid enum value, Instructor
sends the Pydantic validation error back as a follow-up message and retries automatically.

Design:
  - Only fires when >= CORRECTION_THRESHOLD critical fields are null/unknown/invalid
  - Makes exactly ONE additional LLM call per incident that triggers the threshold
  - Is fully non-fatal: any error in the correction pass returns the original json_data
  - Is a no-op when the `instructor` package is not installed

Cost model:
  - ~20% of incidents trigger a correction pass (those with 2+ null critical fields)
  - Each correction call is ~2-4s on DeepSeek V3.1 (small schema, focused prompt)
  - Net per-incident overhead: 0.2 × 3s ≈ 0.6s average
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, field_validator

from src.edu_cti.core.countries import normalize_country

logger = logging.getLogger(__name__)

try:
    import instructor
    INSTRUCTOR_AVAILABLE = True
except (ImportError, TypeError):
    # ImportError: instructor not installed
    # TypeError: instructor ≥1.0 uses Python 3.10+ union syntax (str | Path) which
    # raises TypeError on Python 3.9 during module evaluation.
    INSTRUCTOR_AVAILABLE = False
    instructor = None  # type: ignore[assignment]


# ── Valid enum sets ───────────────────────────────────────────────────────────

# These frozensets mirror the JSON Schema enum lists in extraction_schema.py exactly.
# Keep in sync whenever extraction_schema.py enum lists change.
ATTACK_CATEGORY_ENUMS = frozenset({
    # Ransomware
    "ransomware_encryption", "ransomware_double_extortion", "ransomware_triple_extortion",
    "ransomware_data_leak_only",
    # Phishing / BEC
    "phishing_credential_harvest", "phishing_malware_delivery", "spear_phishing",
    "whaling", "business_email_compromise", "smishing", "vishing",
    # Data breach
    "data_breach_external", "data_breach_internal", "data_exposure_misconfiguration",
    "data_leak_accidental",
    # DDoS
    "ddos_volumetric", "ddos_application", "ddos_protocol",
    # Malware
    "malware_trojan", "malware_worm", "malware_backdoor", "malware_rootkit",
    "malware_cryptominer", "malware_infostealer", "malware_rat", "malware_botnet",
    # Access
    "unauthorized_access", "privilege_escalation", "credential_stuffing",
    "brute_force", "password_spraying",
    # Web
    "web_defacement", "sql_injection", "xss_attack", "api_abuse",
    # Insider
    "insider_malicious", "insider_negligent", "insider_compromised",
    # Supply chain
    "supply_chain_software", "supply_chain_hardware", "supply_chain_service_provider",
    "third_party_compromise",
    # Other
    "social_engineering", "physical_breach", "account_takeover",
    "extortion_no_ransomware", "hacktivism", "espionage", "sabotage", "fraud",
    "unknown", "other",
})

INSTITUTION_TYPE_ENUMS = frozenset({
    "university", "community_college", "technical_college", "vocational_school",
    "k12_school", "school_district", "research_institute", "research_center",
    "medical_school", "university_hospital", "teaching_hospital", "online_university",
    "library", "tribal_college", "military_academy", "edtech_platform",
    "tutoring_service", "consortium", "education_department", "education_ministry",
    "student_loan_servicer", "education_nonprofit", "education_vendor", "unknown",
})

ATTACK_VECTOR_ENUMS = frozenset({
    # Email
    "phishing_email", "spear_phishing_email", "malicious_attachment", "malicious_link",
    "business_email_compromise",
    # Credential
    "stolen_credentials", "credential_stuffing", "brute_force", "password_spraying",
    "credential_phishing", "session_hijacking",
    # Vulnerability
    "vulnerability_exploit_known", "vulnerability_exploit_zero_day", "unpatched_system",
    "misconfiguration", "default_credentials",
    # Web
    "drive_by_download", "watering_hole", "malvertising", "sql_injection",
    "xss", "csrf", "ssrf", "path_traversal",
    # Network
    "exposed_service", "exposed_rdp", "exposed_vpn", "exposed_ssh",
    "exposed_database", "exposed_api", "man_in_the_middle",
    # Supply chain
    "supply_chain_compromise", "third_party_vendor", "software_update_compromise",
    "trusted_relationship",
    # Physical / Social
    "social_engineering", "pretexting", "baiting", "tailgating", "usb_drop",
    # Insider
    "insider_access", "former_employee",
    # Cloud
    "cloud_misconfiguration", "api_key_exposure", "storage_bucket_exposure",
    # Other
    "dns_hijacking", "bgp_hijacking", "sim_swapping", "unknown", "other",
})

# Fire the correction pass when at least this many critical fields are null/unknown
CORRECTION_THRESHOLD = 2

_NULL_SENTINELS = frozenset({"unknown", "other", "", "n/a", "none", "null"})


# ── Pydantic correction model ─────────────────────────────────────────────────

class CriticalFieldsCorrection(BaseModel):
    """
    Slim Pydantic model for the targeted Instructor correction call.

    Only the fields most commonly wrong or null after GBNF extraction. Validators
    enforce enum constraints so Instructor's retry loop fires with a specific error
    message when the LLM outputs an invalid value (e.g. 'ransomware' instead of
    'ransomware_encryption').
    """

    attack_category: Optional[str] = None
    institution_type: Optional[str] = None
    attack_vector: Optional[str] = None
    country: Optional[str] = None
    ransomware_family: Optional[str] = None
    records_affected_exact: Optional[int] = None

    @field_validator("attack_category")
    @classmethod
    def validate_attack_category(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        normalized = v.lower().strip().replace(" ", "_")
        if normalized not in ATTACK_CATEGORY_ENUMS:
            valid = ", ".join(sorted(ATTACK_CATEGORY_ENUMS))
            raise ValueError(
                f"'{v}' is not a valid attack_category. "
                f"Choose the single best match from: {valid}"
            )
        return normalized

    @field_validator("institution_type")
    @classmethod
    def validate_institution_type(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        normalized = v.lower().strip().replace(" ", "_")
        if normalized not in INSTITUTION_TYPE_ENUMS:
            valid = ", ".join(sorted(INSTITUTION_TYPE_ENUMS))
            raise ValueError(
                f"'{v}' is not a valid institution_type. "
                f"Choose from: {valid}"
            )
        return normalized

    @field_validator("attack_vector")
    @classmethod
    def validate_attack_vector(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        normalized = v.lower().strip().replace(" ", "_")
        if normalized not in ATTACK_VECTOR_ENUMS:
            valid = ", ".join(sorted(ATTACK_VECTOR_ENUMS))
            raise ValueError(
                f"'{v}' is not a valid attack_vector. "
                f"Choose from: {valid}"
            )
        return normalized

    @field_validator("country")
    @classmethod
    def validate_country(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        text = str(v).strip()
        if not text or text.lower() in _NULL_SENTINELS:
            return None
        return normalize_country(text)


# ── Field detection helpers ───────────────────────────────────────────────────

def _extract_attack_vector(json_data: Dict[str, Any]) -> Optional[str]:
    """Pull attack_vector from flat fields or nested attack_dynamics, whichever is set."""
    flat = json_data.get("attack_vector")
    if flat and str(flat).lower() not in _NULL_SENTINELS:
        return str(flat)
    nested = json_data.get("attack_dynamics") or {}
    if isinstance(nested, dict):
        val = nested.get("attack_vector")
        if val and str(val).lower() not in _NULL_SENTINELS:
            return str(val)
    return None


def count_null_critical_fields(json_data: Dict[str, Any]) -> Tuple[int, List[str]]:
    """
    Count how many critical fields are null, unknown, or missing.

    Returns (count, list_of_field_names).
    """
    null_fields: List[str] = []

    for field in ("attack_category", "institution_type", "country"):
        val = json_data.get(field)
        if not val or str(val).lower() in _NULL_SENTINELS:
            null_fields.append(field)

    if not _extract_attack_vector(json_data):
        null_fields.append("attack_vector")

    return len(null_fields), null_fields


def should_trigger_correction(json_data: Dict[str, Any]) -> bool:
    """Return True when enough critical fields are missing to warrant a correction pass."""
    count, _ = count_null_critical_fields(json_data)
    return count >= CORRECTION_THRESHOLD


# ── Main correction entry point ───────────────────────────────────────────────

def apply_instructor_corrections(
    json_data: Dict[str, Any],
    article_text: str,
    institution_name: str,
    ollama_client: Any,
    max_retries: int = 3,
) -> Tuple[Dict[str, Any], bool]:
    """
    Run a targeted Instructor correction pass over json_data.

    Makes ONE additional LLM call using Instructor's Pydantic retry loop. If the
    LLM returns an invalid enum value (e.g. attack_category='ransomware'), the
    Pydantic validator raises a ValueError, Instructor appends that error as a new
    user message, and retries automatically up to max_retries times.

    Args:
        json_data:         Extraction result from the GBNF call (may have null fields)
        article_text:      Combined article text (first 8000 chars used for context)
        institution_name:  Institution name from the initial extraction
        ollama_client:     OllamaLLMClient instance (provides .client and .model)
        max_retries:       Max Instructor validation retry attempts (default 3)

    Returns:
        (corrected_json_data, was_corrected_bool)
    """
    if not INSTRUCTOR_AVAILABLE:
        logger.debug("instructor package not installed — skipping correction pass")
        return json_data, False

    _, null_fields = count_null_critical_fields(json_data)
    if not null_fields:
        return json_data, False

    logger.debug(f"Instructor correction triggered — null/invalid fields: {null_fields}")

    try:
        # instructor ≥1.7 dropped from_ollama; use from_openai with Ollama's
        # OpenAI-compatible endpoint (available at <host>/v1).
        from openai import OpenAI as _OpenAI
        _oa_client = _OpenAI(
            base_url=f"{getattr(ollama_client, 'host', 'https://ollama.com')}/v1",
            api_key=getattr(ollama_client, 'api_key', 'ollama'),
        )
        instructor_client = instructor.from_openai(_oa_client, mode=instructor.Mode.JSON)

        current_values = {
            "attack_category": json_data.get("attack_category"),
            "institution_type": json_data.get("institution_type"),
            "attack_vector": _extract_attack_vector(json_data),
            "country": json_data.get("country"),
            "ransomware_family": (
                (json_data.get("attack_dynamics") or {}).get("ransomware_family")
                or json_data.get("ransomware_family")
            ),
        }

        prompt = (
            f"You extracted CTI data from a cybersecurity article about: {institution_name}\n\n"
            f"The following fields are null or unknown after the main extraction: "
            f"{', '.join(null_fields)}\n\n"
            f"Current extracted values: {current_values}\n\n"
            f"Article excerpt:\n{article_text[:8_000].strip()}\n\n"
            "Provide corrected values for the missing/invalid fields only. "
            "Set fields to null if the article genuinely does not mention them."
        )

        corrected: CriticalFieldsCorrection = instructor_client.chat.completions.create(
            model=ollama_client.model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a Cyber Threat Intelligence analyst. "
                        "Extract only the requested fields from the article. "
                        "Return valid JSON."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            response_model=CriticalFieldsCorrection,
            max_retries=max_retries,
            temperature=0,
        )

        applied: List[str] = []

        if corrected.attack_category:
            existing = json_data.get("attack_category")
            if not existing or str(existing).lower() in _NULL_SENTINELS:
                json_data["attack_category"] = corrected.attack_category
                applied.append(f"attack_category={corrected.attack_category}")

        if corrected.institution_type:
            existing = json_data.get("institution_type")
            if not existing or str(existing).lower() in _NULL_SENTINELS:
                json_data["institution_type"] = corrected.institution_type
                applied.append(f"institution_type={corrected.institution_type}")

        if corrected.attack_vector:
            ad = json_data.get("attack_dynamics")
            if isinstance(ad, dict) and (not ad.get("attack_vector") or str(ad.get("attack_vector", "")).lower() in _NULL_SENTINELS):
                ad["attack_vector"] = corrected.attack_vector
                json_data["attack_dynamics"] = ad
                applied.append(f"attack_vector={corrected.attack_vector}")
            elif not _extract_attack_vector(json_data):
                json_data["attack_vector"] = corrected.attack_vector
                applied.append(f"attack_vector={corrected.attack_vector}")

        if corrected.country:
            existing = json_data.get("country")
            if not existing or str(existing).lower() in _NULL_SENTINELS:
                json_data["country"] = corrected.country
                applied.append(f"country={corrected.country}")

        if corrected.ransomware_family:
            ad = json_data.get("attack_dynamics")
            if isinstance(ad, dict) and not ad.get("ransomware_family"):
                ad["ransomware_family"] = corrected.ransomware_family
                json_data["attack_dynamics"] = ad
                applied.append(f"ransomware_family={corrected.ransomware_family}")
            elif not json_data.get("ransomware_family"):
                json_data["ransomware_family"] = corrected.ransomware_family
                applied.append(f"ransomware_family={corrected.ransomware_family}")

        if corrected.records_affected_exact and not json_data.get("records_affected_exact"):
            json_data["records_affected_exact"] = corrected.records_affected_exact
            applied.append(f"records_affected_exact={corrected.records_affected_exact}")

        if applied:
            logger.info(f"Instructor corrections applied: {', '.join(applied)}")
            return json_data, True

        logger.debug("Instructor correction pass returned no new values")
        return json_data, False

    except Exception as exc:
        # Never let correction errors block the main pipeline
        logger.warning(f"Instructor correction pass failed (non-fatal): {exc}")
        return json_data, False
