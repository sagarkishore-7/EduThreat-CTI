"""
MITRE ATT&CK STIX-based technique lookup.

Downloads the MITRE ATT&CK Enterprise STIX bundle once, caches it to disk
under data/mitre_attack_cache.json, and exposes a fast dict-based lookup:

    get_technique_info(technique_id) -> {"name": ..., "tactic": ..., "description": ...}

The cache is refreshed automatically if it is older than CACHE_MAX_AGE_DAYS.
Falls back to the static hand-curated table in post_processing.py if the
download fails and no cache exists.

Railway / Docker note: cache is written to DATA_DIR (same volume as the SQLite
database) so it survives container restarts.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_STIX_URL = (
    "https://raw.githubusercontent.com/mitre/cti/master/"
    "enterprise-attack/enterprise-attack.json"
)
_CACHE_FILENAME = "mitre_attack_cache.json"
_CACHE_MAX_AGE_DAYS = 30  # re-download if cache is older than this

# Runtime cache (populated on first use)
_technique_map: Optional[Dict[str, Dict[str, str]]] = None

# ── Helpers ──────────────────────────────────────────────────────────────────

def _cache_path() -> Path:
    from src.edu_cti.core import config  # lazy import avoids circular deps
    data_dir = Path(getattr(config, "DATA_DIR", "data"))
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / _CACHE_FILENAME


def _cache_is_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    age_days = (time.time() - path.stat().st_mtime) / 86400
    return age_days < _CACHE_MAX_AGE_DAYS


_PHASE_NAME_CANONICAL: Dict[str, str] = {
    # ATT&CK v16+ uses internal phase names that differ from display names
    "stealth": "Defense Evasion",
    "privilege-escalation": "Privilege Escalation",
    "defense-evasion": "Defense Evasion",
    "credential-access": "Credential Access",
    "lateral-movement": "Lateral Movement",
    "command-and-control": "Command and Control",
    "resource-development": "Resource Development",
    "initial-access": "Initial Access",
    "persistence": "Persistence",
    "execution": "Execution",
    "collection": "Collection",
    "exfiltration": "Exfiltration",
    "discovery": "Discovery",
    "impact": "Impact",
    "reconnaissance": "Reconnaissance",
}


def _tactic_from_kill_chain(obj: dict) -> str:
    """Extract canonical tactic display name from kill_chain_phases."""
    phases = obj.get("kill_chain_phases") or []
    for phase in phases:
        if phase.get("kill_chain_name") == "mitre-attack":
            raw = phase.get("phase_name", "")
            return _PHASE_NAME_CANONICAL.get(raw, raw.replace("-", " ").title())
    return ""


def _build_map_from_stix(stix_bundle: dict) -> Dict[str, Dict[str, str]]:
    """Parse STIX bundle → {technique_id: {name, tactic, description}}."""
    result: Dict[str, Dict[str, str]] = {}
    for obj in stix_bundle.get("objects", []):
        if obj.get("type") != "attack-pattern":
            continue
        if obj.get("x_mitre_deprecated") or obj.get("revoked"):
            continue
        # Find ATT&CK external ID (T1xxx)
        ext_id = None
        for ref in obj.get("external_references") or []:
            if ref.get("source_name") == "mitre-attack":
                ext_id = ref.get("external_id", "")
                break
        if not ext_id or not ext_id.startswith("T"):
            continue
        name = obj.get("name", "")
        tactic = _tactic_from_kill_chain(obj)
        # Description: first sentence only (full descriptions are 200+ words)
        raw_desc = obj.get("description", "")
        description = raw_desc.split(". ")[0].strip() if raw_desc else ""
        result[ext_id] = {"name": name, "tactic": tactic, "description": description}
    return result


def _download_stix() -> Optional[Dict[str, Dict[str, str]]]:
    """Download STIX bundle, return parsed technique map or None on failure."""
    logger.info("Downloading MITRE ATT&CK STIX bundle from %s", _STIX_URL)
    try:
        req = urllib.request.Request(_STIX_URL, headers={"User-Agent": "EduThreat-CTI/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            bundle = json.loads(resp.read().decode("utf-8"))
        technique_map = _build_map_from_stix(bundle)
        logger.info("MITRE STIX: loaded %d techniques", len(technique_map))
        return technique_map
    except (urllib.error.URLError, json.JSONDecodeError, Exception) as exc:
        logger.warning("MITRE STIX download failed: %s", exc)
        return None


def _load_from_cache(path: Path) -> Optional[Dict[str, Dict[str, str]]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.debug("MITRE STIX: loaded %d techniques from cache", len(data))
        return data
    except Exception as exc:
        logger.warning("MITRE STIX cache read failed: %s", exc)
        return None


def _save_to_cache(path: Path, technique_map: Dict[str, Dict[str, str]]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(technique_map, f, ensure_ascii=False, separators=(",", ":"))
        logger.debug("MITRE STIX: saved %d techniques to cache at %s", len(technique_map), path)
    except Exception as exc:
        logger.warning("MITRE STIX cache write failed: %s", exc)


# ── Public API ────────────────────────────────────────────────────────────────

def load_technique_map(force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
    """
    Load the MITRE ATT&CK technique map.

    Precedence:
    1. In-memory runtime cache (fastest)
    2. Disk cache (fast, survives process restart)
    3. Live STIX download (slow, updates disk cache)
    4. Empty dict (fallback — post_processing static table still applies)
    """
    global _technique_map
    if _technique_map is not None and not force_refresh:
        return _technique_map

    cache = _cache_path()

    if not force_refresh and _cache_is_fresh(cache):
        data = _load_from_cache(cache)
        if data:
            _technique_map = data
            return _technique_map

    # Attempt live download
    downloaded = _download_stix()
    if downloaded:
        _save_to_cache(cache, downloaded)
        _technique_map = downloaded
        return _technique_map

    # Fall back to disk cache (may be stale or missing)
    if cache.exists():
        data = _load_from_cache(cache)
        if data:
            logger.warning("MITRE STIX: using stale cache (download failed)")
            _technique_map = data
            return _technique_map

    logger.warning("MITRE STIX: no cache and download failed — returning empty map")
    _technique_map = {}
    return _technique_map


def get_technique_info(technique_id: str) -> Optional[Dict[str, str]]:
    """
    Look up a MITRE ATT&CK technique by ID.

    Returns {"name": ..., "tactic": ..., "description": ...} or None.
    Handles both base IDs (T1566) and subtechnique IDs (T1566.001).
    If the exact subtechnique is not found, falls back to the base technique.
    """
    tech_map = load_technique_map()
    if not tech_map:
        return None
    info = tech_map.get(technique_id)
    if info:
        return info
    # Subtechnique fallback: T1566.001 → T1566
    if "." in technique_id:
        base_id = technique_id.split(".")[0]
        return tech_map.get(base_id)
    return None


def hydrate_mitre_techniques(techniques: list) -> bool:
    """
    Fill null technique_name, tactic, description in a list of technique dicts
    (as produced by the LLM and stored in mitre_techniques_json).

    Returns True if any field was filled (caller should re-serialize to JSON).
    """
    changed = False
    for tech in techniques:
        if not isinstance(tech, dict):
            continue
        tid = (tech.get("technique_id") or "").strip()
        if not tid:
            continue
        info = get_technique_info(tid)
        if not info:
            continue
        if not tech.get("technique_name"):
            tech["technique_name"] = info["name"]
            changed = True
        if not tech.get("tactic") and info.get("tactic"):
            tech["tactic"] = info["tactic"]
            changed = True
        if not tech.get("description") and info.get("description"):
            tech["description"] = info["description"]
            changed = True
    return changed
