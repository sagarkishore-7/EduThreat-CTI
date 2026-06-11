"""Shared threat-actor and ransomware-family normalization for v2.

Threat-actor identity (canonicalization + generic detection) now lives in the single
source of truth ``edu_cti.core.actor_identity`` — importable by both this module and
``edu_cti.analysis.campaign_correlation`` without a reverse dependency. This module
re-exports the actor helpers (so existing imports keep working) and owns the separate
ransomware-family normalization.
"""

from __future__ import annotations

from typing import Optional

from src.edu_cti.core.actor_identity import (  # noqa: F401  (re-exported)
    canonical_actor_name,
    is_generic_actor,
    _lookup_candidate_keys,
    _strip_descriptor_suffix_text,
)

# Back-compat public name: the threat-actor resolver lives in actor_identity now.
normalize_threat_actor_name = canonical_actor_name


_RANSOMWARE_FAMILY_ALIASES: dict[str, str] = {
    # Cl0p
    "cl0p": "Cl0p",
    "clop": "Cl0p",
    "cl0p_clop": "Cl0p",
    "cl0p/clop": "Cl0p",
    "cl0p clop": "Cl0p",
    # LockBit family variants stay distinct when versioned.
    "lockbit": "LockBit",
    "lock_bit": "LockBit",
    "lockbit 1.0": "LockBit 1.0",
    "lockbit 2.0": "LockBit 2.0",
    "lockbit 3.0": "LockBit 3.0",
    "lockbit_2": "LockBit 2.0",
    "lockbit2": "LockBit 2.0",
    "lockbit_2_0": "LockBit 2.0",
    "lockbit_3": "LockBit 3.0",
    "lockbit3": "LockBit 3.0",
    "lockbit_3_0": "LockBit 3.0",
    "lockbit black": "LockBit 3.0",
    "lockbit_black": "LockBit 3.0",
    "lockbit green": "LockBit 2.0",
    "lockbit_green": "LockBit 2.0",
    "lockbit red": "LockBit 1.0",
    "lockbit_red": "LockBit 1.0",
    # BlackCat / ALPHV
    "blackcat": "BlackCat/ALPHV",
    "alphv": "BlackCat/ALPHV",
    "blackcat_alphv": "BlackCat/ALPHV",
    "alphv_blackcat": "BlackCat/ALPHV",
    "blackcat/alphv": "BlackCat/ALPHV",
    "black_cat": "BlackCat/ALPHV",
    # Black Basta
    "blackbasta": "Black Basta",
    "black_basta": "Black Basta",
    # Vice Society
    "vice_society": "Vice Society",
    # DoppelPaymer
    "doppelpaymer": "DoppelPaymer",
    "dopplepaymer": "DoppelPaymer",
    "dopplerpaymer": "DoppelPaymer",
    "doppel_paymer": "DoppelPaymer",
    # BabLock / Rorschach
    "bablock_rorschach": "BabLock/Rorschach",
    "bablock": "BabLock/Rorschach",
    "rorschach": "BabLock/Rorschach",
    # REvil / Sodinokibi
    "revil": "REvil",
    "sodinokibi": "REvil",
    "r_evil": "REvil",
    "revil_sodinokibi": "REvil",
    # NetWalker
    "netwalker": "NetWalker",
    "net_walker": "NetWalker",
    # TrickBot
    "trickbot": "TrickBot",
    "trick_bot": "TrickBot",
    # RansomHub
    "ransomhub": "RansomHub",
    "ransom_hub": "RansomHub",
    # AvosLocker
    "avoslocker": "AvosLocker",
    "avos_locker": "AvosLocker",
    # INC Ransom family
    "inc": "INC Ransom",
    "inc_ransom": "INC Ransom",
    "inc_ransomware": "INC Ransom",
    # Straight canonicalization
    "gandcrab": "GandCrab",
    "gand_crab": "GandCrab",
    "medusa": "Medusa",
    "ryuk": "Ryuk",
    "rhysida": "Rhysida",
    "akira": "Akira",
    "conti": "Conti",
    "hive": "Hive",
    "royal": "Royal",
    "fog": "Fog",
    "qilin": "Qilin",
    "snatch": "Snatch",
    "maze": "Maze",
    "monti": "Monti",
    "interlock": "Interlock",
    "funksec": "FunkSec",
    "avaddon": "Avaddon",
    "blacksuit": "BlackSuit",
    "black_suit": "BlackSuit",
    "sinobi": "Sinobi",
    "ako": "AKO",
    "cuba": "Cuba",
    "bianlian": "BianLian",
    "blacklock": "BlackLock",
    "darkbit": "DarkBit",
    "meow": "Meow",
    "noescape": "NoEscape",
    "nova": "Nova",
    "phobos": "Phobos",
    "pysa": "PYSA",
    "radiant": "Radiant",
    "ransomhouse": "RansomHouse",
    "safepay": "SafePay",
    "trigona": "Trigona",
}

_UNKNOWN_FAMILY_VALUES = {
    "",
    "unknown",
    "none",
    "not_applicable",
    "n/a",
    "na",
    "unspecified",
}


def _normalize_lockbit_family(candidate: str) -> Optional[str]:
    if not candidate.startswith("lockbit"):
        return None
    if any(token in candidate for token in ("3_0", "lockbit3", "lockbit_3", "black")):
        return "LockBit 3.0"
    if any(token in candidate for token in ("2_0", "lockbit2", "lockbit_2", "green")):
        return "LockBit 2.0"
    if any(token in candidate for token in ("1_0", "lockbit1", "lockbit_1", "red")):
        return "LockBit 1.0"
    return "LockBit"


def normalize_ransomware_family(name: Optional[str]) -> Optional[str]:
    if not name or not name.strip():
        return None

    for candidate in _lookup_candidate_keys(name):
        if candidate in _UNKNOWN_FAMILY_VALUES:
            return None
        canonical = _RANSOMWARE_FAMILY_ALIASES.get(candidate)
        if canonical:
            return canonical
        lockbit = _normalize_lockbit_family(candidate)
        if lockbit:
            return lockbit

    stripped = _strip_descriptor_suffix_text(name.strip()) or name.strip()
    return stripped or None
