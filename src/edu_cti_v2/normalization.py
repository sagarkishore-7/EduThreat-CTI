"""Shared threat actor and ransomware-family normalization for v2."""

from __future__ import annotations

from typing import Optional

_THREAT_ACTOR_DESCRIPTOR_SUFFIXES = (
    "ransomware",
    "gang",
    "group",
    "operation",
    "operations",
    "operator",
    "operators",
    "collective",
    "crew",
)

_CANONICAL_ACTOR_AND_FAMILY_ALIASES: dict[str, str] = {
    # Cl0p
    "cl0p": "Cl0p",
    "clop": "Cl0p",
    "cl0p_clop": "Cl0p",
    "cl0p/clop": "Cl0p",
    "cl0p clop": "Cl0p",
    # LockBit
    "lockbit": "LockBit",
    "lock_bit": "LockBit",
    "lockbit 2.0": "LockBit",
    "lockbit 3.0": "LockBit",
    "lockbit2": "LockBit",
    "lockbit3": "LockBit",
    "lockbit_2": "LockBit",
    "lockbit_3": "LockBit",
    # BlackCat / ALPHV
    "blackcat": "BlackCat/ALPHV",
    "alphv": "BlackCat/ALPHV",
    "blackcat_alphv": "BlackCat/ALPHV",
    "alphv_blackcat": "BlackCat/ALPHV",
    "blackcat/alphv": "BlackCat/ALPHV",
    "black cat": "BlackCat/ALPHV",
    "black_cat": "BlackCat/ALPHV",
    # Black Basta
    "blackbasta": "Black Basta",
    "black_basta": "Black Basta",
    "black basta": "Black Basta",
    # Vice Society
    "vice_society": "Vice Society",
    "vice society": "Vice Society",
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
    # INC Ransom
    "inc": "INC Ransom",
    "inc_ransom": "INC Ransom",
    "inc ransom": "INC Ransom",
    "inc_ransomware": "INC Ransom",
    # Known straight canonicalization
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


def _normalized_lookup_key(name: str) -> str:
    normalized = (
        name.strip()
        .lower()
        .replace("/", "_")
        .replace("-", "_")
        .replace(" ", "_")
    )
    return "_".join(part for part in normalized.split("_") if part)


def _lookup_candidate_keys(name: Optional[str], *, strip_actor_suffixes: bool = True) -> list[str]:
    if not name or not name.strip():
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def _add(value: Optional[str]) -> None:
        if value and value not in seen:
            seen.add(value)
            candidates.append(value)

    stripped = name.strip()
    _add(stripped.lower())

    normalized_key = _normalized_lookup_key(stripped)
    _add(normalized_key)

    if strip_actor_suffixes and normalized_key:
        tokens = normalized_key.split("_")
        while tokens and tokens[-1] in _THREAT_ACTOR_DESCRIPTOR_SUFFIXES:
            tokens = tokens[:-1]
            _add("_".join(tokens))

    return candidates


def normalize_ransomware_family(name: Optional[str]) -> Optional[str]:
    if not name or not name.strip():
        return None

    for candidate in _lookup_candidate_keys(name):
        if candidate in _UNKNOWN_FAMILY_VALUES:
            return None
        canonical = _CANONICAL_ACTOR_AND_FAMILY_ALIASES.get(candidate)
        if canonical:
            return canonical

    stripped = name.strip()
    return stripped or None


def normalize_threat_actor_name(name: Optional[str]) -> Optional[str]:
    if not name or not name.strip():
        return None

    stripped = name.strip()
    for candidate in _lookup_candidate_keys(stripped):
        canonical = _CANONICAL_ACTOR_AND_FAMILY_ALIASES.get(candidate)
        if canonical:
            return canonical
    return stripped
