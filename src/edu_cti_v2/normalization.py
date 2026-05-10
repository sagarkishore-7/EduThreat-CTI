"""Shared threat actor and ransomware-family normalization for v2."""

from __future__ import annotations

from typing import Optional

_THREAT_ACTOR_DESCRIPTOR_SUFFIXES = (
    "ransomware",
    "ransom",
    "extortion",
    "gang",
    "group",
    "operation",
    "operations",
    "operator",
    "operators",
    "collective",
    "crew",
    "hacker",
    "hackers",
    "affiliate",
    "affiliates",
)

_THREAT_ACTOR_ALIASES: dict[str, str] = {
    # Cl0p
    "cl0p": "Cl0p",
    "clop": "Cl0p",
    "cl0p_clop": "Cl0p",
    "cl0p/clop": "Cl0p",
    "cl0p clop": "Cl0p",
    # LockBit actor stays broad even when family versions differ.
    "lockbit": "LockBit",
    "lock_bit": "LockBit",
    "lockbit_2": "LockBit",
    "lockbit_3": "LockBit",
    "lockbit2": "LockBit",
    "lockbit3": "LockBit",
    "lockbit_black": "LockBit",
    "lockbit_green": "LockBit",
    "lockbit_red": "LockBit",
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
    # INC
    "inc": "INC",
    "inc_ransom": "INC",
    "inc_ransomware": "INC",
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

_UNKNOWN_THREAT_ACTOR_VALUES = {
    "",
    "unknown",
    "unknown_actor",
    "unknown_actors",
    "unknown_group",
    "unknown_gang",
    "unknown_hackers",
    "cybercriminal",
    "cybercriminals",
    "cybercriminal_group",
    "cybercriminal_gang",
    "cybercriminal_collective",
    "foreign_hacking_group",
    "hacker",
    "hackers",
    "hacktivist",
    "hacktivists",
    "malicious_actor",
    "malicious_actors",
    "pro_russian",
    "pro_russian_hackers",
    "russian_hackers",
    "suspected_hackers",
    "threat_actor",
    "threat_actors",
    "unknown_ransomware_gang",
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

    stripped = name.strip()
    return stripped or None


def normalize_threat_actor_name(name: Optional[str]) -> Optional[str]:
    if not name or not name.strip():
        return None

    stripped = name.strip()
    for candidate in _lookup_candidate_keys(stripped):
        if candidate in _UNKNOWN_THREAT_ACTOR_VALUES:
            return None
        canonical = _THREAT_ACTOR_ALIASES.get(candidate)
        if canonical:
            return canonical
        if candidate.startswith("lockbit"):
            return "LockBit"
    return stripped
