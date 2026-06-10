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
    "hacking",
    "affiliate",
    "affiliates",
    "criminal",
    "criminals",
    "cybercriminal",
    "cybercriminals",
    "cybercrime",
    "cybercrimes",
    "syndicate",
    "actor",
    "actors",
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
    "criminal",
    "criminals",
    "cyber_criminal",
    "cyber_criminals",
    "cyber_extortion",
    "cyber_extortionist",
    "cyber_extortionists",
    "extortion",
    "extortion_group",
    "extortion_gang",
    "extortionist",
    "extortionists",
    "ransomware_group",
    "ransomware_gang",
    "ransomware_operator",
    "ransomware_operators",
    "ransomware_affiliate",
    "ransomware_affiliates",
    "attacker",
    "attackers",
    "unauthorized_actor",
    "unauthorised_actor",
    "unidentified",
    "unidentified_actor",
    "unidentified_actors",
    "unidentified_group",
    "unnamed",
    "unnamed_actor",
    "unnamed_group",
}

_GENERIC_GEOPOLITICAL_ACTOR_VALUES = {
    "china",
    "chinese",
    "iran",
    "iranian",
    "north_korea",
    "north_korean",
    "pro_russian",
    "russia",
    "russian",
    "state_backed",
}

# Generic word *tokens* stripped when deciding whether an actor label is purely
# descriptive. After removing these, a label with NOTHING left is generic; one with a
# real name surviving ("Clop" in "Clop cybercriminal") is kept. Geo tokens are listed
# in their split single-word form (the label is split on "_" before matching).
_GENERIC_ACTOR_TOKENS = set(_THREAT_ACTOR_DESCRIPTOR_SUFFIXES) | {
    "cyber",
    "cyberattack",
    "threat",
    "malicious",
    "unknown",
    "unidentified",
    "unnamed",
    "suspected",
    "foreign",
    "pro",
    "unauthorized",
    "unauthorised",
    "attacker",
    "attackers",
    "china",
    "chinese",
    "iran",
    "iranian",
    "north",
    "korea",
    "korean",
    "russia",
    "russian",
    "state",
    "backed",
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


def _strip_descriptor_suffix_text(name: str) -> str:
    parts = [part for part in name.strip().split() if part]
    while parts and _normalized_lookup_key(parts[-1]) in _THREAT_ACTOR_DESCRIPTOR_SUFFIXES:
        parts.pop()
    return " ".join(parts).strip()


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


def is_generic_actor(name: Optional[str]) -> bool:
    """True when an actor label is a generic description, not an attribution.

    A known actor (resolves to a `_THREAT_ACTOR_ALIASES` entry or a LockBit variant) is
    never generic. Otherwise we strip every generic word token (`_GENERIC_ACTOR_TOKENS`)
    from the label: if NOTHING meaningful remains it is generic ("criminal",
    "Russian cyber-extortion group", "threat actors"), but if a real name survives it is
    kept ("Clop cybercriminal" → keeps "clop", "Fog hacking" → keeps "fog").
    """
    if not name or not name.strip():
        return True

    for candidate in _lookup_candidate_keys(name):
        if candidate in _THREAT_ACTOR_ALIASES or candidate.startswith("lockbit"):
            return False
        if candidate in _UNKNOWN_THREAT_ACTOR_VALUES:
            return True

    tokens = [t for t in _normalized_lookup_key(name).split("_") if t]
    core = [t for t in tokens if t not in _GENERIC_ACTOR_TOKENS]
    if not core:
        return True
    core_key = "_".join(core)
    return core_key in _UNKNOWN_THREAT_ACTOR_VALUES or core_key in _GENERIC_GEOPOLITICAL_ACTOR_VALUES


def normalize_threat_actor_name(name: Optional[str]) -> Optional[str]:
    if not name or not name.strip():
        return None
    if is_generic_actor(name):
        return None

    stripped = name.strip()
    for candidate in _lookup_candidate_keys(stripped):
        canonical = _THREAT_ACTOR_ALIASES.get(candidate)
        if canonical:
            return canonical
        if candidate.startswith("lockbit"):
            return "LockBit"

    reduced = _strip_descriptor_suffix_text(stripped)
    if reduced and reduced != stripped:
        return reduced

    return stripped
