"""Single source of truth for threat-actor identity.

Both ``edu_cti.analysis.campaign_correlation`` and ``edu_cti_v2.normalization`` import
from here, so actor canonicalization and generic-label detection are defined ONCE
(previously duplicated, with a "keep in sync" comment that drifted). This module lives in
``edu_cti.core`` — the base package — so neither importer creates a reverse dependency.

Two public entry points:
  * ``canonical_actor_name(name)`` — resolve any raw label to its canonical surface form
    (collapsing aliases, descriptor suffixes, person-name middle initials, and pure
    spacing/case variants), or ``None`` if the label is a generic non-attribution.
  * ``is_generic_actor(name)`` — True when a label is a generic description, not a name.

``ACTOR_TEXT_ALIASES`` is the curated, high-precision set used to scan article *prose*
for actor mentions (distinct from the broad normalize map, which would over-match in text).
"""

from __future__ import annotations

import re
from typing import Optional

# Descriptor tokens stripped from the tail of an actor label ("Skira Team" -> "Skira",
# "Clop ransomware syndicate" -> "Clop"). Shared with ransomware-family normalization.
ACTOR_DESCRIPTOR_SUFFIXES = (
    "ransomware",
    "ransom",
    "extortion",
    "gang",
    "group",
    "team",
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

# Alias key (normalized) -> canonical surface form. Resolves spelling/spacing variants of
# KNOWN actors. (Ransomware-family-version detail is handled separately in
# edu_cti_v2.normalization; the actor stays broad, e.g. every LockBit variant -> "LockBit".)
THREAT_ACTOR_ALIASES: dict[str, str] = {
    # Cl0p
    "cl0p": "Cl0p", "clop": "Cl0p", "cl0p_clop": "Cl0p", "cl0p/clop": "Cl0p", "cl0p clop": "Cl0p",
    # LockBit (actor stays broad even when family versions differ)
    "lockbit": "LockBit", "lock_bit": "LockBit", "lockbit_2": "LockBit", "lockbit_3": "LockBit",
    "lockbit2": "LockBit", "lockbit3": "LockBit", "lockbit_black": "LockBit",
    "lockbit_green": "LockBit", "lockbit_red": "LockBit",
    # BlackCat / ALPHV
    "blackcat": "BlackCat/ALPHV", "alphv": "BlackCat/ALPHV", "blackcat_alphv": "BlackCat/ALPHV",
    "alphv_blackcat": "BlackCat/ALPHV", "blackcat/alphv": "BlackCat/ALPHV", "black_cat": "BlackCat/ALPHV",
    # Black Basta
    "blackbasta": "Black Basta", "black_basta": "Black Basta",
    # Vice Society
    "vice_society": "Vice Society", "vice society": "Vice Society",
    # DoppelPaymer
    "doppelpaymer": "DoppelPaymer", "dopplepaymer": "DoppelPaymer", "dopplerpaymer": "DoppelPaymer",
    "doppel_paymer": "DoppelPaymer",
    # BabLock / Rorschach
    "bablock_rorschach": "BabLock/Rorschach", "bablock": "BabLock/Rorschach", "rorschach": "BabLock/Rorschach",
    # REvil / Sodinokibi
    "revil": "REvil", "sodinokibi": "REvil", "r_evil": "REvil", "revil_sodinokibi": "REvil",
    # NetWalker
    "netwalker": "NetWalker", "net_walker": "NetWalker",
    # TrickBot
    "trickbot": "TrickBot", "trick_bot": "TrickBot",
    # RansomHub
    "ransomhub": "RansomHub", "ransom_hub": "RansomHub",
    # AvosLocker
    "avoslocker": "AvosLocker", "avos_locker": "AvosLocker",
    # INC
    "inc": "INC", "inc_ransom": "INC", "inc_ransomware": "INC",
    # ShinyHunters (data-extortion group; both spacings collapse)
    "shinyhunters": "ShinyHunters", "shiny_hunters": "ShinyHunters", "shiny hunters": "ShinyHunters",
    # Other curated single-name canonicalizations
    "gandcrab": "GandCrab", "gand_crab": "GandCrab",
    "medusa": "Medusa", "ryuk": "Ryuk", "rhysida": "Rhysida", "akira": "Akira", "conti": "Conti",
    "hive": "Hive", "royal": "Royal", "fog": "Fog", "qilin": "Qilin", "snatch": "Snatch",
    "maze": "Maze", "monti": "Monti", "interlock": "Interlock", "funksec": "FunkSec",
    "avaddon": "Avaddon", "blacksuit": "BlackSuit", "black_suit": "BlackSuit", "sinobi": "Sinobi",
    "ako": "AKO", "cuba": "Cuba", "bianlian": "BianLian", "bian_lian": "BianLian",
    "blacklock": "BlackLock", "darkbit": "DarkBit", "meow": "Meow", "noescape": "NoEscape",
    "nova": "Nova", "phobos": "Phobos", "pysa": "PYSA", "radiant": "Radiant",
    "ransomhouse": "RansomHouse", "safepay": "SafePay", "trigona": "Trigona",
    "play": "Play", "play_ransomware": "Play", "skira": "Skira", "skira_team": "Skira",
}

# Generic / junk actor values (normalized) that are never an attribution.
UNKNOWN_THREAT_ACTOR_VALUES = {
    "", "unknown", "unknown_actor", "unknown_actors", "unknown_group", "unknown_gang",
    "unknown_hackers", "cybercriminal", "cybercriminals", "cybercriminal_group",
    "cybercriminal_gang", "cybercriminal_collective", "foreign_hacking_group", "hacker",
    "hackers", "hacktivist", "hacktivists", "malicious_actor", "malicious_actors", "pro_russian",
    "pro_russian_hackers", "russian_hackers", "suspected_hackers", "threat_actor", "threat_actors",
    "unknown_ransomware_gang", "criminal", "criminals", "cyber_criminal", "cyber_criminals",
    "cyber_extortion", "cyber_extortionist", "cyber_extortionists", "extortion", "extortion_group",
    "extortion_gang", "extortionist", "extortionists", "ransomware_group", "ransomware_gang",
    "ransomware_operator", "ransomware_operators", "ransomware_affiliate", "ransomware_affiliates",
    "attacker", "attackers", "unauthorized_actor", "unauthorised_actor", "unidentified",
    "unidentified_actor", "unidentified_actors", "unidentified_group", "unnamed", "unnamed_actor",
    "unnamed_group",
}

# Bare nationality / state labels are not attributions on their own.
GENERIC_GEOPOLITICAL_ACTOR_VALUES = {
    "china", "chinese", "iran", "iranian", "north_korea", "north_korean", "pro_russian",
    "russia", "russian", "state_backed",
}

# Generic word *tokens*: strip these from a label; if NOTHING real remains the label is
# generic ("Russian cyber-extortion group"), but a surviving name is kept ("Clop" in
# "Clop cybercriminal").
GENERIC_ACTOR_TOKENS = set(ACTOR_DESCRIPTOR_SUFFIXES) | {
    "cyber", "cyberattack", "threat", "malicious", "unknown", "unidentified", "unnamed",
    "suspected", "foreign", "pro", "unauthorized", "unauthorised", "attacker", "attackers",
    "china", "chinese", "iran", "iranian", "north", "korea", "korean", "russia", "russian",
    "state", "backed",
}

# Curated high-precision aliases for scanning article PROSE (canonical -> alias tuple).
# Kept tight on purpose: these names appear verbatim in text with low false-match risk.
ACTOR_TEXT_ALIASES: dict[str, tuple[str, ...]] = {
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


def _normalized_lookup_key(name: str) -> str:
    normalized = name.strip().lower().replace("/", "_").replace("-", "_").replace(" ", "_")
    return "_".join(part for part in normalized.split("_") if part)


def _tight_key(name: str) -> str:
    """Alphanumerics-only lowercase key: collapses spacing/punctuation variants so
    "Shiny Hunters" and "ShinyHunters" share a key (but "Cl0p" != "Clop")."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


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
        while tokens and tokens[-1] in ACTOR_DESCRIPTOR_SUFFIXES:
            tokens = tokens[:-1]
            _add("_".join(tokens))
    return candidates


def _strip_descriptor_suffix_text(name: str) -> str:
    parts = [part for part in name.strip().split() if part]
    while parts and _normalized_lookup_key(parts[-1]) in ACTOR_DESCRIPTOR_SUFFIXES:
        parts.pop()
    return " ".join(parts).strip()


_MIDDLE_INITIAL_RE = re.compile(r"^[A-Za-z]\.?$")


def _normalize_person_name(name: str) -> str:
    """Drop single-letter middle initials so "Matthew D. Lane" == "Matthew Lane".

    Only interior tokens are dropped (first and last are always kept), and only for
    multi-word labels — group names rarely carry a single-letter middle token."""
    parts = name.split()
    if len(parts) < 3:
        return name
    kept = [parts[0]] + [p for p in parts[1:-1] if not _MIDDLE_INITIAL_RE.match(p)] + [parts[-1]]
    return " ".join(kept)


def is_generic_actor(name: Optional[str]) -> bool:
    """True when an actor label is a generic description, not an attribution.

    A known actor (resolves to an alias or a LockBit variant) is never generic. Otherwise
    strip the generic tokens: nothing left => generic; a real name surviving => kept."""
    if not name or not name.strip():
        return True
    for candidate in _lookup_candidate_keys(name):
        if candidate in THREAT_ACTOR_ALIASES or candidate.startswith("lockbit"):
            return False
        if candidate in UNKNOWN_THREAT_ACTOR_VALUES:
            return True
    tokens = [t for t in _normalized_lookup_key(name).split("_") if t]
    core = [t for t in tokens if t not in GENERIC_ACTOR_TOKENS]
    if not core:
        return True
    core_key = "_".join(core)
    return core_key in UNKNOWN_THREAT_ACTOR_VALUES or core_key in GENERIC_GEOPOLITICAL_ACTOR_VALUES


# Static, deterministic tight-key -> canonical-surface-form map (no run-time mutation):
# built from the canonical names so e.g. "shiny hunters" collapses to "ShinyHunters".
_CANONICAL_BY_TIGHT_KEY: dict[str, str] = {}
for _canon in set(THREAT_ACTOR_ALIASES.values()) | set(ACTOR_TEXT_ALIASES.keys()):
    _CANONICAL_BY_TIGHT_KEY.setdefault(_tight_key(_canon), _canon)


def canonical_actor_name(name: Optional[str]) -> Optional[str]:
    """Resolve a raw actor label to its canonical surface form, or None if generic.

    Order: generic check -> alias resolve -> descriptor-suffix strip + person-name
    normalization -> static tight-key collapse against known canonicals -> cleaned label.
    Deterministic (no stateful registry), so re-running correlation is stable."""
    if not name or not name.strip():
        return None
    if is_generic_actor(name):
        return None
    # 1. Known alias (handles cl0p/clop, lockbit variants, shiny hunters, etc.)
    for candidate in _lookup_candidate_keys(name):
        canonical = THREAT_ACTOR_ALIASES.get(candidate)
        if canonical:
            return canonical
        if candidate.startswith("lockbit"):
            return "LockBit"
    # 2. Clean: strip trailing descriptors, drop person middle initials.
    cleaned = _normalize_person_name(_strip_descriptor_suffix_text(name.strip()) or name.strip())
    # 2b. The cleaned form may now be a known alias ("Skira Team" -> "Skira").
    for candidate in _lookup_candidate_keys(cleaned):
        canonical = THREAT_ACTOR_ALIASES.get(candidate)
        if canonical:
            return canonical
    # 3. Static tight-key collapse onto a known canonical surface form.
    collapsed = _CANONICAL_BY_TIGHT_KEY.get(_tight_key(cleaned))
    if collapsed:
        return collapsed
    return cleaned
