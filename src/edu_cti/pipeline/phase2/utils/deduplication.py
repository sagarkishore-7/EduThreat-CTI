"""
Post-enrichment deduplication for EduThreat-CTI.

Handles duplicate narratives of the same incident across different sources.
The same matching logic is reused by both the running pipeline and the admin
dedup endpoint so 14-day institution dedup behaves consistently everywhere.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None

try:
    from thefuzz import fuzz
except ImportError:
    fuzz = None

logger = logging.getLogger(__name__)

_DETAIL_SCORE_SQL = """
(
    COALESCE(ef.timeline_events_count, 0) * 5 +
    COALESCE(ef.mitre_techniques_count, 0) * 3 +
    CASE WHEN i.primary_url IS NOT NULL AND i.primary_url != '' THEN 2 ELSE 0 END +
    CASE WHEN COALESCE(ef.enriched_summary, i.llm_summary) IS NOT NULL
              AND LENGTH(COALESCE(ef.enriched_summary, i.llm_summary)) > 0 THEN 2 ELSE 0 END +
    CASE WHEN ef.attack_category IS NOT NULL AND ef.attack_category != '' THEN 1 ELSE 0 END +
    CASE WHEN ef.threat_actor_name IS NOT NULL AND ef.threat_actor_name != '' THEN 1 ELSE 0 END +
    CASE WHEN ef.ransomware_family IS NOT NULL AND ef.ransomware_family != '' THEN 1 ELSE 0 END +
    CASE WHEN ef.data_breached IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN ef.data_exfiltrated IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN ef.records_affected_exact IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN ef.users_affected_exact IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN ef.ransom_amount IS NOT NULL THEN 1 ELSE 0 END
)
"""

_HEADLINE_AFTER_RE = re.compile(
    r"\b(?:targets?|targeted|hits?|attacks?|attacked|breach(?:es)?|breached|"
    r"compromises?|compromised|victimizes?|victimized|affects?|affected|"
    r"impacts?|impacted|strikes?|struck|hacks?|hacked)\s+(?P<victim>.+)$",
    re.IGNORECASE,
)
_HEADLINE_BEFORE_RE = re.compile(
    r"^(?P<victim>.+?)\s+(?:suffers?|hit by|targeted by|attacked by|"
    r"breached by|compromised by|falls victim to|impacted by)\b.*$",
    re.IGNORECASE,
)
_EDU_KEYWORD_RE = re.compile(
    r"\b(?:university|college|school|school district|independent school district|"
    r"academy|institute|seminary|district|isd|board of education|education department|"
    r"community college|polytechnic)\b",
    re.IGNORECASE,
)
_ATTACK_TERM_RE = re.compile(
    r"\b(?:ransomware|cyberattack|cyber attack|cyber incident|attack|breach|breached|"
    r"hack|hacked|malware|targets?|targeted|hits?|hit|claims?|claimed|compromised|"
    r"incident|victimizes?|victimized|data leak|data breach)\b",
    re.IGNORECASE,
)
_ENTITY_PATTERNS = (
    # "Name Type" pattern — e.g. "Alamo Heights School District", "Los Angeles Community College".
    # Intentionally NOT re.IGNORECASE so that the leading [A-Z] anchor rejects attack
    # verbs like "hits", "ransomware", "attack" (which start with lowercase letters).
    re.compile(
        r"[A-Z][A-Za-z0-9&'.,-]*(?:\s+[A-Z][A-Za-z0-9&'.,-]*)*\s+"
        r"(?:Independent\s+School\s+Districts?|School\s+Districts?|Community\s+Colleges?|"
        r"Public\s+Schools?|"
        r"University(?:\s+System)?|Colleges?|Schools?|Academy|Seminary|ISD|"
        r"Boards?\s+of\s+Education|Education\s+Departments?|Polytechnics?|"
        r"Institutes?(?:\s+of(?:\s+[A-Z][A-Za-z0-9&'.,-]*)+)?)"
        r"\b",
    ),
    # "University/College of Name" pattern — e.g. "University of Kentucky", "College of William and Mary"
    re.compile(
        r"\b(?:University|College|Institute|School)\s+of\s+"
        r"[A-Z][A-Za-z0-9&'.,-]*(?:\s+[A-Za-z][A-Za-z0-9&'.,-]*)*",
    ),
)
_PREFIX_PATTERNS = [
    r"^university\s+of\s+",
    r"^the\s+university\s+of\s+",
    r"^the\s+",
    r"^university\s+",
    r"^college\s+of\s+",
    r"^college\s+",
    r"^school\s+of\s+",
    r"^school\s+",
]
_SUFFIX_PATTERNS = [
    r"\s+university$",
    r"\s+college$",
    r"\s+school$",
    r"\s+institute$",
    r"\s+university\s+system$",
]
_STOP_TOKENS = {
    "the",
    "of",
    "at",
    "for",
    "and",
}


def clean_institution_name(name: Optional[str]) -> str:
    """
    Strip headline-style attack wrappers and keep the victim institution label.

    Example:
      "Qilin Ransomware Targets Alamo Heights School District"
      -> "Alamo Heights School District"
    """
    if not name:
        return ""

    text = str(name).strip().strip("\"'“”")
    text = re.sub(r"\s+", " ", text)
    if not text:
        return ""
    if not _ATTACK_TERM_RE.search(text):
        return text

    candidates: List[str] = []

    for pattern in _ENTITY_PATTERNS:
        for match in pattern.finditer(text):
            value = match.group(0).strip(" -,:;")
            if value:
                candidates.append(value)

    # Use the LAST match so "attack hits Victim" picks "Victim" (via "hits"), not "hits Victim"
    after_matches = list(_HEADLINE_AFTER_RE.finditer(text))
    if after_matches:
        victim = after_matches[-1].group("victim")
        # Strip leading prepositions: "on Baltimore City..." → "Baltimore City..."
        victim = re.sub(r"^(?:on|at|in|for|from|of|against|the)\s+", "", victim, flags=re.IGNORECASE)
        candidates.append(victim.strip(" -,:;"))

    before_match = _HEADLINE_BEFORE_RE.search(text)
    if before_match:
        candidates.append(before_match.group("victim").strip(" -,:;"))

    if not candidates:
        return text

    def _score(candidate: str) -> tuple[int, int]:
        score = 0
        if _EDU_KEYWORD_RE.search(candidate):
            score += 50
        if not _ATTACK_TERM_RE.search(candidate):
            score += 25
        if len(candidate) < len(text):
            score += 10
        # Negate length so max() prefers shorter strings on equal score (more specific name wins)
        return (score, -len(candidate))

    best = max(candidates, key=_score)
    return best or text


def normalize_institution_name(name: str) -> str:
    """
    Normalize institution name for comparison.
    """
    if not name:
        return ""

    import re as _re

    normalized = clean_institution_name(name).lower().strip()
    if not normalized:
        return ""

    if _re.search(r"\.", normalized) and " " not in normalized:
        normalized = normalized.split(".")[0]

    for prefix in _PREFIX_PATTERNS:
        normalized = _re.sub(prefix, "", normalized)
    for suffix in _SUFFIX_PATTERNS:
        normalized = _re.sub(suffix, "", normalized)

    normalized = _re.sub(r"[^\w\s-]", "", normalized)
    normalized = _re.sub(r"\s+", " ", normalized).strip()
    return normalized


def choose_best_institution_name(*names: Optional[str]) -> Optional[str]:
    """
    Pick the cleanest, most institution-like label from a set of candidates.
    """
    scored: List[tuple[int, int, str]] = []
    for raw in names:
        cleaned = clean_institution_name(raw)
        if not cleaned:
            continue
        score = 0
        if _EDU_KEYWORD_RE.search(cleaned):
            score += 30
        if not _ATTACK_TERM_RE.search(cleaned):
            score += 20
        score += len(_core_tokens(cleaned)) * 5
        scored.append((score, len(cleaned), cleaned))

    if not scored:
        return None
    return max(scored)[2]


def _core_tokens(name: str) -> set[str]:
    normalized = normalize_institution_name(name)
    if not normalized:
        return set()
    return {
        token
        for token in normalized.split()
        if token and token not in _STOP_TOKENS and len(token) > 1
    }


def institution_names_match(name1: str, name2: str, threshold: int = 85) -> bool:
    """
    Compare institution names using cleaning, token overlap, and fuzzy fallback.
    """
    n1 = normalize_institution_name(name1)
    n2 = normalize_institution_name(name2)
    if not n1 or not n2:
        return False
    if n1 == n2:
        return True

    tokens1 = _core_tokens(name1)
    tokens2 = _core_tokens(name2)
    if tokens1 and tokens2:
        if tokens1 == tokens2:
            return True
        smaller, larger = sorted((tokens1, tokens2), key=len)
        # Subset match: require >=2 tokens AND coverage ratio >=0.7 to avoid
        # false positives like "Los Angeles" matching "Los Angeles Unified School District"
        if len(smaller) >= 2 and smaller.issubset(larger) and len(smaller) / len(larger) >= 0.7:
            return True

    if fuzz is not None:
        sorted_1 = " ".join(sorted(tokens1 or n1.split()))
        sorted_2 = " ".join(sorted(tokens2 or n2.split()))
        return max(
            fuzz.token_sort_ratio(n1, n2),
            fuzz.token_sort_ratio(sorted_1, sorted_2),
        ) >= threshold

    seq_score = SequenceMatcher(None, n1, n2).ratio() * 100
    token_score = SequenceMatcher(
        None,
        " ".join(sorted(tokens1 or n1.split())),
        " ".join(sorted(tokens2 or n2.split())),
    ).ratio() * 100
    return max(seq_score, token_score) >= threshold


def _names_match_pair(
    n1: str,
    t1: set,
    n2: str,
    t2: set,
    threshold: int = 85,
) -> bool:
    """Return True if a single normalized-name/token pair matches another."""
    if not n1 or not n2:
        return False
    if n1 == n2:
        return True
    if t1 and t2:
        if t1 == t2:
            return True
        smaller, larger = sorted((t1, t2), key=len)
        if len(smaller) >= 2 and smaller.issubset(larger) and len(smaller) / len(larger) >= 0.7:
            return True
    if fuzz is not None:
        s1 = " ".join(sorted(t1 or n1.split()))
        s2 = " ".join(sorted(t2 or n2.split()))
        return max(fuzz.token_sort_ratio(n1, n2), fuzz.token_sort_ratio(s1, s2)) >= threshold
    seq = SequenceMatcher(None, n1, n2).ratio() * 100
    tok = SequenceMatcher(
        None,
        " ".join(sorted(t1 or n1.split())),
        " ".join(sorted(t2 or n2.split())),
    ).ratio() * 100
    return max(seq, tok) >= threshold


def parse_incident_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse incident date string to datetime object.
    """
    if not date_str:
        return None

    try:
        if date_parser:
            parsed = date_parser.parse(date_str)
            if parsed.tzinfo is not None:
                return parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed.replace(tzinfo=None)
    except (ValueError, TypeError):
        pass

    for fmt, length in (("%Y-%m-%d", 10), ("%Y-%m", 7), ("%Y", 4)):
        try:
            return datetime.strptime(str(date_str)[:length], fmt)
        except (ValueError, TypeError):
            continue

    logger.warning(f"Could not parse date: {date_str}")
    return None


def dates_within_window(
    date1: Optional[datetime],
    date2: Optional[datetime],
    days: int = 14,
) -> bool:
    """
    Check if two dates are within specified window.
    """
    if not date1 and not date2:
        return True
    if not date1 or not date2:
        return True

    d1 = date1.replace(tzinfo=None)
    d2 = date2.replace(tzinfo=None)
    return abs((d1 - d2).days) <= days


def find_duplicate_institutions(
    conn: sqlite3.Connection,
    incident_id: str,
    institution_name: Optional[str],
    incident_date: Optional[str],
    window_days: int = 14,
    name_threshold: int = 85,
) -> List[Dict[str, Any]]:
    """
    Find enriched incidents with the same institution within the date window.
    """
    if not institution_name:
        return []

    incident_dt = parse_incident_date(incident_date)
    cur = conn.execute(
        """
        SELECT i.incident_id,
               COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) AS institution_name,
               i.incident_date,
               i.llm_summary
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_id != ?
          AND i.llm_enriched = 1
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) IS NOT NULL
        """,
        (incident_id,),
    )

    duplicates: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        other_name = row["institution_name"]
        if not institution_names_match(institution_name, other_name, threshold=name_threshold):
            continue

        row_date = parse_incident_date(row["incident_date"])
        if not dates_within_window(incident_dt, row_date, window_days):
            continue

        duplicates.append(
            {
                "incident_id": row["incident_id"],
                "institution_name": other_name,
                "incident_date": row["incident_date"],
                "confidence": len(row["llm_summary"] or ""),
            }
        )

    return duplicates


def _row_score(row: sqlite3.Row) -> tuple[int, int, int, str]:
    return (
        int(row["llm_enriched"] or 0),
        int(row["detail_score"] or 0),
        int(row["summary_length"] or 0),
        int(row["source_count"] or 0),
        row["ingested_at"] or "",
    )


def _merge_notes(*notes: Optional[str]) -> Optional[str]:
    merged: List[str] = []
    for note in notes:
        stripped = (note or "").strip()
        if stripped and stripped not in merged:
            merged.append(stripped)
    return " | ".join(merged) if merged else None


def _merge_duplicate_into_keeper(conn: sqlite3.Connection, keep_id: str, dup_id: str) -> None:
    keep_row = conn.execute(
        """
        SELECT i.*,
               ef.institution_name AS flat_institution_name
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_id = ?
        """,
        (keep_id,),
    ).fetchone()
    dup_row = conn.execute(
        """
        SELECT i.*,
               ef.institution_name AS flat_institution_name
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_id = ?
        """,
        (dup_id,),
    ).fetchone()

    if not keep_row or not dup_row:
        return

    keep_urls = {u.strip() for u in (keep_row["all_urls"] or "").split(";") if u.strip()}
    dup_urls = {u.strip() for u in (dup_row["all_urls"] or "").split(";") if u.strip()}
    merged_urls = ";".join(sorted(keep_urls | dup_urls)) or None

    best_name = choose_best_institution_name(
        keep_row["flat_institution_name"],
        dup_row["flat_institution_name"],
        keep_row["institution_name"],
        dup_row["institution_name"],
        keep_row["victim_raw_name"],
        dup_row["victim_raw_name"],
        keep_row["title"],
        dup_row["title"],
    )

    keep_fields = {
        "all_urls": merged_urls,
        "leak_site_url": keep_row["leak_site_url"] or dup_row["leak_site_url"],
        "screenshot_url": keep_row["screenshot_url"] or dup_row["screenshot_url"],
        "source_detail_url": keep_row["source_detail_url"] or dup_row["source_detail_url"],
        "attack_type_hint": keep_row["attack_type_hint"] or dup_row["attack_type_hint"],
        "country": keep_row["country"] or dup_row["country"],
        "country_code": keep_row["country_code"] or dup_row["country_code"],
        "region": keep_row["region"] or dup_row["region"],
        "city": keep_row["city"] or dup_row["city"],
        "incident_date": keep_row["incident_date"] or dup_row["incident_date"],
        "date_precision": keep_row["date_precision"] or dup_row["date_precision"],
        "institution_name": best_name or keep_row["institution_name"] or dup_row["institution_name"],
        "victim_raw_name": keep_row["victim_raw_name"] or dup_row["victim_raw_name"] or best_name,
        "notes": _merge_notes(keep_row["notes"], dup_row["notes"]),
    }

    conn.execute(
        """
        UPDATE incidents
        SET all_urls = ?,
            leak_site_url = ?,
            screenshot_url = ?,
            source_detail_url = ?,
            attack_type_hint = ?,
            country = ?,
            country_code = ?,
            region = ?,
            city = ?,
            incident_date = ?,
            date_precision = ?,
            institution_name = ?,
            victim_raw_name = ?,
            notes = ?,
            last_updated_at = ?
        WHERE incident_id = ?
        """,
        (
            keep_fields["all_urls"],
            keep_fields["leak_site_url"],
            keep_fields["screenshot_url"],
            keep_fields["source_detail_url"],
            keep_fields["attack_type_hint"],
            keep_fields["country"],
            keep_fields["country_code"],
            keep_fields["region"],
            keep_fields["city"],
            keep_fields["incident_date"],
            keep_fields["date_precision"],
            keep_fields["institution_name"],
            keep_fields["victim_raw_name"],
            keep_fields["notes"],
            datetime.utcnow().isoformat(),
            keep_id,
        ),
    )

    if best_name:
        conn.execute(
            """
            UPDATE incident_enrichments_flat
            SET institution_name = ?, updated_at = ?
            WHERE incident_id = ?
            """,
            (best_name, datetime.utcnow().isoformat(), keep_id),
        )

    conn.execute(
        """
        INSERT OR IGNORE INTO incident_sources
        (incident_id, source, source_event_id, first_seen_at, confidence)
        SELECT ?, source, source_event_id, first_seen_at, confidence
        FROM incident_sources
        WHERE incident_id = ?
        """,
        (keep_id, dup_id),
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO source_events
        (source, source_event_id, incident_id, first_seen_at)
        SELECT source, source_event_id, ?, first_seen_at
        FROM source_events
        WHERE incident_id = ?
        """,
        (keep_id, dup_id),
    )
    has_articles = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'articles'"
    ).fetchone()
    if has_articles:
        conn.execute(
            """
            INSERT OR IGNORE INTO articles
            (incident_id, url, title, content, author, publish_date, fetch_successful,
             fetch_error, content_length, fetched_at, url_score, url_score_reasoning, is_primary)
            SELECT ?, url, title, content, author, publish_date, fetch_successful,
                   fetch_error, content_length, fetched_at, url_score, url_score_reasoning, 0
            FROM articles
            WHERE incident_id = ?
            """,
            (keep_id, dup_id),
        )
    conn.execute(
        """
        INSERT INTO pipeline_checkpoint (incident_id, phase, completed_at)
        SELECT ?, phase, completed_at
        FROM pipeline_checkpoint
        WHERE incident_id = ?
        ON CONFLICT(incident_id) DO UPDATE
        SET phase = excluded.phase, completed_at = excluded.completed_at
        """,
        (keep_id, dup_id),
    )

    tables = [
        "incident_sources",
        "source_events",
        "pipeline_checkpoint",
        "incident_enrichments",
        "incident_enrichments_flat",
    ]
    if has_articles:
        tables.insert(0, "articles")

    for table in tables:
        conn.execute(f"DELETE FROM {table} WHERE incident_id = ?", (dup_id,))
    conn.execute("DELETE FROM incidents WHERE incident_id = ?", (dup_id,))


def deduplicate_by_institution(
    conn: sqlite3.Connection,
    window_days: int = 14,
    name_threshold: int = 85,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Deduplicate incidents by institution name within a date window.

    Only enriched incidents participate. This keeps distinct source records alive
    until Phase 2 has extracted their article-specific CTI, then chooses the best
    enriched survivor and merges source attribution afterward.
    """
    logger.info("Starting deduplication by institution name...")

    cur = conn.execute(
        f"""
        SELECT i.incident_id,
               COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) AS institution_name,
               NULLIF(i.victim_raw_name, '') AS raw_victim_name,
               i.incident_date,
               i.ingested_at,
               COALESCE(i.llm_enriched, 0) AS llm_enriched,
               {_DETAIL_SCORE_SQL} AS detail_score,
               COALESCE(LENGTH(i.llm_summary), 0) AS summary_length,
               (SELECT COUNT(*) FROM incident_sources s WHERE s.incident_id = i.incident_id) AS source_count
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.llm_enriched = 1
          AND (i.llm_excluded IS NULL OR i.llm_excluded = 0)
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) IS NOT NULL
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) != ''
        ORDER BY i.llm_enriched DESC, i.ingested_at DESC
        """
    )
    rows = cur.fetchall()
    total_candidates = len(rows)
    if total_candidates == 0:
        return {
            "success": True,
            "total_enriched": 0,
            "checked": 0,
            "removed": 0,
            "remaining": 0,
            "groups_merged": 0,
        }

    parent: Dict[int, int] = {i: i for i in range(total_candidates)}

    def find(idx: int) -> int:
        while parent[idx] != idx:
            parent[idx] = parent[parent[idx]]
            idx = parent[idx]
        return idx

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if _row_score(rows[ra]) >= _row_score(rows[rb]):
            parent[rb] = ra
        else:
            parent[ra] = rb

    # Pre-compute normalized names and token sets once to avoid redundant regex
    # work inside the O(N²) comparison loops.
    # Each slot stores a primary name (ef.institution_name → institution_name → victim_raw_name)
    # and an optional secondary name from victim_raw_name.
    #
    # Why victim_raw_name and not institution_name?
    # save_enrichment_result() overwrites incidents.institution_name with the LLM result
    # (e.g. "UCF"), but leaves victim_raw_name untouched when it already had a value.
    # So victim_raw_name preserves the original ingestion name ("University of Central
    # Florida") even after the LLM stores an abbreviation.  Using it as a second matching
    # candidate catches the LLM-abbreviation ↔ full-name mismatch.
    norm_cache: List[str] = []
    token_cache: List[set] = []
    norm_victim_cache: List[str] = []
    token_victim_cache: List[set] = []
    for row in rows:
        primary = row["institution_name"] or ""
        victim = row["raw_victim_name"] or ""
        norm_cache.append(normalize_institution_name(primary))
        token_cache.append(_core_tokens(primary))
        # Only add as secondary candidate if it differs from the primary (avoids redundant checks)
        norm_victim_cache.append(normalize_institution_name(victim) if victim != primary else "")
        token_victim_cache.append(_core_tokens(victim) if victim != primary else set())

    def _fast_match(idx_a: int, idx_b: int) -> bool:
        """
        Match on primary institution_name first, then cross-check against
        victim_raw_name on each side.

        victim_raw_name holds the original ingestion name even after the LLM
        overwrites institution_name with an abbreviation.  For example:
          - Incident A: ef.institution_name="UCF", victim_raw_name="University of Central Florida"
          - Incident B: ef.institution_name="University of Central Florida"
        Primary-vs-primary fails ("ucf" ≠ "central florida"), but
        victim_raw_name_A-vs-primary_B succeeds.
        """
        candidates_a = [(norm_cache[idx_a], token_cache[idx_a])]
        candidates_b = [(norm_cache[idx_b], token_cache[idx_b])]
        if norm_victim_cache[idx_a]:
            candidates_a.append((norm_victim_cache[idx_a], token_victim_cache[idx_a]))
        if norm_victim_cache[idx_b]:
            candidates_b.append((norm_victim_cache[idx_b], token_victim_cache[idx_b]))
        return any(
            _names_match_pair(n1, t1, n2, t2, threshold=name_threshold)
            for n1, t1 in candidates_a
            for n2, t2 in candidates_b
        )

    dated: List[tuple[int, datetime]] = []
    undated: List[int] = []
    for idx, row in enumerate(rows):
        parsed = parse_incident_date(row["incident_date"])
        if parsed is None:
            undated.append(idx)
        else:
            dated.append((idx, parsed))

    dated.sort(key=lambda item: item[1])
    window = timedelta(days=window_days)

    for pos, (idx_i, date_i) in enumerate(dated):
        for idx_j, date_j in dated[pos + 1:]:
            if date_j - date_i > window:
                break
            if _fast_match(idx_i, idx_j):
                union(idx_i, idx_j)

    for pos, idx_i in enumerate(undated):
        for idx_j in undated[pos + 1:]:
            if _fast_match(idx_i, idx_j):
                union(idx_i, idx_j)

    groups: Dict[int, List[int]] = {}
    for idx in range(len(rows)):
        groups.setdefault(find(idx), []).append(idx)

    merge_groups = [(root, members) for root, members in groups.items() if len(members) > 1]

    if dry_run:
        preview = []
        for root, members in merge_groups[:50]:
            keep = rows[root]
            dupes = [rows[m] for m in members if m != root]
            preview.append(
                {
                    "keep": keep["incident_id"],
                    "keep_name": keep["institution_name"],
                    "keep_date": keep["incident_date"],
                    "merge_count": len(dupes),
                    "duplicates": [
                        {"id": d["incident_id"], "date": d["incident_date"]}
                        for d in dupes
                    ],
                }
            )
        return {
            "success": True,
            "dry_run": True,
            "groups_found": len(merge_groups),
            "incidents_to_remove": sum(len(members) - 1 for _, members in merge_groups),
            "preview": preview,
        }

    removed_count = 0
    for root, members in merge_groups:
        keep_id = rows[root]["incident_id"]
        for member in members:
            if member == root:
                continue
            dup_id = rows[member]["incident_id"]
            logger.info(f"Dedup: merging {dup_id} -> {keep_id}")
            _merge_duplicate_into_keeper(conn, keep_id, dup_id)
            removed_count += 1

    conn.commit()

    stats = {
        "success": True,
        "total_enriched": total_candidates,
        "checked": len(merge_groups),
        "removed": removed_count,
        "remaining": total_candidates - removed_count,
        "groups_merged": len(merge_groups),
        "window_days": window_days,
        "name_threshold": name_threshold,
    }

    logger.info(
        "Deduplication complete: "
        f"{total_candidates} total, {stats['checked']} groups, "
        f"{stats['removed']} removed, {stats['remaining']} remaining"
    )
    return stats


def dedup_incident_after_save(
    conn: sqlite3.Connection,
    incident_id: str,
    window_days: int = 14,
    name_threshold: int = 85,
) -> Optional[str]:
    """
    Check whether a just-enriched incident duplicates an existing enriched incident
    and merge them if so.  Called inline after each save_enrichment_result() so
    duplicates are resolved immediately rather than waiting for an end-of-batch pass.

    Returns the surviving incident_id if a merge happened (may differ from incident_id
    if the new incident was the weaker one), or None if no duplicate found.
    """
    row = conn.execute(
        f"""
        SELECT i.incident_id,
               COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) AS institution_name,
               NULLIF(i.victim_raw_name, '') AS raw_victim_name,
               i.incident_date,
               COALESCE(i.llm_enriched, 0) AS llm_enriched,
               {_DETAIL_SCORE_SQL} AS detail_score,
               COALESCE(LENGTH(i.llm_summary), 0) AS summary_length,
               (SELECT COUNT(*) FROM incident_sources s WHERE s.incident_id = i.incident_id) AS source_count,
               i.ingested_at
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_id = ?
          AND i.llm_enriched = 1
        """,
        (incident_id,),
    ).fetchone()

    if not row or not row["institution_name"]:
        return None

    primary_norm = normalize_institution_name(row["institution_name"])
    primary_tokens = _core_tokens(row["institution_name"])
    victim_norm = normalize_institution_name(row["raw_victim_name"] or "") if row["raw_victim_name"] != row["institution_name"] else ""
    victim_tokens = _core_tokens(row["raw_victim_name"] or "") if victim_norm else set()

    candidates_new = [(primary_norm, primary_tokens)]
    if victim_norm:
        candidates_new.append((victim_norm, victim_tokens))

    incident_dt = parse_incident_date(row["incident_date"])
    window = timedelta(days=window_days)

    # Scan all other enriched incidents for a match
    existing = conn.execute(
        f"""
        SELECT i.incident_id,
               COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) AS institution_name,
               NULLIF(i.victim_raw_name, '') AS raw_victim_name,
               i.incident_date,
               COALESCE(i.llm_enriched, 0) AS llm_enriched,
               {_DETAIL_SCORE_SQL} AS detail_score,
               COALESCE(LENGTH(i.llm_summary), 0) AS summary_length,
               (SELECT COUNT(*) FROM incident_sources s WHERE s.incident_id = i.incident_id) AS source_count,
               i.ingested_at
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_id != ?
          AND i.llm_enriched = 1
          AND (i.llm_excluded IS NULL OR i.llm_excluded = 0)
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.institution_name, ''), NULLIF(i.victim_raw_name, '')) IS NOT NULL
        """,
        (incident_id,),
    ).fetchall()

    for other in existing:
        other_name = other["institution_name"] or ""
        if not other_name:
            continue

        # Date window check
        other_dt = parse_incident_date(other["incident_date"])
        if incident_dt and other_dt and abs((incident_dt - other_dt).days) > window_days:
            continue

        # Name match: check cross-product of primary + victim_raw candidates
        other_norm = normalize_institution_name(other_name)
        other_tokens = _core_tokens(other_name)
        other_victim_norm = normalize_institution_name(other["raw_victim_name"] or "") if other["raw_victim_name"] != other["institution_name"] else ""
        other_victim_tokens = _core_tokens(other["raw_victim_name"] or "") if other_victim_norm else set()

        candidates_other = [(other_norm, other_tokens)]
        if other_victim_norm:
            candidates_other.append((other_victim_norm, other_victim_tokens))

        matched = any(
            _names_match_pair(n1, t1, n2, t2, threshold=name_threshold)
            for n1, t1 in candidates_new
            for n2, t2 in candidates_other
        )
        if not matched:
            continue

        # Found a duplicate — decide which to keep (higher score wins)
        new_score = _row_score(row)
        other_score = _row_score(other)

        if new_score >= other_score:
            keep_id, dup_id = incident_id, other["incident_id"]
        else:
            keep_id, dup_id = other["incident_id"], incident_id

        logger.info(f"Inline dedup: merging {dup_id} → {keep_id} (same institution, {window_days}-day window)")
        _merge_duplicate_into_keeper(conn, keep_id, dup_id)
        conn.commit()
        return keep_id

    return None
