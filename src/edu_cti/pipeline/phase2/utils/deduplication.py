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

_HEADLINE_AFTER_RE = re.compile(
    r"\b(?:targets?|targeted|hits?|attacks?|attacked|breaches?|breached|"
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
    re.compile(
        r"[A-Z0-9][A-Za-z0-9&'.,-]*(?:\s+[A-Z0-9][A-Za-z0-9&'.,-]*)*\s+"
        r"(?:Independent\s+School\s+District|School\s+District|Community\s+College|"
        r"University(?:\s+System)?|College|School|Academy|Seminary|ISD|"
        r"Board\s+of\s+Education|Education\s+Department|Polytechnic|"
        r"Institute(?:\s+of(?:\s+[A-Z0-9][A-Za-z0-9&'.,-]*)+)?)",
        re.IGNORECASE,
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

    after_match = _HEADLINE_AFTER_RE.search(text)
    if after_match:
        candidates.append(after_match.group("victim").strip(" -,:;"))

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
        return (score, len(candidate))

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
               COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.university_name, ''), NULLIF(i.victim_raw_name, '')) AS institution_name,
               i.incident_date,
               i.llm_summary
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.incident_id != ?
          AND i.llm_enriched = 1
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.university_name, ''), NULLIF(i.victim_raw_name, '')) IS NOT NULL
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
                "university_name": other_name,
                "incident_date": row["incident_date"],
                "confidence": len(row["llm_summary"] or ""),
            }
        )

    return duplicates


def _row_score(row: sqlite3.Row) -> tuple[int, int, str]:
    return (
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
        keep_row["university_name"],
        dup_row["university_name"],
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
        "university_name": best_name or keep_row["university_name"] or dup_row["university_name"],
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
            university_name = ?,
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
            keep_fields["university_name"],
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
    Deduplicate enriched incidents by institution name within a date window.

    This is the runtime equivalent of the admin 14-day dedup logic and is used
    after enrichment rounds so duplicates are merged while the pipeline runs.
    """
    logger.info("Starting post-enrichment deduplication by institution name...")

    cur = conn.execute(
        """
        SELECT i.incident_id,
               COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.university_name, ''), NULLIF(i.victim_raw_name, '')) AS institution_name,
               i.incident_date,
               i.ingested_at,
               COALESCE(LENGTH(i.llm_summary), 0) AS summary_length,
               (SELECT COUNT(*) FROM incident_sources s WHERE s.incident_id = i.incident_id) AS source_count
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON i.incident_id = ef.incident_id
        WHERE i.llm_enriched = 1
          AND (i.llm_excluded IS NULL OR i.llm_excluded = 0)
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.university_name, ''), NULLIF(i.victim_raw_name, '')) IS NOT NULL
          AND COALESCE(NULLIF(ef.institution_name, ''), NULLIF(i.university_name, ''), NULLIF(i.victim_raw_name, '')) != ''
        ORDER BY i.ingested_at DESC
        """
    )
    rows = cur.fetchall()
    total_enriched = len(rows)
    if total_enriched == 0:
        return {
            "success": True,
            "total_enriched": 0,
            "checked": 0,
            "removed": 0,
            "remaining": 0,
            "groups_merged": 0,
        }

    parent: Dict[int, int] = {i: i for i in range(len(rows))}

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
        name_i = rows[idx_i]["institution_name"]
        for idx_j, date_j in dated[pos + 1:]:
            if date_j - date_i > window:
                break
            if institution_names_match(name_i, rows[idx_j]["institution_name"], threshold=name_threshold):
                union(idx_i, idx_j)

    for pos, idx_i in enumerate(undated):
        name_i = rows[idx_i]["institution_name"]
        for idx_j in undated[pos + 1:]:
            if institution_names_match(name_i, rows[idx_j]["institution_name"], threshold=name_threshold):
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
        "total_enriched": total_enriched,
        "checked": len(merge_groups),
        "removed": removed_count,
        "remaining": total_enriched - removed_count,
        "groups_merged": len(merge_groups),
        "window_days": window_days,
        "name_threshold": name_threshold,
    }

    logger.info(
        "Post-enrichment deduplication complete: "
        f"{stats['total_enriched']} total, {stats['checked']} groups, "
        f"{stats['removed']} removed, {stats['remaining']} remaining"
    )
    return stats
