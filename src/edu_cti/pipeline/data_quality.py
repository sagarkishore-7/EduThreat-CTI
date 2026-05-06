"""
Data quality sweeper.

Scans the incidents table for fields the LLM commonly gets wrong:
- Unparseable / future / pre-1990 dates (incident_date, source_published_date,
  discovery_date, timeline event dates)
- Institution names that are actually article headlines

For each flagged incident:
- < MAX_REENRICH_ATTEMPTS: strip enrichment, set llm_enriched=0 with a reason
  hint so the next enrichment cycle picks it up; the enricher prompt reads
  re_enrich_reason and tells the LLM what to do differently this time
  (necessary because temperature=0 → identical input → identical output).
- >= MAX_REENRICH_ATTEMPTS: set manual_review_required=1 so an admin can
  edit the row directly via the dashboard.

Manually-edited fields (manually_edited_fields JSON list) are never touched
by re-enrichment — see save_enrichment_result.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# Acceptable date window — covers all realistic education incidents.
MIN_DATE = date(1990, 1, 1)
# Allow 3 days into the future to absorb timezone edge cases (UTC vs local).
FUTURE_TOLERANCE_DAYS = 3

# Cap before manual review takes over. Each retry uses a different prompt
# hint, so the LLM has a meaningful chance of converging — but we don't
# loop forever.
MAX_REENRICH_ATTEMPTS = 3

# Strict YYYY-MM-DD with optional time suffix.
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T ]|$)")


def _today_plus_buffer() -> date:
    return date.today() + timedelta(days=FUTURE_TOLERANCE_DAYS)


def is_safe_date(value: Optional[str]) -> bool:
    """
    True only when value parses as a valid YYYY-MM-DD date in
    [MIN_DATE, today + FUTURE_TOLERANCE_DAYS]. Anything else (None, garbage,
    negative years, future dates beyond the buffer) returns False.
    """
    if not value:
        # None / empty is "no date provided", not "bad date" — treat as safe.
        return True
    s = str(value).strip()
    if not s:
        return True
    if not _ISO_DATE_RE.match(s):
        return False
    try:
        d = date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return False
    if d < MIN_DATE:
        return False
    if d > _today_plus_buffer():
        return False
    return True


def _is_headline_format(name: Optional[str], title: Optional[str]) -> bool:
    """Wrapper that imports the existing post_processing heuristic lazily."""
    if not name:
        return False
    try:
        from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
        return bool(is_headline_format(name, title))
    except Exception:
        return False


def _timeline_dates(timeline_json: Optional[str]) -> List[str]:
    """Extract every event.date from a JSON-encoded timeline list."""
    if not timeline_json:
        return []
    try:
        events = json.loads(timeline_json)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(events, list):
        return []
    out: List[str] = []
    for e in events:
        if isinstance(e, dict):
            d = e.get("date")
            if d:
                out.append(str(d))
    return out


def _diagnose_incident(row: sqlite3.Row) -> Optional[str]:
    """
    Inspect one joined row from incidents + incident_enrichments_flat.
    Return a short reason string if the row needs re-enrichment, else None.
    """
    reasons: List[str] = []

    # Date validity
    if not is_safe_date(row["incident_date"]):
        reasons.append(f"incident_date={row['incident_date']!r}")
    if not is_safe_date(row["source_published_date"]):
        reasons.append(f"source_published_date={row['source_published_date']!r}")
    if not is_safe_date(row["discovery_date"]):
        reasons.append(f"discovery_date={row['discovery_date']!r}")
    bad_timeline = [d for d in _timeline_dates(row["timeline_json"]) if not is_safe_date(d)]
    if bad_timeline:
        reasons.append(f"timeline_dates={bad_timeline[:3]!r}")

    # Headline-as-institution
    inst_name = row["institution_name"]
    title = row["title"]
    if _is_headline_format(inst_name, title):
        reasons.append(f"institution_name_looks_like_headline={inst_name!r}")

    return "; ".join(reasons) if reasons else None


def _manually_edited(row: sqlite3.Row, field: str) -> bool:
    raw = row["manually_edited_fields"] if "manually_edited_fields" in row.keys() else None
    if not raw:
        return False
    try:
        fields = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return False
    return isinstance(fields, list) and field in fields


def sweep_invalid_data(conn: sqlite3.Connection) -> dict:
    """
    Find every enriched incident with bad date(s) or headline-as-institution
    and either queue it for re-enrichment or flag it for manual review.

    Idempotent — safe to run on a schedule. Skips rows that have already
    been manually edited (those fields are locked).
    """
    cur = conn.execute(
        """
        SELECT
            i.incident_id,
            i.institution_name,
            i.title,
            i.incident_date,
            i.source_published_date,
            i.discovery_date,
            i.re_enrich_attempts,
            i.manual_review_required,
            i.manually_edited_fields,
            ef.timeline_json
        FROM incidents i
        LEFT JOIN incident_enrichments_flat ef ON ef.incident_id = i.incident_id
        WHERE i.llm_enriched = 1
          AND COALESCE(i.llm_excluded, 0) = 0
          AND COALESCE(i.manual_review_required, 0) = 0
        """
    )
    rows = cur.fetchall()

    requeued = 0
    flagged_for_review = 0
    skipped_manual = 0

    today_str = datetime.utcnow().isoformat()

    for row in rows:
        reason = _diagnose_incident(row)
        if not reason:
            continue

        # Don't re-enrich a row whose every flagged field is locked behind a
        # manual edit — admin already made the call.
        if _manually_edited(row, "institution_name") and "headline" in reason and "date" not in reason:
            skipped_manual += 1
            continue

        attempts = (row["re_enrich_attempts"] or 0) + 1

        if attempts >= MAX_REENRICH_ATTEMPTS:
            conn.execute(
                """
                UPDATE incidents
                SET manual_review_required = 1,
                    manual_review_reason = ?,
                    re_enrich_attempts = ?,
                    re_enrich_reason = ?
                WHERE incident_id = ?
                """,
                (reason, attempts, reason, row["incident_id"]),
            )
            flagged_for_review += 1
            logger.info(
                "Data quality: %s flagged for manual review after %d attempts | %s",
                row["incident_id"], attempts, reason,
            )
        else:
            # Re-queue for enrichment. Clear llm_enriched so the worker picks it up;
            # store the reason so the prompt builder can include a hint.
            conn.execute(
                """
                UPDATE incidents
                SET llm_enriched = 0,
                    llm_enriched_at = NULL,
                    re_enrich_attempts = ?,
                    re_enrich_reason = ?
                WHERE incident_id = ?
                """,
                (attempts, reason, row["incident_id"]),
            )
            requeued += 1
            logger.info(
                "Data quality: re-queueing %s (attempt %d/%d) | %s",
                row["incident_id"], attempts, MAX_REENRICH_ATTEMPTS, reason,
            )

    conn.commit()

    return {
        "scanned": len(rows),
        "requeued_for_reenrichment": requeued,
        "flagged_for_manual_review": flagged_for_review,
        "skipped_manually_edited": skipped_manual,
        "checked_at": today_str,
    }


def get_re_enrich_hint(conn: sqlite3.Connection, incident_id: str) -> Optional[str]:
    """
    Return a prompt hint for an incident that has been queued for re-enrichment.

    Called by the enricher when building the LLM user message; injecting an
    explicit "previous attempt produced X which is wrong because Y" line is
    the only way to break temperature=0 determinism — the model needs new
    information in the prompt to produce a different output.
    """
    try:
        cur = conn.execute(
            "SELECT re_enrich_attempts, re_enrich_reason FROM incidents WHERE incident_id = ?",
            (incident_id,),
        )
        row = cur.fetchone()
    except sqlite3.OperationalError:
        return None
    if not row or not row["re_enrich_attempts"]:
        return None
    reason = row["re_enrich_reason"] or "previous extraction had errors"
    return (
        "=== RE-ENRICHMENT HINT ===\n"
        f"This is re-enrichment attempt {row['re_enrich_attempts']}. The previous extraction had problems:\n"
        f"  {reason}\n"
        "Pay extra attention to the affected fields. Read the article body carefully:\n"
        "- Institution name should be the actual school / college / university named in the article body, "
        "NOT the news headline.\n"
        "- Dates must be in YYYY-MM-DD format and reflect when the incident actually occurred — never use\n"
        "  today's date as a placeholder; never use a date from before 1990 or in the future.\n"
        "============================="
    )
