"""
Phase 2: Enrichment Pipeline CLI

Main entry point for Phase 2 LLM enrichment pipeline.
Coordinates article fetching, education relevance checking, URL scoring,
and comprehensive CTI extraction.
"""

import argparse
import gc
import logging
import os
import random
import sys
import time
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage.article_storage import ArticleProcessor, init_articles_table
from src.edu_cti.pipeline.phase2.storage.db import (
    get_unenriched_incidents,
    save_enrichment_result,
    mark_incident_skipped,
    get_enrichment_stats,
    checkpoint_mark,
    checkpoint_get_fetched,
    checkpoint_clear,
    init_incident_enrichments_table,
)
from src.edu_cti.pipeline.phase2.utils.deduplication import deduplicate_by_institution, dedup_incident_after_save
from src.edu_cti.pipeline.phase2.csv_export import export_enriched_dataset
from src.edu_cti.core.config import (
    DB_PATH,
    OLLAMA_API_KEY,
    OLLAMA_HOST,
    OLLAMA_MODEL,
    ENRICHMENT_BATCH_SIZE,
    ENRICHMENT_RATE_LIMIT_DELAY,
    ENRICHMENT_SKIP_SOURCES,
    FETCH_IMPOSSIBLE_SOURCES,
    SERP_MAX_ATTEMPTS,
    PHASE2_MEMORY_MONITOR_ENABLED,
    PHASE2_MEMORY_CHECK_INTERVAL,
    PHASE2_MEMORY_GC_INTERVAL,
    PHASE2_MEMORY_SOFT_LIMIT_MB,
    PHASE2_MEMORY_HARD_LIMIT_MB,
    PHASE2_MEMORY_SOFT_LIMIT_PCT,
    PHASE2_MEMORY_HARD_LIMIT_PCT,
)
from src.edu_cti.core.logging_utils import configure_logging
from src.edu_cti.core.db import get_connection, init_db
from src.edu_cti.core import metrics as _metrics
from pathlib import Path

# Module-level logger — used by module-level helpers (_record_serp_failure, etc.)
logger = logging.getLogger(__name__)

# Module-level cancel event — set by pipeline manager to request graceful stop
_cancel_event = threading.Event()

# Per-incident in-progress guard — prevents two workers from enriching the same
# incident simultaneously when the queue is fed from multiple sources.
_in_progress: set = set()
_in_progress_lock = threading.Lock()

# Module-level flag — torch.set_num_interop_threads() can only be called once
# per process. Track that we've configured PyTorch threading so the auto-loop's
# second round doesn't try to re-set and emit a misleading warning.
_PYTORCH_THREADS_SET: bool = False

_memory_policy_lock = threading.Lock()
_memory_policy: Optional[Dict[str, Any]] = None
_memory_guard_state_lock = threading.Lock()
_memory_guard_state: Dict[str, Any] = {
    "pause_requested": False,
    "reason": None,
    "rss_mb": None,
    "soft_limit_logged": False,
    "hard_limit_mb": None,
    "container_limit_mb": None,
}


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _running_on_railway() -> bool:
    return bool(os.environ.get("RAILWAY_SERVICE_ID") or os.environ.get("RAILWAY_ENVIRONMENT"))


def _explicit_memory_limits_configured() -> bool:
    return PHASE2_MEMORY_SOFT_LIMIT_MB > 0 or PHASE2_MEMORY_HARD_LIMIT_MB > 0


def _apply_runtime_safety_overrides(args, logger: logging.Logger) -> Dict[str, Any]:
    """
    Resolve runtime overrides for Phase 2.

    Workers come from a single source: args.workers (which is wired to the
    ENRICHMENT_WORKERS env var via config.py). No more auto-capping based
    on detected RAM, no more Railway-specific clamping — the operator
    sets ENRICHMENT_WORKERS to whatever they want and we honour it.

    Pre-warm defaults to True. Loading the ML models once in the main
    thread before workers start is strictly safer than letting N workers
    race to load them concurrently. The PHASE2_PREWARM_ML_MODELS=false
    escape hatch is kept for diagnostic edge cases.
    """
    overrides = {
        "ml_disabled_for_run": _env_flag("DISABLE_ML_FEATURES", "0"),
        "prewarm_ml_models": _env_flag("PHASE2_PREWARM_ML_MODELS", "1"),
        "effective_workers": max(1, int(getattr(args, "workers", 1))),
    }
    if overrides["ml_disabled_for_run"]:
        overrides["prewarm_ml_models"] = False
    logger.info(
        "Phase 2 config: workers=%d, ML pre-warm=%s, ML disabled=%s",
        overrides["effective_workers"],
        overrides["prewarm_ml_models"],
        overrides["ml_disabled_for_run"],
    )
    return overrides

# --- Critical fields used to compute per-incident completeness score (0-10) ---
_COMPLETENESS_FIELDS = [
    "attack_category", "institution_name", "institution_type",
    "incident_date", "country", "ransomware_family",
    "threat_actor_name", "attack_vector", "records_affected_exact",
]

# All fields tracked for per-field fill-rate metrics
_TRACKED_FIELDS = [
    "institution_name", "institution_type", "country", "region",
    "incident_date", "attack_category", "attack_vector", "ransomware_family",
    "threat_actor_name", "threat_actor_category", "threat_actor_motivation",
    "threat_actor_origin_country", "records_affected_exact", "users_affected_exact",
    "was_ransom_demanded", "ransom_amount", "ransom_paid",
    "data_breached", "data_exfiltrated", "dwell_time_days", "recovery_duration_days",
    "mitre_attack_techniques", "timeline", "cve_ids",
    "enriched_summary", "malware_families", "attacker_tools",
]


def _emit_enrichment_metrics(incident_id: str, enrichment_result, raw_json_data: Optional[dict], conn) -> None:
    """Emit field-completeness and source novelty metrics after a successful enrichment save."""
    try:
        raw = raw_json_data or {}

        def _has(field: str) -> bool:
            v = raw.get(field)
            if v is None:
                return False
            if isinstance(v, (list, dict)):
                return len(v) > 0
            if isinstance(v, str):
                return bool(v.strip())
            return True

        # Per-field fill rate counters
        for field in _TRACKED_FIELDS:
            if _has(field):
                _metrics.increment("field_populated_total", labels={"field": field})
            else:
                _metrics.increment("field_null_total", labels={"field": field})

        # Completeness score (0–10 key fields)
        score = sum(1 for f in _COMPLETENESS_FIELDS if _has(f))
        _metrics.observe("incident_completeness_score", float(score))

        # Source novelty: check whether this incident_id's source was already present
        # A source is "novel" if this is the only source reporting this incident.
        try:
            cur = conn.execute(
                "SELECT COUNT(DISTINCT source) FROM incident_sources WHERE incident_id = ?",
                (incident_id,)
            )
            row = cur.fetchone()
            source_count = row[0] if row else 1
            src_prefix = incident_id.split("_")[0]
            if source_count == 1:
                _metrics.increment("source_novel_incident_total", labels={"source": src_prefix})
            else:
                _metrics.increment("source_duplicate_total", labels={"source": src_prefix})
        except Exception:
            pass

        # Pipeline throughput gauge (incidents enriched per hour this session)
        _metrics.set_gauge("pipeline_queue_depth", 0)  # placeholder — updated by fetch phase

        # Estimated LLM cost per incident: DeepSeek ~$0.27/M input tokens, ~50K tokens/incident
        _DEEPSEEK_COST_PER_TOKEN = 0.27 / 1_000_000
        _AVG_TOKENS_PER_INCIDENT = 50_000
        _estimated_cost = _DEEPSEEK_COST_PER_TOKEN * _AVG_TOKENS_PER_INCIDENT
        _metrics.observe("enrichment_cost_per_incident_usd", _estimated_cost)

    except Exception as _e:
        logger.debug(f"Non-fatal: failed to emit enrichment metrics for {incident_id}: {_e}")


class EnrichmentWatchdog:
    """
    Detects enrichment pipeline stalls and triggers a clean process exit.

    A stall is defined as no pipeline activity (fetch iteration or queue item
    processed) for more than ``stall_seconds`` seconds.  On stall detection the
    watchdog calls ``os._exit(1)`` so Railway/Docker restarts the container.

    Usage:
        watchdog = EnrichmentWatchdog(stall_seconds=300)
        watchdog.start()
        ...
        watchdog.heartbeat()   # call after each queue item is processed
        ...
        watchdog.stop()
    """

    def __init__(self, stall_seconds: int = 300):
        self._stall_seconds = stall_seconds
        self._last_beat = time.monotonic()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._last_beat = time.monotonic()
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._watch, daemon=True, name="enrichment-watchdog"
        )
        self._thread.start()
        logger.info(f"[WATCHDOG] Started — stall threshold {self._stall_seconds}s")

    def stop(self) -> None:
        self._stop_event.set()

    def heartbeat(self) -> None:
        """Call after every queue item processed (any outcome) to reset the stall timer."""
        self._last_beat = time.monotonic()

    def is_stalled(self) -> bool:
        return (time.monotonic() - self._last_beat) > self._stall_seconds

    def _watch(self) -> None:
        while not self._stop_event.wait(timeout=30):
            elapsed = time.monotonic() - self._last_beat
            if elapsed > self._stall_seconds:
                logger.error(
                    f"[WATCHDOG] Enrichment stalled for {elapsed:.0f}s "
                    f"(threshold {self._stall_seconds}s) — triggering restart"
                )
                os._exit(1)


# Module-level watchdog instance — shared by all consumer threads
_watchdog: Optional["EnrichmentWatchdog"] = None


def _get_watchdog() -> Optional["EnrichmentWatchdog"]:
    return _watchdog


def _clear_watchdog() -> None:
    """Stop and clear any lingering watchdog from the current process."""
    global _watchdog
    if _watchdog is None:
        return
    try:
        _watchdog.stop()
    except Exception:
        pass
    _watchdog = None


def _reset_memory_policy_cache() -> None:
    global _memory_policy
    with _memory_policy_lock:
        _memory_policy = None


def _reset_memory_guard_state() -> None:
    with _memory_guard_state_lock:
        _memory_guard_state.update(
            {
                "pause_requested": False,
                "reason": None,
                "rss_mb": None,
                "soft_limit_logged": False,
                "hard_limit_mb": None,
                "container_limit_mb": None,
            }
        )


def _reset_phase2_run_state() -> None:
    """Clear per-run cancellation/progress state before starting a new Phase 2 run."""
    _cancel_event.clear()
    with _in_progress_lock:
        _in_progress.clear()
    _progress.update({"step": "", "detail": "", "percent": 0})
    _reset_memory_guard_state()


def _get_memory_guard_state() -> Dict[str, Any]:
    with _memory_guard_state_lock:
        return dict(_memory_guard_state)


def _detect_container_memory_limit_bytes() -> Optional[int]:
    """Read the container memory limit from cgroups when available."""
    candidates = (
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    )
    for path in candidates:
        try:
            raw = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if not raw or raw.lower() == "max":
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        # Ignore absurd sentinel values that effectively mean "unlimited".
        if value <= 0 or value >= (1 << 60):
            continue
        return value
    return None


def _resolve_memory_policy() -> Optional[Dict[str, Any]]:
    """Resolve the effective memory thresholds for this Phase 2 run."""
    if not PHASE2_MEMORY_MONITOR_ENABLED:
        return None

    soft_mb = PHASE2_MEMORY_SOFT_LIMIT_MB if PHASE2_MEMORY_SOFT_LIMIT_MB > 0 else None
    hard_mb = PHASE2_MEMORY_HARD_LIMIT_MB if PHASE2_MEMORY_HARD_LIMIT_MB > 0 else None
    limit_bytes = _detect_container_memory_limit_bytes()
    limit_mb = (
        int(limit_bytes / (1024 * 1024))
        if limit_bytes and limit_bytes > 0
        else None
    )

    source = "cgroup"
    if (
        _running_on_railway()
        and not _explicit_memory_limits_configured()
        and (
            limit_mb is None
            or limit_mb >= int(os.environ.get("PHASE2_RAILWAY_CGROUP_SUSPECT_MB", "16384"))
        )
    ):
        # Railway frequently reports host memory in cgroups rather than the
        # service cap. Use conservative defaults so the guard trips before the
        # platform OOM-kills the service.
        soft_mb = int(os.environ.get("PHASE2_RAILWAY_SOFT_LIMIT_MB", "384"))
        hard_mb = int(os.environ.get("PHASE2_RAILWAY_HARD_LIMIT_MB", "512"))
        source = "railway_fallback"
    elif limit_mb is not None:
        if soft_mb is None:
            soft_mb = max(1, int(limit_mb * PHASE2_MEMORY_SOFT_LIMIT_PCT))
        if hard_mb is None:
            hard_mb = max(soft_mb + 1 if soft_mb is not None else 1, int(limit_mb * PHASE2_MEMORY_HARD_LIMIT_PCT))
        source = "cgroup"
    elif hard_mb is not None and soft_mb is None:
        soft_mb = max(1, int(hard_mb * 0.85))
        source = "explicit_hard_only"

    if hard_mb is None:
        return None

    if soft_mb is None:
        soft_mb = max(1, hard_mb - 128)
    if soft_mb >= hard_mb:
        soft_mb = max(1, hard_mb - 1)

    check_interval = max(1, PHASE2_MEMORY_CHECK_INTERVAL)
    gc_interval = max(1, PHASE2_MEMORY_GC_INTERVAL)
    if source == "railway_fallback":
        check_interval = 1
        gc_interval = min(gc_interval, 10)

    return {
        "soft_limit_mb": soft_mb,
        "hard_limit_mb": hard_mb,
        "container_limit_mb": limit_mb,
        "check_interval": check_interval,
        "gc_interval": gc_interval,
        "source": source,
    }


def _get_memory_policy() -> Optional[Dict[str, Any]]:
    global _memory_policy
    with _memory_policy_lock:
        if _memory_policy is None:
            _memory_policy = _resolve_memory_policy()
        return dict(_memory_policy) if _memory_policy else None


def _request_memory_pause(*, rss_mb: float, hard_limit_mb: int, container_limit_mb: Optional[int]) -> bool:
    """Signal a graceful pause when memory pressure exceeds the hard threshold."""
    with _memory_guard_state_lock:
        if _memory_guard_state["pause_requested"]:
            return False
        reason = (
            f"RSS {rss_mb:.0f} MB exceeded hard limit {hard_limit_mb} MB"
            + (
                f" (container limit {container_limit_mb} MB)"
                if container_limit_mb
                else ""
            )
        )
        _memory_guard_state.update(
            {
                "pause_requested": True,
                "reason": reason,
                "rss_mb": rss_mb,
                "hard_limit_mb": hard_limit_mb,
                "container_limit_mb": container_limit_mb,
            }
        )

    _progress["step"] = "Pausing for memory pressure"
    _progress["detail"] = reason
    _cancel_event.set()
    if _watchdog:
        _watchdog.heartbeat()
    _metrics.increment("pipeline_memory_pause_total")
    logger.warning("[MEM] %s — requesting graceful Phase 2 pause", reason)
    return True


def _check_memory_pressure(items_processed: int) -> bool:
    """Return True when the Phase 2 run should pause for memory pressure."""
    policy = _get_memory_policy()
    if not policy:
        return False

    gc_interval = policy["gc_interval"]
    if items_processed > 0 and items_processed % gc_interval == 0:
        gc.collect()
        # Ask glibc to release free pages back to the OS.
        # PyTorch CPU allocator and large string allocations (e.g. 670 KB Oxylabs
        # HTML blobs) stay committed in the heap after Python GC; malloc_trim(0)
        # forces glibc to return them, preventing monotonic RSS growth over long runs.
        try:
            import ctypes
            ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
        except Exception:
            pass
        # Audit HF cache and prune if it has bloated past the safe threshold.
        # Observed: HF cache grew from 0 to 1.3 GB in 30 min during enrichment —
        # likely tokenizer / model artifacts being re-saved per call. Pruning
        # mid-run releases the disk pages and lets the kernel reclaim buffer
        # cache; the model weights themselves are kept in RAM, so the next
        # NER call doesn't trigger a re-download (only the cache directory
        # gets recreated empty). If a re-download is triggered it's bounded
        # to ~250 MB once, far better than letting the dir grow to GB+.
        try:
            from src.edu_cti.core.config import DATA_DIR
            import shutil
            from pathlib import Path as _P
            hf_cache = _P(DATA_DIR) / "hf_cache"
            if hf_cache.exists():
                sz = sum(f.stat().st_size for f in hf_cache.rglob("*") if f.is_file())
                # Threshold = 600 MB. Models legitimately occupy ~250 MB; any
                # excess is accumulated artifacts.
                if sz > 600 * 1024 * 1024:
                    logger.warning(
                        "[MEM] HF cache bloat detected: %.1f MB — pruning mid-run",
                        sz / (1024 * 1024),
                    )
                    shutil.rmtree(hf_cache, ignore_errors=True)
                    hf_cache.mkdir(parents=True, exist_ok=True)
        except Exception as _e:
            logger.debug("HF cache prune skipped: %s", _e)
        logger.debug(f"[MEM] gc.collect() + malloc_trim after {items_processed} items")

    if items_processed <= 0 or items_processed % policy["check_interval"] != 0:
        return _get_memory_guard_state()["pause_requested"]

    try:
        import psutil
    except ImportError:
        return False

    rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
    _metrics.observe("pipeline_rss_mb", float(rss_mb))

    soft_mb = policy["soft_limit_mb"]
    hard_mb = policy["hard_limit_mb"]
    container_limit_mb = policy.get("container_limit_mb")

    if rss_mb >= hard_mb:
        _request_memory_pause(
            rss_mb=rss_mb,
            hard_limit_mb=hard_mb,
            container_limit_mb=container_limit_mb,
        )
        return True

    state = _get_memory_guard_state()
    if rss_mb >= soft_mb:
        if not state["soft_limit_logged"]:
            with _memory_guard_state_lock:
                _memory_guard_state["soft_limit_logged"] = True
            _metrics.increment("pipeline_memory_soft_limit_total")
            logger.warning(
                "[MEM] RSS %.0f MB reached soft limit %s MB%s — forcing GC and continuing",
                rss_mb,
                soft_mb,
                f" (container limit {container_limit_mb} MB)" if container_limit_mb else "",
            )
        gc.collect()
    elif state["soft_limit_logged"] and rss_mb < (soft_mb * 0.90):
        with _memory_guard_state_lock:
            _memory_guard_state["soft_limit_logged"] = False

    return state["pause_requested"]


def _record_serp_failure(conn, incident_id: str) -> bool:
    """
    Increment serp_attempt_count for an incident after a failed SERP search.

    Returns True if the incident has now hit SERP_MAX_ATTEMPTS and was soft-deleted,
    False if it was only incremented (caller should skip it for this run).
    """
    conn.execute(
        "UPDATE incidents SET serp_attempt_count = COALESCE(serp_attempt_count, 0) + 1 WHERE incident_id = ?",
        (incident_id,),
    )
    conn.commit()
    row = conn.execute(
        "SELECT serp_attempt_count FROM incidents WHERE incident_id = ?", (incident_id,)
    ).fetchone()
    count = row[0] if row else 0
    if count >= SERP_MAX_ATTEMPTS:
        # Soft-delete: mark as excluded so we stop trying, but keep the row for audit.
        # (The public API never shows llm_excluded=1 rows since they have no enrichment data.)
        conn.execute(
            """
            UPDATE incidents
            SET llm_excluded = 1,
                llm_excluded_reason = 'serp_exhausted',
                llm_enriched = 1,
                llm_enriched_at = datetime('now')
            WHERE incident_id = ?
            """,
            (incident_id,),
        )
        conn.commit()
        logger.info(
            f"Soft-excluded {incident_id} after {count} failed SERP attempts "
            f"(no articles found, incident is unenrichable)"
        )
        return True
    logger.info(
        f"SERP attempt {count}/{SERP_MAX_ATTEMPTS} for {incident_id} — will retry next run"
    )
    return False


def _mark_curated_incident_stub(conn, incident_id: str) -> None:
    """Clear stale articles for curated incidents using the standard SQLite retry helper."""
    from src.edu_cti.core.db import run_with_sqlite_lock_retry

    def _apply() -> None:
        conn.execute("DELETE FROM articles WHERE incident_id = ?", (incident_id,))
        conn.execute(
            "UPDATE incidents SET llm_enriched = 1, "
            "llm_enriched_at = datetime('now') WHERE incident_id = ?",
            (incident_id,),
        )
        conn.commit()

    run_with_sqlite_lock_retry(
        conn,
        _apply,
        operation=f"clear stale curated articles for {incident_id}",
    )

# Module-level progress dict — updated during execution, read by pipeline manager
# Uses two sub-phases: "Fetching articles" (0-30%) and "LLM Enrichment" (30-100%)
_progress = {"step": "", "detail": "", "percent": 0}

# SKIP_ENRICHMENT_SOURCES / FETCH_IMPOSSIBLE_SOURCES imported from config above.
# Kept as module-level aliases for backward compatibility with any direct references.
SKIP_ENRICHMENT_SOURCES = ENRICHMENT_SKIP_SOURCES


def _create_secondary_incidents(
    conn,
    parent_incident: Dict,
    secondary_list: List[Dict],
    source_url: str,
) -> None:
    """
    Create stub incidents for secondary victims found in a roundup article.

    Each stub gets:
    - The roundup article URL in all_urls so Phase 2 can SERP-discover a dedicated article
    - llm_enriched=0 so it will be picked up for enrichment on the next run
    - Source attribution copied from the parent incident

    Name+date dedup in _ingest_batch will silently skip any that already exist.
    """
    from src.edu_cti.core.models import BaseIncident, make_incident_id
    from src.edu_cti.core.db import (
        insert_incident,
        add_incident_source,
        source_event_exists,
        run_with_sqlite_lock_retry,
    )
    from src.edu_cti.core.db import find_duplicate_by_name_and_date

    parent_source = (parent_incident.get("source") or
                     parent_incident.get("incident_id", "unknown").split("_")[0])

    created = 0
    # Names that indicate the LLM couldn't identify the institution.
    # Creating a stub for these is pointless — SERP can't search for "Unknown"
    # and the LLM prompt hint would be useless. Skip them entirely.
    _INVALID_VICTIM_NAMES = {
        "", "unknown", "unknown school", "unknown institution", "unknown university",
        "unnamed", "unnamed school", "undisclosed", "undisclosed institution",
        "n/a", "none", "redacted", "unidentified",
    }

    skipped = 0
    for entry in secondary_list:
        victim_name = (entry.get("victim_name") or "").strip()
        if not victim_name or victim_name.lower() in _INVALID_VICTIM_NAMES:
            logger.debug(f"Skipping secondary stub with unusable victim name: {victim_name!r}")
            skipped += 1
            continue

        incident_date = entry.get("incident_date") or None
        _at           = entry.get("attack_type")
        # LLM sometimes returns list values in secondary incident dicts
        attack_type   = (_at[0] if _at else None) if isinstance(_at, list) else (_at or None)
        country       = entry.get("country") or None
        if isinstance(country, list):
            country = country[0] if country else None
        brief_desc    = entry.get("brief_description") or None
        if isinstance(brief_desc, list):
            brief_desc = " ".join(str(x) for x in brief_desc) if brief_desc else None

        # Build notes: store the roundup reference + brief description for context.
        # The roundup URL is intentionally NOT put in all_urls — if it were, the
        # next enrichment run would re-fetch the same roundup article, the LLM
        # would see all N victims again with no context about which one to focus on,
        # and would arbitrarily pick a different "primary". Instead we leave
        # all_urls=[] so fetch_articles_phase() triggers SERP to find a dedicated article.
        stub_notes_parts = []
        if source_url:
            stub_notes_parts.append(f"Extracted from roundup: {source_url}")
        if brief_desc:
            stub_notes_parts.append(brief_desc)
        stub_notes = "\n".join(stub_notes_parts) or None

        # Dedup: skip if an incident for this victim+date already exists in DB
        stub = BaseIncident(
            incident_id="",  # temp — assigned below
            source=parent_source,
            source_event_id=None,
            institution_name=victim_name,
            victim_raw_name=victim_name,
            institution_type=None,
            country=country,
            region=None,
            city=None,
            incident_date=incident_date,
            date_precision="unknown",
            source_published_date=None,
            ingested_at=None,
            title=victim_name,
            subtitle=None,
            primary_url=None,
            all_urls=[],  # empty: SERP discovery will find a dedicated article
            attack_type_hint=attack_type,
            notes=stub_notes,
        )
        dup_id = find_duplicate_by_name_and_date(conn, stub)
        if dup_id:
            logger.debug(f"Secondary incident already exists for '{victim_name}' → {dup_id}")
            skipped += 1
            continue

        # Build stable ID and event key
        dedup_key = f"{victim_name.lower()}|{incident_date or ''}|roundup_extract"
        incident_id = make_incident_id(parent_source, dedup_key)

        if source_event_exists(conn, parent_source, dedup_key):
            skipped += 1
            continue

        stub.incident_id = incident_id
        stub.source_event_id = dedup_key
        first_seen_at = (
            parent_incident.get("ingested_at")
            or datetime.utcnow().isoformat() + "Z"
        )
        confidence = parent_incident.get("source_confidence")

        try:
            def _create_once() -> None:
                insert_incident(conn, stub)
                add_incident_source(
                    conn,
                    incident_id,
                    parent_source,
                    dedup_key,
                    first_seen_at,
                    confidence,
                )
                conn.commit()

            run_with_sqlite_lock_retry(
                conn,
                _create_once,
                operation=f"create secondary incident {incident_id}",
            )
            created += 1
            logger.info(
                f"Created secondary incident {incident_id} for '{victim_name}' "
                f"(extracted from roundup article)"
            )
        except Exception as e:
            logger.warning(f"Failed to create secondary incident for '{victim_name}': {e}")

    if created or skipped:
        logger.info(
            f"Secondary incidents from roundup: {created} created, {skipped} already exist"
        )


def dict_to_incident(incident_dict: Dict, conn) -> BaseIncident:
    """Convert incident dict to BaseIncident."""
    from src.edu_cti.core.db import get_incident_sources
    
    # Get source list to get primary source
    sources = get_incident_sources(conn, incident_dict["incident_id"])
    
    # Get primary source (first one seen)
    primary_source = sources[0]["source"] if sources else "unknown"
    primary_source_event_id = sources[0]["source_event_id"] if sources else None
    
    return BaseIncident(
        incident_id=incident_dict["incident_id"],
        source=primary_source,
        source_event_id=primary_source_event_id,
        title=incident_dict.get("title"),
        subtitle=incident_dict.get("subtitle"),
        institution_name=incident_dict.get("institution_name") or incident_dict.get("victim_raw_name") or "Unknown",
        victim_raw_name=incident_dict.get("victim_raw_name"),
        institution_type=incident_dict.get("institution_type"),
        country=incident_dict.get("country"),
        region=incident_dict.get("region"),
        city=incident_dict.get("city"),
        incident_date=incident_dict.get("incident_date"),
        date_precision=incident_dict.get("date_precision", "unknown"),
        source_published_date=incident_dict.get("source_published_date"),
        ingested_at=incident_dict.get("ingested_at"),
        all_urls=incident_dict.get("all_urls", []),
        primary_url=incident_dict.get("primary_url"),
        attack_type_hint=incident_dict.get("attack_type_hint"),
        status=incident_dict.get("status", "suspected"),
        source_confidence=incident_dict.get("source_confidence", "medium"),
        notes=incident_dict.get("notes"),
        re_enrich_attempts=incident_dict.get("re_enrich_attempts"),
        re_enrich_reason=incident_dict.get("re_enrich_reason"),
    )


_UNENRICHABLE_NAMES = {
    "unknown", "unknown school", "unknown institution", "unknown university",
    "unnamed", "unnamed school", "undisclosed", "n/a", "none",
    "redacted", "unidentified",
}


def _build_queue_payload(incident: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "incident_id": incident["incident_id"],
        "institution_name": incident.get("institution_name") or incident.get("victim_raw_name") or "Unknown",
        "victim_raw_name": incident.get("victim_raw_name"),
        "institution_type": None,
        "country": incident.get("country"),
        "region": incident.get("region"),
        "city": incident.get("city"),
        "incident_date": incident.get("incident_date"),
        "date_precision": incident.get("date_precision") or "unknown",
        "source_published_date": incident.get("source_published_date"),
        "ingested_at": None,
        "title": incident.get("title"),
        "subtitle": None,
        "primary_url": None,
        "all_urls": incident.get("all_urls") or [],
        "attack_type_hint": incident.get("attack_type_hint"),
        "status": incident.get("status") or "suspected",
        "source_confidence": incident.get("source_confidence") or "medium",
        "notes": incident.get("notes"),
        "re_enrich_attempts": incident.get("re_enrich_attempts"),
        "re_enrich_reason": incident.get("re_enrich_reason"),
    }


def _fetch_single_incident_for_queue(
    incident: Dict[str, Any],
    *,
    shared_rate_limiter,
    min_delay_seconds: float,
    max_delay_seconds: float,
) -> Dict[str, Any]:
    """
    Fetch and prepare exactly one incident for the enrichment queue.

    Runs in a fetch worker thread with its own DB connection and ArticleFetcher,
    then returns a queue payload as soon as this incident's article state is ready.
    """
    from src.edu_cti.pipeline.phase2.utils.fetching_strategy import (
        SmartArticleFetchingStrategy,
        discover_articles_via_serp,
    )
    from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher, ArticleContent
    from src.edu_cti.pipeline.phase2.storage.article_storage import get_all_articles_for_incident, save_article
    from src.edu_cti.core.db import clear_broken_urls, mark_urls_as_broken
    from src.edu_cti.pipeline.phase2.storage.db import mark_incident_skipped

    incident_id = incident["incident_id"]
    thread_conn = get_connection()
    article_fetcher = ArticleFetcher()
    fetching_strategy = SmartArticleFetchingStrategy(
        conn=thread_conn,
        rate_limiter=shared_rate_limiter,
        article_fetcher=article_fetcher,
    )
    result = {
        "incident_id": incident_id,
        "processed": 1,
        "articles_fetched": 0,
        "errors": 0,
        "queue_payload": None,
        "checkpoint_phase": None,
    }

    try:
        if _cancel_event.is_set():
            result["processed"] = 0
            return result

        incident_name = (incident.get("institution_name") or incident.get("victim_raw_name") or "").strip()
        incident_urls = incident.get("all_urls") or []
        if not incident_urls and incident_name.lower() in _UNENRICHABLE_NAMES:
            logger.info(
                "Skipping unenrichable stub %s (name=%r, no URLs) — marking skipped",
                incident_id,
                incident_name,
            )
            mark_incident_skipped(
                thread_conn,
                incident_id,
                reason=f"No URLs and unidentifiable institution name: {incident_name!r}",
            )
            thread_conn.commit()
            return result

        source_prefix = incident_id.split("_")[0]
        if source_prefix in SKIP_ENRICHMENT_SOURCES:
            logger.debug("Skipping fetch for IOC source incident: %s", incident_id)
            return result

        if source_prefix in FETCH_IMPOSSIBLE_SOURCES:
            existing_articles = get_all_articles_for_incident(thread_conn, incident_id)
            usable_existing_articles = [
                article
                for article in existing_articles
                if article.get("fetch_successful")
                and article.get("content")
                and len(article["content"].strip()) > 50
            ]
            if usable_existing_articles:
                result["queue_payload"] = _build_queue_payload(incident)
                result["articles_fetched"] = 1
                result["checkpoint_phase"] = "article_fetch"
                return result

            serp_urls = discover_articles_via_serp(incident)
            fetched_any = False
            for serp_url in serp_urls:
                domain = fetching_strategy.rate_limiter.extract_domain(serp_url)
                if not domain or not fetching_strategy.rate_limiter.can_fetch_from_domain(domain):
                    continue
                fetching_strategy.rate_limiter.wait_if_needed(domain)
                try:
                    ac = fetching_strategy.article_fetcher.fetch_article(serp_url)
                    fetching_strategy.rate_limiter.record_fetch(domain, success=ac.fetch_successful)
                    save_article(thread_conn, incident_id=incident_id, url=serp_url, article=ac)
                    if ac.fetch_successful:
                        thread_conn.commit()
                        fetched_any = True
                        break
                except Exception as exc:
                    logger.debug("SERP fetch error %s: %s", serp_url, exc)
                    save_article(
                        thread_conn,
                        incident_id=incident_id,
                        url=serp_url,
                        article=ArticleContent(
                            url=serp_url,
                            title="",
                            content="",
                            fetch_successful=False,
                            error_message=str(exc),
                            content_length=0,
                        ),
                    )
            if fetched_any:
                result["queue_payload"] = _build_queue_payload(incident)
                result["articles_fetched"] = 1
                result["checkpoint_phase"] = "article_fetch"
            else:
                _record_serp_failure(thread_conn, incident_id)
            return result

        existing_articles = get_all_articles_for_incident(thread_conn, incident_id)
        usable_existing_articles = [
            article
            for article in existing_articles
            if article.get("fetch_successful")
            and article.get("content")
            and len(article["content"].strip()) > 50
        ]
        has_existing_articles = len(usable_existing_articles) > 0

        results = fetching_strategy.fetch_articles_for_incidents([incident])
        articles = results.get(incident_id, [])
        successful_articles = [a for a in articles if a.fetch_successful]
        failed_articles = [a for a in articles if not a.fetch_successful]

        if failed_articles:
            broken_urls = [a.url for a in failed_articles if a.url]
            if broken_urls:
                mark_urls_as_broken(thread_conn, incident_id, broken_urls)
                logger.info("Marked %d URL(s) as broken for incident %s", len(broken_urls), incident_id)
                thread_conn.commit()

        should_queue = False
        if successful_articles:
            successful_urls = [a.url for a in successful_articles if a.url]
            if successful_urls:
                clear_broken_urls(thread_conn, incident_id, successful_urls)
                thread_conn.commit()
            should_queue = True
            result["articles_fetched"] = 1
            logger.info("Fetched %d articles for %s", len(successful_articles), incident_id)
        elif has_existing_articles:
            current_urls = set(incident.get("all_urls") or [])
            existing_urls = {a["url"] for a in usable_existing_articles if a.get("url")}
            aligned = current_urls & existing_urls if current_urls else existing_urls
            if current_urls and not aligned:
                thread_conn.execute("DELETE FROM articles WHERE incident_id = ?", (incident_id,))
                thread_conn.commit()
                logger.warning(
                    "Purged %d stale article(s) for %s (DB URLs %s ∉ current all_urls %s)",
                    len(existing_articles),
                    incident_id,
                    existing_urls,
                    current_urls,
                )
            else:
                should_queue = True
                result["articles_fetched"] = 1
                logger.info("Incident %s has %d aligned articles in DB", incident_id, len(existing_articles))

        if should_queue:
            result["queue_payload"] = _build_queue_payload(incident)
            result["checkpoint_phase"] = "article_fetch"
        elif not successful_articles and not has_existing_articles:
            if source_prefix == "comparitech":
                inc_name = incident.get("institution_name") or incident.get("victim_raw_name") or "an educational institution"
                inc_date = incident.get("incident_date") or ""
                inc_city = incident.get("city") or ""
                inc_region = incident.get("region") or ""
                inc_country = incident.get("country") or "US"
                inc_notes = incident.get("notes") or ""
                inc_title = incident.get("title") or f"Ransomware attack on {inc_name}"
                location_parts = [p for p in [inc_city, inc_region, inc_country] if p]
                location_str = ", ".join(location_parts) if location_parts else inc_country
                synthetic_content = (
                    f"{inc_title}\n\n"
                    f"{inc_name} suffered a ransomware cyberattack"
                    + (f" in {inc_date[:4]}" if inc_date else "") + "."
                    + (f" Location: {location_str}." if location_str else "")
                    + (f"\n\nDetails from Comparitech tracker: {inc_notes}" if inc_notes else "")
                    + "\n\nThis is a confirmed ransomware incident targeting an educational institution "
                    "recorded in the Comparitech ransomware attack database."
                )
                synthetic_article = ArticleContent(
                    url=f"comparitech://synthetic/{incident_id}",
                    title=inc_title,
                    content=synthetic_content,
                    fetch_successful=True,
                )
                save_article(
                    thread_conn,
                    incident_id=incident_id,
                    url=synthetic_article.url,
                    article=synthetic_article,
                )
                thread_conn.commit()
                result["queue_payload"] = _build_queue_payload(incident)
                result["checkpoint_phase"] = "article_fetch"
                result["articles_fetched"] = 1
                logger.info("Comparitech: synthesized article for %s (%s)", incident_id, inc_name)
            else:
                deleted = _record_serp_failure(thread_conn, incident_id)
                result["errors"] += 1
                if deleted:
                    logger.info("Deleted unenrichable incident %s after exhausted SERP retries", incident_id)

        return result
    except Exception as exc:
        logger.error("Error fetching articles for %s: %s", incident_id, exc, exc_info=True)
        result["errors"] += 1
        return result
    finally:
        try:
            article_fetcher.http_client.close()
        except Exception:
            pass
        thread_conn.close()


def fetch_articles_phase(
    conn,
    unenriched: List[Dict],
    incident_queue: queue.Queue,
    limit: Optional[int] = None,
    fetch_workers: Optional[int] = None,
    min_delay_seconds: float = 0.5,  # Oxylabs rotates IPs — no need for long domain delays
    max_delay_seconds: float = 1.5,
) -> Dict[str, int]:
    """
    Phase 1: Fetch articles and store in database using smart fetching strategy.
    
    Uses domain-based rate limiting and random incident selection to avoid
    bot detection and ensure efficient fetching.
    
    Pushes incidents to queue as soon as articles are fetched (producer pattern).
    
    Args:
        conn: Database connection
        unenriched: List of unenriched incidents
        incident_queue: Queue to push incidents with fetched articles
        limit: Maximum number of incidents to process
        fetch_workers: Number of concurrent article fetch workers. Defaults to
            a small bounded value based on workload if not provided.
        min_delay_seconds: Minimum delay between fetches
        max_delay_seconds: Maximum delay between fetches
    
    Returns:
        Statistics dict with counts
    """
    logger = logging.getLogger(__name__)
    logger.info("PHASE 1: Article Fetching")
    
    from src.edu_cti.pipeline.phase2.utils.fetching_strategy import (
        SmartArticleFetchingStrategy,
        DomainRateLimiter,
    )
    from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher
    
    # Initialize rate limiter and fetching strategy
    rate_limiter = DomainRateLimiter(
        min_delay_seconds=min_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        max_fetches_per_hour=200,  # Oxylabs rotates IPs — server-side per-IP limits don't apply
        block_duration_seconds=3600,
    )
    
    article_fetcher = ArticleFetcher()
    fetching_strategy = SmartArticleFetchingStrategy(
        conn=conn,
        rate_limiter=rate_limiter,
        article_fetcher=article_fetcher,
    )
    
    stats = {
        "processed": 0,
        "articles_fetched": 0,
        "errors": 0,
    }
    
    # Get randomly selected incidents with domain diversity
    num_incidents = limit if limit else len(unenriched)
    incidents_to_process = fetching_strategy.get_random_incidents_for_enrichment(
        limit=num_incidents,
        exclude_domains=[],  # Can add recently blocked domains here
    )
    
    # If we got fewer incidents than requested due to domain filtering,
    # try again with less strict filtering
    if len(incidents_to_process) < num_incidents and num_incidents < len(unenriched):
        logger.warning(
            f"Only selected {len(incidents_to_process)}/{num_incidents} incidents due to domain filtering. "
            f"Retrying with less strict filtering..."
        )
        # Get more incidents, including those with potentially blocked domains
        additional_needed = num_incidents - len(incidents_to_process)
        additional = fetching_strategy.get_random_incidents_for_enrichment(
            limit=additional_needed * 2,  # Get 2x to account for filtering
            exclude_domains=[],  # Don't exclude any domains
        )
        # Add unique incidents
        existing_ids = {inc["incident_id"] for inc in incidents_to_process}
        for inc in additional:
            if inc["incident_id"] not in existing_ids:
                incidents_to_process.append(inc)
                if len(incidents_to_process) >= num_incidents:
                    break
    
    if not incidents_to_process:
        logger.warning("No incidents available for fetching")
        return stats
    
    logger.info(f"Selected {len(incidents_to_process)} incidents for fetching (random selection with domain diversity)")

    # --- Fast-path: incidents that already have articles saved from a previous run ---
    # Push them directly to the LLM queue without re-fetching anything, BUT only
    # when the stored article URLs are aligned with the incident's current all_urls.
    # If all_urls changed between runs (scraper re-ingested a different URL), the DB
    # articles are stale and would produce misaligned enrichment (wrong primary_url).
    from src.edu_cti.pipeline.phase2.storage.article_storage import get_all_articles_for_incident
    fast_path_raw = [i for i in incidents_to_process if i.get("has_articles")]
    needs_fetch = [i for i in incidents_to_process if not i.get("has_articles")]
    fast_path = []
    for fp_incident in fast_path_raw:
        fp_id = fp_incident["incident_id"]
        current_urls = set(fp_incident.get("all_urls") or [])
        if current_urls:
            db_articles = get_all_articles_for_incident(conn, fp_id)
            db_urls = {a["url"] for a in db_articles if a.get("url")}
            if not (current_urls & db_urls):
                # No overlap — stale articles from a different URL set.  Purge and re-fetch.
                conn.execute("DELETE FROM articles WHERE incident_id = ?", (fp_id,))
                conn.commit()
                logger.warning(
                    f"Fast-path purged stale articles for {fp_id}: "
                    f"DB had {db_urls}, current all_urls={current_urls}"
                )
                needs_fetch.append(fp_incident)
                continue
        fast_path.append(fp_incident)

    if fast_path:
        logger.info(f"Fast-pathing {len(fast_path)} incidents with existing aligned articles straight to LLM queue")
        for fp_incident in fast_path:
            incident_queue.put(_build_queue_payload(fp_incident))
            stats["processed"] += 1
            stats["articles_fetched"] += 1
    incidents_to_process = needs_fetch

    if not incidents_to_process:
        logger.info("No fresh article fetch work after fast-path; returning queued incidents immediately")
        try:
            article_fetcher.http_client.close()
        except Exception:
            pass
        return stats

    # Keep the watchdog alive during the fetch phase via a background thread.
    # The main loop heartbeats once per incident, but a single slow fetch_article()
    # call (hanging newspaper3k / slow proxy) can block for minutes without
    # returning — preventing the next iteration's heartbeat.  This thread fires
    # every 30 s unconditionally, so the watchdog never false-fires during fetch.
    import threading as _threading
    _keepalive_stop = _threading.Event()

    def _fetch_keepalive():
        while not _keepalive_stop.is_set():
            if _watchdog:
                _watchdog.heartbeat()
            _keepalive_stop.wait(30)

    _keepalive_thread = _threading.Thread(target=_fetch_keepalive, daemon=True, name="fetch-keepalive")
    _keepalive_thread.start()

    try:
        total_incidents = len(incidents_to_process)
        if fetch_workers is None:
            fetch_workers = 1 if total_incidents <= 1 else min(2, total_incidents)
        else:
            fetch_workers = max(1, min(int(fetch_workers), total_incidents or 1))
        logger.info(
            "Starting %d fetch worker(s); incidents will be queued for LLM enrichment as soon as each fetch completes",
            fetch_workers,
        )

        future_map = {}
        with ThreadPoolExecutor(max_workers=fetch_workers, thread_name_prefix="fetcher") as executor:
            for incident in incidents_to_process:
                if _cancel_event.is_set():
                    logger.info("Cancel requested — stopping fetch submissions")
                    break
                future = executor.submit(
                    _fetch_single_incident_for_queue,
                    incident,
                    shared_rate_limiter=rate_limiter,
                    min_delay_seconds=min_delay_seconds,
                    max_delay_seconds=max_delay_seconds,
                )
                future_map[future] = incident["incident_id"]

            completed = 0
            for future in as_completed(future_map):
                completed += 1
                progress_pct = (completed / total_incidents) * 100 if total_incidents else 100
                _progress["step"] = "Fetching articles"
                _progress["detail"] = f"{completed}/{total_incidents} ({stats['articles_fetched']} fetched)"
                _progress["percent"] = int(progress_pct * 0.30)
                if completed % 10 == 0 or completed == total_incidents:
                    logger.info("Fetching [%d/%d] (%.1f%%)", completed, total_incidents, progress_pct)
                if _watchdog:
                    _watchdog.heartbeat()

                try:
                    fetch_result = future.result()
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(
                        "Unhandled fetch worker error for %s: %s",
                        future_map.get(future, "unknown"),
                        e,
                        exc_info=True,
                    )
                    continue

                stats["processed"] += fetch_result.get("processed", 0)
                stats["articles_fetched"] += fetch_result.get("articles_fetched", 0)
                stats["errors"] += fetch_result.get("errors", 0)

                incident_payload = fetch_result.get("queue_payload")
                if incident_payload:
                    incident_queue.put(incident_payload)
                    _metrics.set_gauge("pipeline_queue_depth", float(incident_queue.qsize()))
                    logger.debug(
                        "Pushed %s to queue (size: %s)",
                        incident_payload["incident_id"],
                        incident_queue.qsize(),
                    )
                    checkpoint_mark(
                        conn,
                        incident_payload["incident_id"],
                        phase=fetch_result.get("checkpoint_phase") or "article_fetch",
                    )

    except Exception as e:
        stats["errors"] += max(1, len(incidents_to_process))
        logger.error(f"Error during article fetching: {e}")
    finally:
        # Stop the keepalive thread — fetch phase is done.
        _keepalive_stop.set()
        _keepalive_thread.join(timeout=5)
        # Close HttpClient so its Playwright ThreadPoolExecutor is shut down.
        # Without this, each run leaks one executor thread per ArticleFetcher,
        # eventually exhausting the OS thread limit ("can't start new thread").
        try:
            article_fetcher.http_client.close()
        except Exception:
            pass

    logger.info(f"Fetching complete: {stats['processed']} processed, {stats['articles_fetched']} fetched, {stats['errors']} errors")

    return stats


def enrich_articles_phase(
    conn,
    enricher: IncidentEnricher,
    incident_queue: queue.Queue,
    fetch_complete_event: threading.Event,
    skip_if_not_education: bool = False,
    rate_limit_delay: float = 1.0,
    total_expected: int = 0,
    cancel_event: Optional[threading.Event] = None,
    db_already_enriched: int = 0,
    db_total_incidents: int = 0,
) -> Dict[str, int]:
    """
    Phase 2: Sequential LLM enrichment (queue-based consumer).

    Args:
        conn: Database connection
        enricher: IncidentEnricher instance
        incident_queue: Queue to consume incidents from
        fetch_complete_event: Event to signal when fetching is complete
        skip_if_not_education: Whether to skip non-education incidents
        rate_limit_delay: Delay between LLM calls
        total_expected: Incidents to process in this run (for per-run progress)
        db_already_enriched: Incidents already enriched in DB at run start (for cumulative display)
        db_total_incidents: Total incidents in DB at run start (for cumulative display)

    Returns:
        Statistics dict with counts
    """
    logger = logging.getLogger(__name__)
    logger.info("PHASE 2: LLM Enrichment")
    
    stats = {
        "processed": 0,
        "enriched": 0,
        "skipped": 0,
        "errors": 0,
    }
    
    # SENTINEL value to signal end of queue
    SENTINEL = None
    
    logger.debug(f"Queue initial state: empty={incident_queue.empty()}, size={incident_queue.qsize()}")
    
    # CONSUMER LOOP: Process incidents from queue as they arrive
    items_processed = 0
    while True:
        # Check for cancellation before processing next incident
        if cancel_event and cancel_event.is_set():
            logger.info(f"Cancel requested — stopping enrichment (processed {items_processed} items)")
            break

        try:
            # Block until an item is available (no timeout needed — sentinel handles exit)
            try:
                incident_dict = incident_queue.get(timeout=10.0)
            except queue.Empty:
                # Periodic wakeup — check cancel before blocking again
                if cancel_event and cancel_event.is_set():
                    logger.info(f"Cancel requested — stopping enrichment (processed {items_processed} items)")
                    break
                continue

            # Check for sentinel (None) — producer pushes one per worker after fetching
            if incident_dict is SENTINEL:
                logger.debug(f"Received sentinel — stopping consumer (processed {items_processed} items)")
                incident_queue.task_done()
                break
            
            incident_id = incident_dict["incident_id"]

            # Per-incident guard: if another worker already claimed this incident,
            # drop it — SQLite would serialise the write anyway and we'd overwrite.
            with _in_progress_lock:
                if incident_id in _in_progress:
                    logger.debug(f"Worker collision avoided — {incident_id} already in progress, skipping")
                    incident_queue.task_done()
                    continue
                _in_progress.add(incident_id)

            # Skip IOC-only sources that produce no education articles
            source_prefix = incident_id.split("_")[0]
            if source_prefix in SKIP_ENRICHMENT_SOURCES:
                logger.debug(f"Skipping IOC source incident: {incident_id}")
                stats["skipped"] += 1
                with _in_progress_lock:
                    _in_progress.discard(incident_id)
                incident_queue.task_done()
                continue

            stats["processed"] += 1

            # Calculate progress percentage - log every 10th or at milestones
            # Enrichment phase maps to 30-100% of overall progress
            if total_expected > 0:
                raw_pct = (stats["processed"] / total_expected) * 100
                # Map 0-100% enrichment progress into 30-100% overall range
                scaled_pct = 30 + int(raw_pct * 0.70)
                # Cumulative DB progress: already enriched before this run + enriched so far in this run
                db_enriched_now = db_already_enriched + stats.get("enriched", 0)
                db_total_str = f"/{db_total_incidents}" if db_total_incidents else ""
                # Update module-level progress for pipeline manager
                _progress["step"] = "LLM Enrichment"
                _progress["detail"] = (
                    f"{stats['processed']}/{total_expected} this run | "
                    f"{db_enriched_now}{db_total_str} enriched in DB"
                )
                _progress["percent"] = min(scaled_pct, 100)
                if stats["processed"] % 10 == 0 or raw_pct % 10 < 1:  # Every 10 or every 10%
                    logger.info(
                        f"Enriching [{stats['processed']}/{total_expected} this run"
                        f" | {db_enriched_now}{db_total_str} enriched in DB]"
                        f" ({raw_pct:.1f}%) — "
                        f"{stats.get('enriched', 0)} enriched, {stats.get('skipped', 0)} skipped, {stats.get('errors', 0)} errors"
                    )
            else:
                if stats["processed"] % 10 == 0:
                    logger.info(f"Enriching [{stats['processed']}]")
            
            try:
                # Read full incident data from DB (queue only has minimal data)
                query = """
                    SELECT * FROM incidents WHERE incident_id = ?
                """
                cur = conn.execute(query, (incident_id,))
                row = cur.fetchone()
                
                if not row:
                    logger.warning(f"Incident {incident_id} not found in database")
                    stats["errors"] += 1
                    checkpoint_clear(conn, incident_id)
                    with _in_progress_lock:
                        _in_progress.discard(incident_id)
                    incident_queue.task_done()
                    continue

                # Check if already enriched (shouldn't happen, but check anyway)
                if row["llm_enriched"] == 1:
                    logger.warning(f"Incident {incident_id} is already enriched - skipping")
                    stats["skipped"] += 1
                    checkpoint_clear(conn, incident_id)
                    with _in_progress_lock:
                        _in_progress.discard(incident_id)
                    incident_queue.task_done()
                    continue
                
                # Build full incident dict from DB
                all_urls_str = row["all_urls"] or ""
                all_urls = [url.strip() for url in all_urls_str.split(";") if url.strip()]
                full_incident_dict = {
                    "incident_id": row["incident_id"],
                    "institution_name": row["institution_name"] or row["victim_raw_name"] or "Unknown",
                    "victim_raw_name": row["victim_raw_name"],
                    "institution_type": row["institution_type"],
                    "country": row["country"],
                    "region": row["region"],
                    "city": row["city"],
                    "incident_date": row["incident_date"],
                    "date_precision": row["date_precision"] or "unknown",
                    "source_published_date": row["source_published_date"],
                    "ingested_at": row["ingested_at"],
                    "title": row["title"],
                    "subtitle": row["subtitle"],
                    "primary_url": row["primary_url"],
                    "all_urls": all_urls,
                    "attack_type_hint": row["attack_type_hint"],
                    "status": row["status"] or "suspected",
                    "source_confidence": row["source_confidence"] or "medium",
                    "notes": row["notes"],
                }
                
                # Convert to BaseIncident
                incident = dict_to_incident(full_incident_dict, conn=conn)
                
                # Process incident (reads articles from DB) - SEQUENTIAL, ONE AT A TIME
                logger.debug(f"Processing {incident_id}")
                try:
                    enrichment_result = enricher.process_incident(
                        incident=incident,
                        skip_if_not_education=skip_if_not_education,
                        conn=conn,  # Required: pass connection to read articles from DB
                    )
                    logger.debug(f"process_incident returned: {type(enrichment_result)}, is None: {enrichment_result is None}")
                except Exception as e:
                    from src.edu_cti.pipeline.phase2.llm_client import RateLimitError
                    if isinstance(e, RateLimitError):
                        # Jittered exponential backoff — only THIS worker sleeps.
                        # Other workers keep enriching uninterrupted.
                        retry_count = incident_dict.get("_rate_limit_retries", 0)
                        wait_time = min(120, (2 ** retry_count) * 10 + random.uniform(0, 5))
                        logger.warning(
                            f"Rate limit for {incident_id} (retry #{retry_count+1}) — "
                            f"this worker sleeping {wait_time:.0f}s, others continue"
                        )
                        with _in_progress_lock:
                            _in_progress.discard(incident_id)
                        incident_queue.task_done()
                        # Annotate retry count so backoff grows on repeated hits
                        incident_dict["_rate_limit_retries"] = retry_count + 1
                        # Use put_nowait with fallback — queue may be full during back-pressure
                        try:
                            incident_queue.put_nowait(incident_dict)
                        except queue.Full:
                            # Queue full: drop requeue and let it be retried next pipeline run
                            logger.warning(f"Queue full — {incident_id} will retry next run")
                        time.sleep(wait_time)
                        stats["errors"] += 1
                        continue
                    elif isinstance(e, (TimeoutError, OSError)):
                        # LLM timeout/connection error — re-queue, brief pause, continue
                        logger.warning(f"Timeout/connection error for {incident_id}: {e} — requeueing")
                        with _in_progress_lock:
                            _in_progress.discard(incident_id)
                        incident_queue.task_done()
                        try:
                            incident_queue.put_nowait(incident_dict)
                        except queue.Full:
                            logger.warning(f"Queue full — {incident_id} timeout retry deferred to next run")
                        time.sleep(2.0)  # Reduced from 5s — other workers unaffected
                        stats["errors"] += 1
                        continue
                    else:
                        logger.error(f"Error in process_incident for {incident_id}: {e}", exc_info=True)
                        stats["errors"] += 1
                        with _in_progress_lock:
                            _in_progress.discard(incident_id)
                        incident_queue.task_done()
                        continue
                
                # process_incident returns tuple (enrichment_result, raw_json_data)
                if isinstance(enrichment_result, tuple):
                    enrichment_result, raw_json_data = enrichment_result
                else:
                    raw_json_data = None
                
                if enrichment_result:
                    # Save enrichment result (with raw JSON data for country/region/city extraction)
                    saved = save_enrichment_result(conn, incident_id, enrichment_result, raw_json_data=raw_json_data)
                    if saved:
                        # --- Post-save: delete sector-report / no-victim incidents ---
                        # If the LLM could not identify a specific institution (institution_name
                        # is null or a placeholder like "Unknown") this is a trend/report
                        # article, not a specific incident.  Delete it so the DB only contains
                        # real discrete incidents.
                        _UNKNOWN_INSTITUTION_NAMES = {
                            "", "unknown", "unknown institution", "unknown school",
                            "unknown university", "unnamed", "unidentified", "undisclosed",
                            "n/a", "none", "redacted",
                        }
                        resolved_name = ""
                        if enrichment_result.education_relevance and enrichment_result.education_relevance.institution_identified:
                            resolved_name = enrichment_result.education_relevance.institution_identified.strip()
                        elif raw_json_data and raw_json_data.get("institution_name"):
                            resolved_name = str(raw_json_data["institution_name"]).strip()
                        # Also check the original incident name (may have been set by ingestor).
                        # incident can be a dict OR a BaseIncident Pydantic object.
                        _ig = (lambda f: incident.get(f) if isinstance(incident, dict) else getattr(incident, f, None))
                        original_name = (_ig("institution_name") or _ig("victim_raw_name") or "").strip()
                        effective_name = resolved_name or original_name

                        if effective_name.lower() in _UNKNOWN_INSTITUTION_NAMES:
                            from src.edu_cti.pipeline.phase2.storage.db import delete_incident
                            if delete_incident(conn, incident_id, reason="sector_report_no_institution"):
                                conn.commit()
                                stats["skipped"] += 1
                                checkpoint_clear(conn, incident_id)
                                logger.info(f"⊘ EXCLUDED  {incident_id} | no specific institution identified (sector report/trend article)")
                            else:
                                conn.commit()
                                stats["enriched"] += 1
                                checkpoint_clear(conn, incident_id)
                                logger.warning(f"✓ ENRICHED  {incident_id} | WARNING: institution unknown — could not delete")
                            with _in_progress_lock:
                                _in_progress.discard(incident_id)
                            incident_queue.task_done()
                            continue

                        # Commit immediately after each save to prevent long-running transactions
                        # This allows API reads to proceed while enrichment is running
                        conn.commit()
                        stats["enriched"] += 1
                        checkpoint_clear(conn, incident_id)
                        if _watchdog:
                            _watchdog.heartbeat()
                        primary = enrichment_result.primary_url or ""
                        logger.info(
                            f"✓ ENRICHED  {incident_id} | {primary[:80]}"
                        )

                        # --- Research metrics: field completeness + source novelty ---
                        _emit_enrichment_metrics(incident_id, enrichment_result, raw_json_data, conn)

                        # Inline dedup: check immediately whether this incident duplicates
                        # an existing enriched incident and merge on the spot.
                        try:
                            survivor = dedup_incident_after_save(conn, incident_id)
                            if survivor and survivor != incident_id:
                                logger.info(f"  ↳ inline dedup: {incident_id} merged into {survivor}")
                        except Exception as _dedup_err:
                            logger.warning(f"Inline dedup error for {incident_id}: {_dedup_err}")

                        # --- Secondary incidents from roundup articles ---
                        # If the LLM detected other edu victims in the same article
                        # (e.g. "week in breach" digest), create stub incidents for each.
                        # They have no article URLs yet — Phase 2 SERP discovery will
                        # find dedicated articles for them on the next pipeline run.
                        if enrichment_result.other_edu_incidents:
                            _ig2 = (lambda f: incident.get(f) if isinstance(incident, dict) else getattr(incident, f, None))
                            _all_urls = _ig2("all_urls") or []
                            _source_url = _ig2("primary_url") or (_all_urls[0] if _all_urls else "")
                            _create_secondary_incidents(
                                conn,
                                parent_incident=incident if isinstance(incident, dict) else incident.__dict__,
                                secondary_list=enrichment_result.other_edu_incidents,
                                source_url=_source_url,
                            )
                    else:
                        stats["skipped"] += 1
                        logger.warning(f"~ SKIPPED   {incident_id} | save_enrichment_result returned falsy (unexpected)")
                else:
                    # Check what kind of failure we have
                    if raw_json_data and isinstance(raw_json_data, dict):
                        if raw_json_data.get("_not_education_related"):
                            # Curated education sources (comparitech, ransomwarelive, etc.) list
                            # real incidents but their stored articles may be stale/wrong.
                            # LLM correctly says "not edu" about wrong articles — but the incident
                            # IS real.  Protect these from deletion: clear bad articles and keep
                            # the incident as a stub (source metadata is still valuable).
                            _CURATED_EDU_SOURCES = {"comparitech", "ransomlook"}
                            _src_prefix = incident_id.split("_")[0]
                            if _src_prefix in _CURATED_EDU_SOURCES:
                                # Delete stale articles so they don't cause wrong classification again.
                                # Mark llm_enriched=1 so SERP doesn't retry this incident forever —
                                # the source metadata (institution, attack type, date) is already good.
                                _mark_curated_incident_stub(conn, incident_id)
                                stats["skipped"] += 1
                                checkpoint_clear(conn, incident_id)
                                logger.warning(
                                    f"~ STUB      {incident_id} | curated-source: wrong articles cleared, "
                                    f"kept as metadata stub (institution/date/attack from source)"
                                )
                            else:
                                # Non-curated source — soft-delete (keep row, clear articles/enrichments)
                                from src.edu_cti.pipeline.phase2.storage.db import delete_incident
                                reason = raw_json_data.get("_reason", "Not education-related")
                                if delete_incident(conn, incident_id, reason="not_education_related"):
                                    stats["skipped"] += 1
                                    checkpoint_clear(conn, incident_id)
                                    logger.info(f"⊘ EXCLUDED  {incident_id} | not edu: {reason[:80]}")
                                else:
                                    stats["errors"] += 1
                                    logger.warning(f"✗ EXCL-FAIL {incident_id} | could not soft-exclude non-edu incident")
                        elif raw_json_data.get("_enrichment_failed"):
                            # Enrichment failed (JSON parsing, etc.) - DON'T mark as skipped, will retry
                            reason = raw_json_data.get("_reason", "Enrichment failed")
                            stats["errors"] += 1
                            logger.warning(f"✗ RETRY     {incident_id} | LLM failed: {reason[:80]}")
                        else:
                            stats["errors"] += 1
                            logger.warning(f"✗ RETRY     {incident_id} | unknown LLM result")
                    else:
                        # No enrichment result and no error info — typically means no
                        # fetchable article content.  Leaving llm_enriched=0 so the
                        # incident is retried on the next run.
                        stats["errors"] += 1
                        logger.warning(f"✗ RETRY     {incident_id} | no article content (will retry)")
                
                # Rate limiting between LLM calls
                if rate_limit_delay > 0:
                    logger.debug(f"Waiting {rate_limit_delay}s before next LLM call...")
                    time.sleep(rate_limit_delay)
                
                # Mark task as done
                with _in_progress_lock:
                    _in_progress.discard(incident_id)
                incident_queue.task_done()
                items_processed += 1
                # Heartbeat on any completed item (not just successful enrichment)
                # so the watchdog doesn't fire when enrichers are busy deleting/skipping.
                if _watchdog:
                    _watchdog.heartbeat()

                if _check_memory_pressure(items_processed):
                    logger.info(
                        "Memory pause requested — worker stopping after %s processed queue item(s)",
                        items_processed,
                    )
                    break

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"✗ Error processing incident {incident_id}: {e}", exc_info=True)
                with _in_progress_lock:
                    _in_progress.discard(incident_id)
                incident_queue.task_done()
                items_processed += 1
                if _watchdog:
                    _watchdog.heartbeat()
                if _check_memory_pressure(items_processed):
                    logger.info(
                        "Memory pause requested after error path — worker stopping after %s processed queue item(s)",
                        items_processed,
                    )
                    break
                # Don't mark as processed - will retry on next run

        except Exception as e:
            logger.error(f"✗ Error in consumer loop: {e}", exc_info=True)
            stats["errors"] += 1

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Phase 2: LLM enrichment pipeline for EduThreat-CTI.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run enrichment for all unenriched incidents
  python -m src.edu_cti.pipeline.phase2

  # Limit to first 10 incidents
  python -m src.edu_cti.pipeline.phase2 --limit 10

  # Process with custom batch size and rate limit
  python -m src.edu_cti.pipeline.phase2 --batch-size 5 --rate-limit-delay 3.0

  # Use 3 parallel consumer threads for faster enrichment
  python -m src.edu_cti.pipeline.phase2 --workers 3 --rate-limit-delay 0.5
        """,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of incidents to process (default: all unenriched)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=ENRICHMENT_BATCH_SIZE,
        help=f"Number of incidents to process per batch (default: {ENRICHMENT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--skip-non-education",
        action="store_true",
        default=True,
        help="Skip incidents not related to education (default: True)",
    )
    parser.add_argument(
        "--keep-non-education",
        action="store_false",
        dest="skip_non_education",
        help="Keep incidents even if not education-related",
    )
    parser.add_argument(
        "--rate-limit-delay",
        type=float,
        default=ENRICHMENT_RATE_LIMIT_DELAY,
        help=f"Delay between API calls in seconds (default: {ENRICHMENT_RATE_LIMIT_DELAY})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to log file (default: logs/pipeline.log)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel enrichment consumer threads (default: 1). "
             "Higher values speed up enrichment but use more API quota.",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export enriched dataset to CSV after completion",
    )
    parser.add_argument(
        "--csv-output",
        type=Path,
        default=None,
        help="Path to output CSV file (default: data/processed/enriched_dataset.csv)",
    )
    return parser.parse_args()


def main() -> Optional[Dict[str, Any]]:
    """Main entry point for Phase 2 enrichment pipeline."""
    args = parse_args()
    _clear_watchdog()
    _reset_memory_policy_cache()
    _reset_phase2_run_state()
    
    # Setup logging
    from pathlib import Path as PathLib
    log_file_path = PathLib(args.log_file) if args.log_file else None
    configure_logging(args.log_level, log_file=log_file_path)
    logger = logging.getLogger(__name__)

    safety_overrides = _apply_runtime_safety_overrides(args, logger)

    memory_policy = _get_memory_policy()
    if memory_policy:
        container_limit_mb = memory_policy.get("container_limit_mb")
        limit_detail = f", container={container_limit_mb} MB" if container_limit_mb else ""
        source_detail = f", source={memory_policy.get('source')}" if memory_policy.get("source") else ""
        logger.info(
            "Phase 2 memory policy enabled: soft=%s MB, hard=%s MB%s%s",
            memory_policy["soft_limit_mb"],
            memory_policy["hard_limit_mb"],
            limit_detail,
            source_detail,
        )
    else:
        logger.info("Phase 2 memory policy disabled or no container memory limit detected")
    
    # Initialize database
    conn = get_connection()
    init_db(conn)  # Ensure tables exist
    init_incident_enrichments_table(conn)
    init_articles_table(conn)
    
    try:
        # Get enrichment stats
        stats = get_enrichment_stats(conn)
        logger.info(f"Enrichment Statistics:")
        logger.info(f"  Total incidents: {stats['total_incidents']}")
        logger.info(f"  Already enriched: {stats['enriched_incidents']}")
        logger.info(f"  Unenriched: {stats['unenriched_incidents']}")
        logger.info(f"  Ready for enrichment: {stats['ready_for_enrichment']}")
        
        # Get actionable incidents for this run.
        # Checkpointed incidents are intentionally retained here — if articles were
        # already fetched before a crash/restart, fetch_articles_phase() will fast-path
        # them directly into the LLM queue instead of re-fetching.
        #
        # Cap each run to PHASE2_RUN_LIMIT incidents (default 1000) so we never
        # load all 11k+ unenriched rows into memory at once. Each row holds
        # multiple text columns (title, notes, all_urls, etc.) — at ~5 KB
        # average × 11k rows = ~55 MB just for the in-memory list, plus
        # downstream copies in the queue and fetching strategy. Processing
        # in batches of 1000 keeps that under 5 MB and lets each crash/restart
        # recover from a checkpoint within the same batch.
        # The pipeline manager / cron re-invokes Phase 2 after the batch
        # completes, so the next 1000 are picked up automatically.
        _run_limit = args.limit
        if _run_limit is None:
            _run_limit = int(os.environ.get("PHASE2_RUN_LIMIT", "1000"))
            logger.info("Phase 2 run cap: %d incidents (override via PHASE2_RUN_LIMIT)", _run_limit)
        args.limit = _run_limit
        unenriched = get_unenriched_incidents(conn, limit=args.limit)
        checkpointed = checkpoint_get_fetched(conn)
        if checkpointed:
            resumed = sum(1 for incident in unenriched if incident["incident_id"] in checkpointed)
            if resumed:
                logger.info(
                    f"Checkpoint: resuming {resumed} already-fetched incident(s) via fast-path"
                )

        if not unenriched:
            logger.info("No incidents ready for processing")
            return {
                "fetch_stats": {"processed": 0, "articles_fetched": 0, "errors": 0},
                "run_stats": {"processed": 0, "enriched": 0, "skipped": 0, "errors": 0},
                "enrichment_stats": stats,
                "memory_pause_requested": False,
                "memory_pause_reason": None,
            }

        # PHASE 1 & 2: Concurrent producer-consumer pattern
        already_e = stats.get("enriched_incidents", 0)
        total_db  = stats.get("total_incidents", 0)
        logger.info(f"{'='*60}")
        logger.info(
            f"Phase 2 run starting | DB: {already_e}/{total_db} enriched | "
            f"{len(unenriched)} remaining in this batch"
        )
        logger.info(f"{'='*60}")
        
        # Initialize LLM enricher (needed for consumer thread)
        try:
            llm_client = OllamaLLMClient(
                api_key=OLLAMA_API_KEY,
                host=OLLAMA_HOST,
                model=OLLAMA_MODEL,
            )
            enricher = IncidentEnricher(llm_client=llm_client)
            logger.info(f"Initialized LLM client with model: {OLLAMA_MODEL}")
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            logger.error("Make sure OLLAMA_API_KEY is set in environment")
            sys.exit(1)
        
        # Start enrichment watchdog — kills process if no heartbeat for 10 minutes.
        # The fetch phase can be slow (SERP retries, TheRecord pagination), so we give
        # it 600s before declaring a stall. Railway auto-restarts on exit(1).
        global _watchdog
        _watchdog = EnrichmentWatchdog(stall_seconds=600)
        _watchdog.start()

        # Create queue and synchronization primitives
        # maxsize=100 provides back-pressure: if enrichment falls behind (slow LLM),
        # the fetch producer blocks on put() automatically — prevents memory explosion.
        incident_queue = queue.Queue(maxsize=100)
        fetch_complete_event = threading.Event()
        fetch_stats = {"processed": 0, "articles_fetched": 0, "errors": 0}

        # Total incidents to process (for progress tracking)
        total_to_process = len(unenriched)
        num_workers = max(1, min(args.workers, 8))  # Cap at 8 workers
        if num_workers != safety_overrides["effective_workers"]:
            safety_overrides["effective_workers"] = num_workers

        # Snapshot DB enrichment state at run start for cumulative progress display.
        # This lets each progress line show "[N/M this run | X/Y enriched in DB]"
        # so it's clear how far along the overall dataset we are, not just this cycle.
        db_already_enriched = stats.get("enriched_incidents", 0)
        db_total_incidents = stats.get("total_incidents", 0)

        # Aggregated stats across all consumer threads (thread-safe)
        stats_lock = threading.Lock()
        combined_enrich_stats = {"processed": 0, "enriched": 0, "skipped": 0, "errors": 0}

        def consumer_thread(worker_id: int):
            """Consumer thread that processes incidents from queue."""
            # Each worker gets its own DB connection and LLM client
            thread_conn = get_connection()
            try:
                if num_workers > 1:
                    # Each worker needs its own LLM client for thread safety
                    worker_llm = OllamaLLMClient(
                        api_key=OLLAMA_API_KEY,
                        host=OLLAMA_HOST,
                        model=OLLAMA_MODEL,
                    )
                    worker_enricher = IncidentEnricher(llm_client=worker_llm)
                else:
                    worker_enricher = enricher

                logger.info(f"Worker-{worker_id} started")
                worker_stats = enrich_articles_phase(
                    conn=thread_conn,
                    enricher=worker_enricher,
                    incident_queue=incident_queue,
                    fetch_complete_event=fetch_complete_event,
                    skip_if_not_education=args.skip_non_education,
                    rate_limit_delay=args.rate_limit_delay,
                    total_expected=total_to_process,
                    cancel_event=_cancel_event,
                    db_already_enriched=db_already_enriched,
                    db_total_incidents=db_total_incidents,
                )
                s = worker_stats
                logger.info(
                    f"Worker-{worker_id} done — "
                    f"processed={s['processed']} enriched={s['enriched']} "
                    f"skipped={s['skipped']} errors={s['errors']}"
                )

                # Merge stats thread-safely
                with stats_lock:
                    for key in combined_enrich_stats:
                        combined_enrich_stats[key] += worker_stats.get(key, 0)
            except Exception as e:
                logger.error(f"Error in consumer worker-{worker_id}: {e}", exc_info=True)
                with stats_lock:
                    combined_enrich_stats["errors"] += 1
            finally:
                thread_conn.close()

        # Constrain PyTorch CPU threading to a single thread per inference call.
        # set_num_interop_threads can only be called ONCE per process — calling
        # it again on subsequent main() invocations (auto-loop rounds 2+) raises
        # because parallel work has already started. Track whether we've already
        # set it so subsequent rounds skip the call silently.
        global _PYTORCH_THREADS_SET
        if not globals().get("_PYTORCH_THREADS_SET", False):
            try:
                os.environ.setdefault("OMP_NUM_THREADS", "1")
                os.environ.setdefault("MKL_NUM_THREADS", "1")
                os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
                os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
                os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
                import torch as _torch
                _torch.set_num_threads(1)
                _torch.set_num_interop_threads(1)
                _PYTORCH_THREADS_SET = True
                logger.info("PyTorch constrained to single-threaded inference (OMP/MKL/torch=1)")
            except Exception as _torch_err:
                # set_num_interop_threads can fail if called after work started.
                # set_num_threads still applies process-wide and is the more
                # important one for inference memory.
                logger.debug("PyTorch thread constraint partially applied: %s", _torch_err)
                _PYTORCH_THREADS_SET = True

        # Pre-warm ML models in the main thread before workers start.
        # Loading them here (sequential, low-memory moment) prevents the race
        # where all workers try to load 150 MB + 90 MB simultaneously mid-run.
        if safety_overrides["prewarm_ml_models"]:
            try:
                from src.edu_cti.pipeline.phase2.extraction.ner_preprocessor import _load_model as _ner_load
                from src.edu_cti.pipeline.phase2.extraction.mitre_rag import _load_embed_model as _rag_load, _load_index as _rag_index
                logger.info("Pre-warming ML models before enrichment workers start...")
                _ner_load()
                _rag_load()
                _rag_index()
                logger.info("ML model pre-warm complete.")
            except Exception as _pw_err:
                logger.warning("ML model pre-warm failed (non-fatal): %s", _pw_err)

        # Start a background memory janitor thread that runs every 30s
        # independent of worker progress. The per-worker memory check fires
        # on item-count multiples — but with 4 workers each counting locally,
        # only 7 total enrichments yield <2 items per worker, so no worker
        # ever crosses the gc_interval threshold and the HF cache prune
        # never fires. A timer-based thread guarantees the prune fires.
        _janitor_stop = threading.Event()

        def _memory_janitor():
            try:
                from src.edu_cti.core.config import DATA_DIR
                from pathlib import Path as _P
                import shutil as _sh
                import psutil as _ps
            except Exception as _imp_err:
                logger.warning("Memory janitor disabled: %s", _imp_err)
                return
            hf_cache = _P(DATA_DIR) / "hf_cache"
            HF_THRESHOLD_BYTES = 300 * 1024 * 1024  # 300 MB — far stricter than the 600 MB worker check
            # Hard ceiling: when Python RSS crosses this, force-cancel the run so
            # workers stop pulling new articles. Memory then drains as in-flight
            # enrichments finish. Far better than letting Railway OOM-kill the
            # whole container — that loses Playwright + DB-write state.
            # 5500 MB ≈ 72% of 7629 MB container limit.
            HARD_RSS_CEILING_MB = int(os.environ.get("PHASE2_RSS_HARD_CEILING_MB", "5500"))
            while not _janitor_stop.is_set():
                try:
                    rss_mb = _ps.Process(os.getpid()).memory_info().rss / (1024 * 1024)
                    cache_bytes = 0
                    if hf_cache.exists():
                        cache_bytes = sum(f.stat().st_size for f in hf_cache.rglob("*") if f.is_file())
                    cache_mb = cache_bytes / (1024 * 1024)
                    in_progress_count = len(_in_progress)
                    logger.info(
                        "[JANITOR] RSS=%.0f MB | hf_cache=%.0f MB | in_progress=%d",
                        rss_mb, cache_mb, in_progress_count,
                    )
                    # 1. HF cache prune (existing behaviour)
                    # Skip the rmtree if any worker is actively enriching, AND
                    # the cache is below an emergency ceiling. PyTorch holds
                    # mmap handles on the cached model files; deleting them
                    # while a worker is in the middle of inference can stress
                    # the kernel page cache and (rarely) trigger an OOM spike.
                    # We accept slightly larger cache (up to 600 MB) as long as
                    # workers are busy, only pruning when idle. Beyond 600 MB
                    # we prune anyway because the bloat itself is the bigger risk.
                    EMERGENCY_PRUNE_BYTES = 600 * 1024 * 1024
                    if cache_bytes > HF_THRESHOLD_BYTES and hf_cache.exists():
                        if in_progress_count > 0 and cache_bytes < EMERGENCY_PRUNE_BYTES:
                            logger.info(
                                "[JANITOR] HF cache=%.1f MB > %.0f MB threshold but %d worker(s) active — deferring prune",
                                cache_mb,
                                HF_THRESHOLD_BYTES / (1024 * 1024),
                                in_progress_count,
                            )
                        else:
                            logger.warning(
                                "[JANITOR] HF cache=%.1f MB > %.0f MB threshold — pruning (workers=%d)",
                                cache_mb,
                                HF_THRESHOLD_BYTES / (1024 * 1024),
                                in_progress_count,
                            )
                            try:
                                _sh.rmtree(hf_cache, ignore_errors=True)
                                hf_cache.mkdir(parents=True, exist_ok=True)
                                gc.collect()
                                try:
                                    import ctypes as _ct
                                    _ct.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
                                except Exception:
                                    pass
                                logger.info("[JANITOR] HF cache pruned + GC + malloc_trim")
                            except Exception as _e:
                                logger.warning("[JANITOR] HF prune failed: %s", _e)
                    # 2. Hard RSS ceiling — graceful cancel before Railway OOM
                    if rss_mb > HARD_RSS_CEILING_MB and not _cancel_event.is_set():
                        logger.error(
                            "[JANITOR] RSS=%.0f MB > %.0f MB hard ceiling — "
                            "signalling cancel to drain queue before Railway OOM-kill",
                            rss_mb, HARD_RSS_CEILING_MB,
                        )
                        _request_memory_pause(
                            rss_mb=rss_mb,
                            hard_limit_mb=HARD_RSS_CEILING_MB,
                            container_limit_mb=None,
                        )
                        # Aggressive reclamation
                        gc.collect()
                        try:
                            import ctypes as _ct
                            _ct.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
                        except Exception:
                            pass
                except Exception as _e:
                    logger.debug("[JANITOR] iteration failed: %s", _e)
                # 10s interval — fine-grained visibility into RSS growth rate.
                # Earlier 30s gap missed a 643 MB jump that caused OOM.
                _janitor_stop.wait(10)

        _janitor_thread = threading.Thread(
            target=_memory_janitor, daemon=True, name="memory-janitor"
        )
        _janitor_thread.start()
        logger.info("Started memory janitor (10s interval, prunes HF cache > 300 MB, cancels run > 5500 MB RSS)")

        # Start consumer threads
        consumers = []
        for i in range(num_workers):
            t = threading.Thread(target=consumer_thread, args=(i,), daemon=False, name=f"enricher-{i}")
            t.start()
            consumers.append(t)
        logger.info(f"Started {num_workers} enrichment consumer thread(s)")

        # Run producer (fetching) in main thread
        try:
            fetch_stats = fetch_articles_phase(
                conn,
                unenriched,
                incident_queue=incident_queue,
                limit=args.limit,
                fetch_workers=max(1, min(num_workers, 4)),
                min_delay_seconds=2.0,
                max_delay_seconds=5.0,
            )
        except Exception as e:
            logger.error(f"Error in producer (fetching): {e}", exc_info=True)
            fetch_stats["errors"] += 1
        finally:
            fetch_complete_event.set()
            # Push one sentinel (None) per consumer so each worker exits its get() loop cleanly
            # without backoff polling.
            for _ in range(num_workers):
                incident_queue.put(None)
            logger.info(f"Fetching complete — pushed {num_workers} sentinel(s) to queue")

        # Wait for all consumer threads — no timeout.
        # Each LLM call can take up to 180s, so even a modest batch (e.g. 500 incidents)
        # needs 500 * 180s = 25 hours. The old timeout of total*2 (e.g. 3300s = 55min)
        # was killing the consumer mid-enrichment. Use None to wait indefinitely; the
        # cancel_event mechanism provides graceful shutdown when needed.
        logger.info(f"Waiting for {num_workers} consumer(s) to finish (no timeout — cancel via admin panel)...")
        for t in consumers:
            # Poll every 60s so we can log progress and check for cancel
            while t.is_alive():
                t.join(timeout=60)
                if t.is_alive():
                    with stats_lock:
                        done = combined_enrich_stats["enriched"] + combined_enrich_stats["skipped"] + combined_enrich_stats["errors"]
                    logger.info(f"Still waiting for {t.name}... ({done}/{total_to_process} done so far)")

        logger.info("All consumer threads finished")

        # Stop the memory janitor (daemon, but explicit stop is cleaner)
        try:
            _janitor_stop.set()
        except Exception:
            pass

        # Final stats
        enrich_stats = combined_enrich_stats
        processed = fetch_stats["processed"]
        enriched = enrich_stats.get("enriched", 0)
        skipped = enrich_stats.get("skipped", 0)
        errors = fetch_stats.get("errors", 0) + enrich_stats.get("errors", 0)
        
        memory_state = _get_memory_guard_state()

        # Final pipeline stats
        logger.info("=" * 60)
        logger.info("Phase 2 Pipeline Complete")
        logger.info(f"  Articles Fetched: {fetch_stats['articles_fetched']}")
        logger.info(f"  LLM Enriched: {enriched}")
        logger.info(f"  Skipped: {skipped}")
        logger.info(f"  Errors: {errors}")
        if memory_state["pause_requested"]:
            logger.warning(f"  Memory Pause: {memory_state['reason']}")
        logger.info("=" * 60)
        
        # Post-enrichment deduplication by institution name (if enrichment occurred)
        if enrich_stats.get("enriched", 0) > 0:
            logger.info("=" * 60)
            logger.info("Running post-enrichment deduplication by institution name...")
            try:
                dedup_stats = deduplicate_by_institution(conn, window_days=14)
                logger.info(f"Post-enrichment deduplication complete:")
                logger.info(f"  Checked: {dedup_stats['checked']}")
                logger.info(f"  Removed: {dedup_stats['removed']}")
                logger.info(f"  Remaining: {dedup_stats['remaining']}")
            except Exception as e:
                logger.error(f"Error during post-enrichment deduplication: {e}", exc_info=True)
        
        # Update final stats
        final_stats = get_enrichment_stats(conn)
        logger.info(f"\n{'='*60}")
        logger.info("Final Statistics:")
        logger.info(f"  Ready for enrichment: {final_stats['ready_for_enrichment']}")
        logger.info(f"  Enriched incidents: {final_stats['enriched_incidents']}")
        logger.info(f"{'='*60}")
        
        # Export enriched dataset to CSV if requested
        if args.export_csv or args.csv_output:
            logger.info("=" * 60)
            logger.info("Exporting enriched dataset to CSV...")
            try:
                output_path = export_enriched_dataset(output_path=args.csv_output)
                if output_path:
                    logger.info(f"✓ Enriched dataset exported to: {output_path}")
                else:
                    logger.info("No enriched incidents to export")
            except Exception as e:
                logger.error(f"Error exporting enriched dataset: {e}", exc_info=True)
        return {
            "fetch_stats": fetch_stats,
            "run_stats": {
                "processed": processed,
                "enriched": enriched,
                "skipped": skipped,
                "errors": errors,
                "articles_fetched": fetch_stats["articles_fetched"],
            },
            "enrichment_stats": final_stats,
            "memory_pause_requested": bool(memory_state["pause_requested"]),
            "memory_pause_reason": memory_state["reason"],
            "memory_rss_mb": memory_state["rss_mb"],
            "memory_hard_limit_mb": memory_state["hard_limit_mb"],
            "memory_container_limit_mb": memory_state["container_limit_mb"],
        }
    finally:
        _clear_watchdog()
        conn.close()


if __name__ == "__main__":
    main()
