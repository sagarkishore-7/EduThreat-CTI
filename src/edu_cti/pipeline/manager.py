"""
Pipeline Manager - Background execution engine for admin dashboard control.

Runs pipeline phases (ingest, enrich, historical, daily) in background threads
with real-time log capture, progress tracking, and run history.

Also manages a built-in scheduler for continuous real-time intelligence collection.
"""

import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger(__name__)


def _load_enrichment_stats() -> Dict[str, int]:
    """Read the latest enrichment counters from the database."""
    from src.edu_cti.core.db import get_connection, init_db
    from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_stats

    conn = get_connection()
    try:
        init_db(conn)
        return get_enrichment_stats(conn)
    finally:
        conn.close()


def _persist_run(run: "PipelineRun") -> None:
    """Persist pipeline run state to DB (fire-and-forget)."""
    try:
        from src.edu_cti.core.db import get_connection, init_db
        conn = get_connection()
        init_db(conn)
        conn.execute(
            """INSERT OR REPLACE INTO pipeline_runs
               (run_id, phase, status, params, started_at, finished_at,
                duration_seconds, result, error, progress_step, progress_detail, progress_percent)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run.run_id,
                run.phase,
                run.status.value,
                json.dumps(run.params) if run.params else None,
                run.started_at,
                run.finished_at,
                run.duration_seconds,
                json.dumps(run.result) if run.result else None,
                run.error,
                run.progress.get("step", ""),
                run.progress.get("detail", ""),
                run.progress.get("percent", 0),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Failed to persist run {run.run_id}: {e}")


def _recover_interrupted_runs() -> List[Dict]:
    """On startup, mark any 'running' pipeline_runs as 'interrupted' and return them."""
    recovered = []
    try:
        from src.edu_cti.core.db import get_connection, init_db
        conn = get_connection()
        init_db(conn)
        rows = conn.execute(
            "SELECT run_id, phase, params, started_at FROM pipeline_runs WHERE status = 'running'"
        ).fetchall()
        now = datetime.utcnow().isoformat()
        for row in rows:
            conn.execute(
                """UPDATE pipeline_runs SET status = 'interrupted',
                   finished_at = ?, error = 'Container restarted while pipeline was running'
                   WHERE run_id = ?""",
                (now, row[0]),
            )
            recovered.append({
                "run_id": row[0],
                "phase": row[1],
                "params": json.loads(row[2]) if row[2] else {},
                "started_at": row[3],
            })
        if recovered:
            conn.commit()
            logger.warning(
                f"Recovered {len(recovered)} interrupted pipeline run(s) from previous container: "
                + ", ".join(r["run_id"] for r in recovered)
            )
        conn.close()
    except Exception as e:
        logger.debug(f"Pipeline run recovery check failed: {e}")
    return recovered


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"  # Container restarted while running
    PAUSED = "paused"  # Gracefully paused due to memory pressure or operator action


class PipelineRun:
    """Represents a single pipeline execution."""

    def __init__(self, run_id: str, phase: str, params: Dict[str, Any]):
        self.run_id = run_id
        self.phase = phase
        self.params = params
        self.status: RunStatus = RunStatus.PENDING
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.duration_seconds: Optional[float] = None
        self.result: Dict[str, Any] = {}
        self.error: Optional[str] = None
        self.progress: Dict[str, Any] = {"step": "", "detail": "", "percent": 0}
        self.logs: deque = deque(maxlen=5000)
        self._cancel_requested = False

    def to_dict(self, include_logs: bool = False) -> Dict[str, Any]:
        d = {
            "run_id": self.run_id,
            "phase": self.phase,
            "params": self.params,
            "status": self.status.value,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "result": self.result,
            "error": self.error,
            "progress": self.progress,
        }
        if include_logs:
            d["logs"] = list(self.logs)
        return d


class RunLogHandler(logging.Handler):
    """Logging handler that captures log records into a PipelineRun."""

    def __init__(self, run: PipelineRun):
        super().__init__()
        self.run = run
        self.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    def emit(self, record):
        try:
            msg = self.format(record)
            self.run.logs.append(msg)
        except Exception:
            pass


class PipelineManager:
    """
    Singleton manager for pipeline execution.

    - Only one pipeline phase runs at a time.
    - Keeps history of recent runs (last 50).
    - Provides log access for streaming to dashboard.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._current_run: Optional[PipelineRun] = None
        self._history: deque = deque(maxlen=50)
        self._thread: Optional[threading.Thread] = None
        self._run_lock = threading.Lock()

        # Recover interrupted runs from previous container lifecycle
        self._interrupted_runs = _recover_interrupted_runs()

        # Scheduler state
        self._scheduler_running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._scheduler_stop_event = threading.Event()
        self._scheduler_schedule = BackgroundScheduler(daemon=True)
        self._scheduler_started_at: Optional[str] = None
        self._scheduler_last_runs: Dict[str, Optional[str]] = {
            "rss": None,
            "api": None,
            "daily": None,
        }
        self._scheduler_total_new: int = 0
        self._auto_resume_interrupted_runs()

    @property
    def current_run(self) -> Optional[PipelineRun]:
        return self._current_run

    @property
    def is_running(self) -> bool:
        return self._current_run is not None and self._current_run.status == RunStatus.RUNNING

    def get_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        # In-memory runs (current session)
        runs = list(self._history)
        runs.reverse()
        in_memory = [r.to_dict() for r in runs[:limit]]
        in_memory_ids = {r["run_id"] for r in in_memory}

        # DB-persisted runs (includes interrupted runs from previous containers)
        try:
            from src.edu_cti.core.db import get_connection, init_db
            conn = get_connection(read_only=True)
            init_db(conn)
            rows = conn.execute(
                "SELECT run_id, phase, status, params, started_at, finished_at, "
                "duration_seconds, result, error, progress_step, progress_detail, progress_percent "
                "FROM pipeline_runs ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            conn.close()
            for row in rows:
                if row[0] in in_memory_ids:
                    continue
                in_memory.append({
                    "run_id": row[0],
                    "phase": row[1],
                    "status": row[2],
                    "params": json.loads(row[3]) if row[3] else {},
                    "started_at": row[4],
                    "finished_at": row[5],
                    "duration_seconds": row[6],
                    "result": json.loads(row[7]) if row[7] else {},
                    "error": row[8],
                    "progress": {
                        "step": row[9] or "",
                        "detail": row[10] or "",
                        "percent": row[11] or 0,
                    },
                })
        except Exception as e:
            logger.debug(f"Failed to load DB pipeline history: {e}")

        # Sort by started_at descending
        in_memory.sort(key=lambda r: r.get("started_at") or "", reverse=True)
        return in_memory[:limit]

    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        if self._current_run and self._current_run.run_id == run_id:
            return self._current_run
        for run in self._history:
            if run.run_id == run_id:
                return run
        return None

    def request_cancel(self) -> bool:
        if self._current_run and self._current_run.status == RunStatus.RUNNING:
            self._current_run._cancel_requested = True
            # Signal Phase 2 enrichment cancel event (if running)
            try:
                from src.edu_cti.pipeline.phase2.__main__ import _cancel_event
                _cancel_event.set()
                logger.info(f"Cancel requested for run {self._current_run.run_id} (phase2 cancel event set)")
            except ImportError:
                pass
            # Signal Phase 1 news scraping cancel event (if running)
            try:
                from src.edu_cti.sources.news.common import _cancel_event as news_cancel
                news_cancel.set()
                logger.info(f"Cancel requested for run {self._current_run.run_id} (news scraping cancel event set)")
            except ImportError:
                logger.info(f"Cancel requested for run {self._current_run.run_id}")
            return True
        return False

    def start_phase(self, phase: str, params: Optional[Dict[str, Any]] = None) -> PipelineRun:
        """
        Start a pipeline phase in a background thread.

        Phases: ingest, enrich, historical, daily, ingest_source, rss, weekly
        """
        with self._run_lock:
            if self.is_running:
                raise RuntimeError(
                    f"Pipeline already running: {self._current_run.phase} "
                    f"(run_id={self._current_run.run_id})"
                )

            run_id = str(uuid.uuid4())[:12]
            run = PipelineRun(run_id, phase, params or {})
            self._current_run = run

            try:
                from src.edu_cti.api.cache import cache_invalidate

                cache_invalidate()
            except Exception:
                pass

            self._thread = threading.Thread(
                target=self._execute_run,
                args=(run,),
                daemon=True,
                name=f"pipeline-{phase}-{run_id}",
            )
            self._thread.start()
            return run

    def _execute_run(self, run: PipelineRun):
        """Execute a pipeline run in background thread."""
        run.status = RunStatus.RUNNING
        run.started_at = datetime.utcnow().isoformat()
        _persist_run(run)  # Mark as running in DB

        # Clear cancel events from previous runs
        try:
            from src.edu_cti.sources.news.common import _cancel_event as news_cancel
            news_cancel.clear()
        except ImportError:
            pass

        # Attach log handler
        log_handler = RunLogHandler(run)
        log_handler.setLevel(logging.DEBUG)
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)

        start_time = time.time()
        try:
            result = self._dispatch_phase(run)
            run.result = result or {}
            if run.result.get("memory_pause_requested"):
                run.status = RunStatus.PAUSED
                run.error = run.result.get("memory_pause_reason")
            else:
                run.status = RunStatus.CANCELLED if run._cancel_requested else RunStatus.COMPLETED
        except Exception as e:
            run.status = RunStatus.FAILED
            run.error = str(e)
            logger.error(f"Pipeline run {run.run_id} failed: {e}", exc_info=True)
        finally:
            run.finished_at = datetime.utcnow().isoformat()
            run.duration_seconds = round(time.time() - start_time, 2)
            root_logger.removeHandler(log_handler)
            self._history.append(run)
            _persist_run(run)  # Persist final state
            try:
                from src.edu_cti.api.cache import cache_invalidate

                cache_invalidate()
            except Exception:
                pass
            # Close the default HTTP client to free Playwright/Chromium memory
            try:
                from src.edu_cti.core.http import _default_client
                if _default_client is not None:
                    _default_client.close()
            except Exception:
                pass

    def _dispatch_phase(self, run: PipelineRun) -> Dict[str, Any]:
        """Route to the correct pipeline phase handler."""
        phase = run.phase
        params = run.params

        if phase == "ingest":
            return self._run_ingest(run, params)
        elif phase == "enrich":
            return self._run_enrich(run, params)
        elif phase == "historical":
            return self._run_historical(run, params)
        elif phase == "daily":
            return self._run_daily(run, params)
        elif phase == "ingest_source":
            return self._run_ingest_source(run, params)
        elif phase == "rss":
            return self._run_rss(run, params)
        elif phase == "weekly":
            return self._run_weekly(run, params)
        else:
            raise ValueError(f"Unknown pipeline phase: {phase}")

    def _auto_resume_interrupted_runs(self) -> None:
        """Auto-resume the newest interrupted long-running pipeline on Railway."""
        from src.edu_cti.core.config import AUTO_RESUME_INTERRUPTED_PIPELINES

        if not AUTO_RESUME_INTERRUPTED_PIPELINES or not self._interrupted_runs:
            return

        resumable_phases = {"historical", "daily", "enrich"}
        candidates = [
            run for run in self._interrupted_runs
            if run.get("phase") in resumable_phases
        ]
        if not candidates:
            return

        latest = max(candidates, key=lambda r: r.get("started_at") or "")
        params = dict(latest.get("params") or {})
        params["auto_resumed"] = True
        params["resumed_from_run_id"] = latest["run_id"]

        try:
            resumed = self.start_phase(latest["phase"], params)
            logger.warning(
                "Auto-resumed interrupted %s pipeline %s as new run %s",
                latest["phase"],
                latest["run_id"],
                resumed.run_id,
            )
        except Exception as e:
            logger.warning(
                "Failed to auto-resume interrupted pipeline %s (%s): %s",
                latest["run_id"],
                latest["phase"],
                e,
            )

    # ------------------------------------------------------------------
    # Phase implementations
    # ------------------------------------------------------------------

    @staticmethod
    def _scale_percent(raw_percent: float, pmin: int, pmax: int) -> int:
        """Map a 0-100 raw percent into the [pmin, pmax] range."""
        return pmin + int((raw_percent / 100.0) * (pmax - pmin))

    def _run_ingest(self, run: PipelineRun, params: Dict,
                    progress_range: tuple = (0, 100), step_prefix: str = "") -> Dict:
        """Run Phase 1 ingestion (all groups).

        progress_range: (min_percent, max_percent) to map internal 0-100 into.
        step_prefix: optional prefix for step text (e.g. "Phase 1: ").
        """
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase1.__main__ import GROUP_COLLECTORS, _ingest_group

        pmin, pmax = progress_range
        full_historical = params.get("full_historical", False)
        groups = params.get("groups", ["curated", "news", "rss", "api"])
        sources = params.get("sources")
        max_pages = params.get("max_pages")
        rss_max_age_days = params.get("rss_max_age_days", 30)
        include_paid_rss = params.get("include_paid_rss", False)

        conn = get_connection()
        init_db(conn)

        total_new = 0
        for i, group in enumerate(groups):
            if run._cancel_requested:
                logger.info("Ingestion cancelled by user")
                break

            raw_pct = (i / len(groups)) * 100
            run.progress = {
                "step": f"{step_prefix}Ingesting {group}",
                "detail": f"Group {i+1}/{len(groups)} — collecting from sources…",
                "percent": self._scale_percent(raw_pct, pmin, pmax),
            }

            label, collector = GROUP_COLLECTORS[group]
            is_rss = group == "rss"
            kwargs = {
                "sources": sources if group in ("curated", "news", "rss") else None,
                "max_pages": max_pages if not is_rss else None,
                "max_age_days": rss_max_age_days if is_rss else None,
                "is_rss": is_rss,
                "incremental": not full_historical,
                "include_paid_rss": include_paid_rss if is_rss else False,
            }
            try:
                count = _ingest_group(conn, label, collector, **kwargs)
                total_new += count
                # Update detail after group finishes
                run.progress = {
                    "step": f"{step_prefix}Ingested {group}",
                    "detail": f"{count} new incidents from {label}",
                    "percent": self._scale_percent(((i + 1) / len(groups)) * 100, pmin, pmax),
                }
                logger.info(f"{label}: {count} new incidents")
            except Exception as e:
                logger.error(f"Error ingesting {label}: {e}", exc_info=True)

        conn.close()
        run.progress = {"step": f"{step_prefix}Ingestion complete", "detail": f"{total_new} total new incidents", "percent": pmax}
        return {"new_incidents": total_new, "groups": groups, "full_historical": full_historical}

    def _run_enrich(self, run: PipelineRun, params: Dict,
                    progress_range: tuple = (0, 100), step_prefix: str = "") -> Dict:
        """Run Phase 2 LLM enrichment.

        Standalone-enrich entry point: keeps invoking _run_one_enrich_batch
        until ready_for_enrichment hits 0 or the run is cancelled. Each batch
        is capped by PHASE2_RUN_LIMIT (default 1000) so memory stays bounded;
        looping here means a single 'enrich' admin click drains the whole
        backlog without manual re-clicks.

        Historical/daily phases call _run_one_enrich_batch directly because
        they already have their own outer loop that interleaves with ingest.

        If params explicitly sets a `limit`, we honour it and run exactly
        one batch (caller wants a bounded run).
        """
        explicit_limit = params.get("limit")
        if explicit_limit:
            return self._run_one_enrich_batch(run, params, progress_range, step_prefix)

        total_enriched = 0
        rounds = 0
        last_result: Dict = {}

        while not run._cancel_requested:
            stats = _load_enrichment_stats()
            ready = stats.get("ready_for_enrichment", 0)
            if ready == 0:
                logger.info(
                    "Enrich phase complete: 0 actionable incidents remain "
                    "(enriched %d across %d round(s))",
                    total_enriched, rounds,
                )
                break

            rounds += 1
            logger.info(
                "Enrich phase round %d starting — %d actionable incidents available",
                rounds, ready,
            )
            result = self._run_one_enrich_batch(run, params, progress_range, step_prefix)
            last_result = result if isinstance(result, dict) else {}

            if isinstance(result, dict):
                total_enriched += result.get("run_stats", {}).get("enriched", 0)
                if result.get("memory_pause_requested"):
                    logger.warning(
                        "Enrich phase paused due to memory pressure after round %d; "
                        "stopping loop. Reason: %s",
                        rounds, result.get("memory_pause_reason"),
                    )
                    break

            # Aggressive inter-round cleanup. Each phase2.main() call leaves
            # behind references in module-level state (queue objects, watchdog
            # closures, thread-local caches). Without this, round 2's startup
            # can OOM 7 seconds in despite round 1 running an hour cleanly —
            # the process-global PyTorch allocator and HF cache symlinks
            # accumulate fragmentation across calls.
            #
            # The 30-second pause lets the kernel actually reclaim cgroup pages
            # (mmap'd files, freed allocator chunks). 2s wasn't enough — the
            # OS lazy-flushes page cache and 30s gives it real time to release
            # memory back to the cgroup before round N+1's startup spike.
            try:
                import gc as _gc
                _gc.collect()
                try:
                    import ctypes as _ct
                    _ct.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
                except Exception:
                    pass
                import time as _time
                _pause_seconds = int(os.environ.get("PHASE2_INTER_ROUND_PAUSE_SECONDS", "30"))
                logger.info(
                    "[ENRICH-LOOP] Round %d done; pausing %ds for kernel reclaim before next round",
                    rounds, _pause_seconds,
                )
                _time.sleep(_pause_seconds)
                # Run gc + trim once more after the pause to catch anything
                # released asynchronously during the wait.
                _gc.collect()
                try:
                    import ctypes as _ct2
                    _ct2.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
                except Exception:
                    pass
                logger.info("[ENRICH-LOOP] Inter-round cleanup complete; starting next round")
            except Exception as _cleanup_err:
                logger.debug("Inter-round cleanup error (non-fatal): %s", _cleanup_err)

        # Surface aggregate counts
        if last_result:
            last_result["enrich_rounds"] = rounds
            last_result["enrich_total_enriched"] = total_enriched
        return last_result

    def _run_one_enrich_batch(self, run: PipelineRun, params: Dict,
                              progress_range: tuple = (0, 100), step_prefix: str = "") -> Dict:
        """Run exactly one Phase 2 batch in a SUBPROCESS.

        Why subprocess instead of in-thread:
        - Each batch starts with a clean Python heap, no allocator
          fragmentation from prior batches, no leaked daemon threads.
        - When phase2 OOMs, ONLY the subprocess dies — the API server
          and pipeline manager survive. The auto-loop catches the
          non-zero exit code and proceeds to the next round.
        - Models redownload is bounded (HF cache survives on the
          persistent volume; only Python heap is fresh).

        progress_range: (min_percent, max_percent) to map internal 0-100 into.
        step_prefix: optional prefix for step text (e.g. "Phase 2: ").
        """
        import sys
        import subprocess
        import re

        pmin, pmax = progress_range
        limit = params.get("limit")
        rate_limit_delay = params.get("rate_limit_delay", 2.0)
        export_csv = params.get("export_csv", False)
        from src.edu_cti.core.config import ENRICHMENT_WORKERS
        workers = params.get("workers", ENRICHMENT_WORKERS)

        run.progress = {"step": f"{step_prefix}Starting enrichment", "detail": "", "percent": pmin}

        # Snapshot DB state BEFORE the subprocess so we can compute the
        # delta after it exits (gives us run_stats.enriched count without
        # an IPC channel back from the subprocess).
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase2.storage.db import get_enrichment_stats
        try:
            _conn_before = get_connection()
            init_db(_conn_before)
            stats_before = get_enrichment_stats(_conn_before)
            _conn_before.close()
        except Exception:
            stats_before = {"enriched_incidents": 0}

        # Build argv for phase2's argparse
        cmd = [sys.executable, "-m", "src.edu_cti.pipeline.phase2"]
        if limit:
            cmd.extend(["--limit", str(limit)])
        if rate_limit_delay:
            cmd.extend(["--rate-limit-delay", str(rate_limit_delay)])
        if workers and workers > 1:
            cmd.extend(["--workers", str(workers)])
        if export_csv:
            cmd.append("--export-csv")
        cmd.extend(["--log-level", "INFO"])

        logger.info("Launching phase2 subprocess: %s", " ".join(cmd[1:]))
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered
            env=os.environ.copy(),
        )

        # Stream subprocess stdout/stderr through our logger, and watch for
        # progress markers we can map back into run.progress.
        memory_pause: Dict[str, Any] = {"requested": False, "reason": None}
        _enrich_re = re.compile(r"Enriching \[(\d+)/(\d+) this run")
        _fetch_re = re.compile(r"Fetching \[(\d+)/(\d+)\]")
        _phase2_logger = logging.getLogger("phase2")

        def _stream_logs():
            try:
                assert proc.stdout is not None
                for line in proc.stdout:
                    line = line.rstrip()
                    if not line:
                        continue
                    _phase2_logger.info(line)
                    # Map "Enriching [X/Y this run | A/B enriched in DB]" → percent
                    m_enr = _enrich_re.search(line)
                    if m_enr:
                        cur, tot = int(m_enr.group(1)), int(m_enr.group(2))
                        # Enrichment phase maps to 30-100% of the overall progress
                        pct = 30 + int((cur / max(tot, 1)) * 70)
                        run.progress = {
                            "step": f"{step_prefix}Enriching",
                            "detail": f"{cur}/{tot}",
                            "percent": self._scale_percent(pct, pmin, pmax),
                        }
                        continue
                    m_fet = _fetch_re.search(line)
                    if m_fet:
                        cur, tot = int(m_fet.group(1)), int(m_fet.group(2))
                        # Fetching phase maps to 0-30% of the overall progress
                        pct = int((cur / max(tot, 1)) * 30)
                        run.progress = {
                            "step": f"{step_prefix}Fetching",
                            "detail": f"{cur}/{tot}",
                            "percent": self._scale_percent(pct, pmin, pmax),
                        }
                        continue
                    if "memory pause" in line.lower() or "Paused for memory pressure" in line:
                        memory_pause["requested"] = True
                        memory_pause["reason"] = line
            except Exception as exc:
                logger.warning("Subprocess log stream error: %s", exc)

        _log_thread = threading.Thread(target=_stream_logs, daemon=True, name="phase2-log-stream")
        _log_thread.start()

        # Wait for subprocess; check cancel every 2s.
        try:
            while proc.poll() is None:
                if run._cancel_requested:
                    logger.info(
                        "Cancel requested — sending SIGTERM to phase2 subprocess (PID %d)",
                        proc.pid,
                    )
                    proc.terminate()
                    try:
                        proc.wait(timeout=60)
                    except subprocess.TimeoutExpired:
                        logger.warning("phase2 subprocess did not exit in 60s; sending SIGKILL")
                        proc.kill()
                        proc.wait(timeout=10)
                    break
                time.sleep(2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
            raise
        finally:
            _log_thread.join(timeout=10)

        return_code = proc.returncode
        logger.info("phase2 subprocess exited with code %d", return_code)

        # Compute run_stats from DB delta. This replaces the in-memory
        # counters that the previous in-thread version returned.
        try:
            _conn_after = get_connection()
            init_db(_conn_after)
            stats_after = get_enrichment_stats(_conn_after)
            _conn_after.close()
        except Exception:
            stats_after = stats_before
        enriched_delta = max(
            0,
            int(stats_after.get("enriched_incidents", 0)) - int(stats_before.get("enriched_incidents", 0)),
        )

        run.progress = {
            "step": f"{step_prefix}Enrichment complete",
            "detail": f"+{enriched_delta} enriched this round",
            "percent": pmax,
        }

        result: Dict[str, Any] = {
            "enrichment_stats": stats_after,
            "run_stats": {"enriched": enriched_delta, "subprocess_exit_code": return_code},
        }
        if memory_pause["requested"]:
            result["memory_pause_requested"] = True
            result["memory_pause_reason"] = memory_pause["reason"]
        # Non-zero exit (signal kill, OOM) — surface so the auto-loop can decide
        # whether to retry or pause. Treat OOM-kill (137 / -9) as memory pause.
        if return_code not in (0, None):
            if return_code in (137, -9, -15):
                logger.warning(
                    "phase2 subprocess killed by signal (exit=%d) — likely OOM; treating as memory pause",
                    return_code,
                )
                result["memory_pause_requested"] = True
                result["memory_pause_reason"] = result.get("memory_pause_reason") or f"subprocess killed (exit={return_code})"
            else:
                result["error"] = f"phase2 subprocess exit code {return_code}"
        return result

    def _run_historical(self, run: PipelineRun, params: Dict) -> Dict:
        """Run full historical pipeline (ingest + enrich in parallel).

        Ingestion and enrichment run concurrently:
        - Phase 1 (ingestion) saves incidents to DB incrementally
        - Phase 2 (enrichment) picks up unenriched incidents as they appear
        - Enrichment loops until ingestion is done AND no actionable incidents remain
        """
        skip_enrich = params.get("skip_enrich", False)
        enrich_limit = params.get("enrich_limit")
        export_csv = params.get("export_csv", False)
        max_pages = params.get("max_pages", 50)  # Default 50 pages per search term (1000 articles)
        from src.edu_cti.core.config import ENABLE_OXYLABS_NEWS_HISTORICAL, ENRICHMENT_WORKERS

        ingest_params = {
            "full_historical": True,
            "groups": ["curated", "news", "rss", "api"],
            "rss_max_age_days": 365,
            "max_pages": max_pages,
            "include_paid_rss": params.get("include_paid_rss", ENABLE_OXYLABS_NEWS_HISTORICAL),
        }
        current_workers = max(1, int(params.get("workers", ENRICHMENT_WORKERS)))

        if skip_enrich:
            # Sequential: just ingest
            ingest_result = self._run_ingest(
                run, ingest_params,
                progress_range=(0, 100), step_prefix="Ingestion: ",
            )
            run.progress = {"step": "Complete", "detail": "", "percent": 100}
            return {"ingest": ingest_result, "enrich": None}

        # --- Parallel: ingest + enrich simultaneously ---
        ingest_result_box = [None]
        ingest_error_box = [None]
        ingest_done = threading.Event()

        def _ingest_worker():
            try:
                ingest_result_box[0] = self._run_ingest(
                    run, ingest_params,
                    progress_range=(0, 30), step_prefix="Ingesting: ",
                )
            except Exception as e:
                ingest_error_box[0] = e
                logger.error(f"Ingestion failed: {e}", exc_info=True)
            finally:
                ingest_done.set()

        ingest_thread = threading.Thread(
            target=_ingest_worker, daemon=True, name="historical-ingest",
        )
        ingest_thread.start()
        logger.info("Historical pipeline: ingestion started in background, enrichment will start in parallel")

        # Wait briefly for first batch of incidents to land in DB
        ingest_done.wait(timeout=30)

        # Run enrichment in a loop: keep enriching as new incidents arrive from ingestion
        enrich_result = None
        total_enriched = 0
        enrich_rounds = 0
        memory_pause_requested = False
        memory_pause_reason = None

        while True:
            if run._cancel_requested:
                break

            # Check how many actionable incidents are available.
            # Use ready_for_enrichment instead of raw unenriched count so we do not
            # spin forever on checkpointed/excluded/non-actionable rows.
            stats = _load_enrichment_stats()
            unenriched = stats.get("unenriched_incidents", 0)
            ready = stats.get("ready_for_enrichment", 0)

            if ready == 0:
                if ingest_done.is_set():
                    if unenriched == 0:
                        logger.info(
                            f"Historical pipeline: ingestion done, no actionable incidents remain "
                            f"(enriched {total_enriched} across {enrich_rounds} rounds)"
                        )
                    else:
                        logger.warning(
                            f"Historical pipeline: ingestion done with {unenriched} unenriched incident(s), "
                            "but none are actionable right now; stopping to avoid a busy loop"
                        )
                    break
                else:
                    # Ingestion still running but nothing actionable yet — wait
                    run.progress = {
                        "step": "Waiting for incidents",
                        "detail": (
                            "Ingestion in progress, waiting for incidents that are ready "
                            "for article fetch or enrichment..."
                        ),
                        "percent": self._scale_percent(0, 30, 100),
                    }
                    logger.info(
                        f"Historical pipeline: waiting for actionable incidents "
                        f"({unenriched} unenriched, {ready} ready)"
                    )
                    time.sleep(30)
                    continue

            # Run one enrichment cycle on whatever is available
            enrich_rounds += 1
            logger.info(
                f"Historical pipeline: enrichment round {enrich_rounds} — "
                f"{ready} actionable / {unenriched} unenriched incidents available"
            )
            # Historical's outer loop already iterates rounds — call the
            # single-batch helper directly so we don't double-loop.
            enrich_result = self._run_one_enrich_batch(run, {
                "limit": enrich_limit,
                "rate_limit_delay": 2.0,
                "export_csv": False,  # Only export on final round
                "workers": current_workers,
            }, progress_range=(30, 100), step_prefix="Enriching: ")

            if enrich_result and isinstance(enrich_result, dict):
                total_enriched += enrich_result.get("run_stats", {}).get("enriched", 0)
                if enrich_result.get("memory_pause_requested"):
                    if current_workers > 1:
                        next_workers = current_workers - 1
                        logger.warning(
                            "Historical pipeline hit memory pressure with %s worker(s); reducing to %s and resuming from checkpoints",
                            current_workers,
                            next_workers,
                        )
                        current_workers = next_workers
                        time.sleep(5)
                        continue
                    memory_pause_requested = True
                    memory_pause_reason = enrich_result.get("memory_pause_reason")
                    run._cancel_requested = True
                    try:
                        from src.edu_cti.sources.news.common import _cancel_event as news_cancel
                        news_cancel.set()
                    except ImportError:
                        pass
                    break

            # If ingestion is still running, loop to pick up new incidents
            if not ingest_done.is_set():
                logger.info("Ingestion still running — will check for more actionable incidents...")
                time.sleep(5)
                continue
            # Ingestion done — do one final check for stragglers
            else:
                final_stats = _load_enrichment_stats()
                if final_stats.get("ready_for_enrichment", 0) == 0:
                    break
                # Still some unenriched — loop again
                logger.info(
                    f"Ingestion done but {final_stats['ready_for_enrichment']} actionable "
                    f"incident(s) remain ({final_stats['unenriched_incidents']} unenriched total) — "
                    "running another enrichment round"
                )

        # Wait for ingestion thread to finish (should already be done)
        ingest_thread.join(timeout=10)

        if ingest_error_box[0]:
            logger.error(f"Ingestion had an error: {ingest_error_box[0]}")

        # Export is opt-in so routine historical runs do not write CSVs implicitly.
        if export_csv and not run._cancel_requested and not memory_pause_requested:
            try:
                from src.edu_cti.pipeline.phase2.csv_export import export_enriched_dataset
                export_enriched_dataset()
                logger.info("Exported enriched dataset to CSV")
            except Exception as e:
                logger.warning(f"CSV export failed: {e}")

        if memory_pause_requested:
            run.progress = {
                "step": "Paused for memory pressure",
                "detail": memory_pause_reason or "Historical pipeline paused gracefully",
                "percent": max(run.progress.get("percent", 0), 30),
            }
        else:
            run.progress = {"step": "Complete", "detail": f"Enriched {total_enriched} incidents across {enrich_rounds} rounds", "percent": 100}
        return {
            "ingest": ingest_result_box[0],
            "enrich": enrich_result,
            "total_enriched": total_enriched,
            "enrich_rounds": enrich_rounds,
            "cancelled": run._cancel_requested,
            "memory_pause_requested": memory_pause_requested,
            "memory_pause_reason": memory_pause_reason,
        }

    def _run_daily(self, run: PipelineRun, params: Dict) -> Dict:
        """Run daily incremental pipeline (ingest + enrich in parallel)."""
        skip_enrich = params.get("skip_enrich", False)
        enrich_limit = params.get("enrich_limit")
        export_csv = params.get("export_csv", False)
        from src.edu_cti.core.config import ENABLE_OXYLABS_NEWS_DAILY, ENRICHMENT_WORKERS

        ingest_params = {
            "full_historical": False,
            "groups": ["curated", "news", "rss", "api"],
            "rss_max_age_days": 7,
            "max_pages": params.get("max_pages", 20),
            "include_paid_rss": params.get("include_paid_rss", ENABLE_OXYLABS_NEWS_DAILY),
        }
        current_workers = max(1, int(params.get("workers", ENRICHMENT_WORKERS)))

        if skip_enrich:
            ingest_result = self._run_ingest(
                run, ingest_params,
                progress_range=(0, 100), step_prefix="Ingestion: ",
            )
            run.progress = {"step": "Complete", "detail": "", "percent": 100}
            return {"ingest": ingest_result, "enrich": None}

        # --- Parallel: ingest + enrich simultaneously ---
        ingest_result_box = [None]
        ingest_done = threading.Event()

        def _ingest_worker():
            try:
                ingest_result_box[0] = self._run_ingest(
                    run, ingest_params,
                    progress_range=(0, 30), step_prefix="Ingesting: ",
                )
            except Exception as e:
                logger.error(f"Daily ingestion failed: {e}", exc_info=True)
            finally:
                ingest_done.set()

        ingest_thread = threading.Thread(
            target=_ingest_worker, daemon=True, name="daily-ingest",
        )
        ingest_thread.start()

        # Wait briefly for first incidents to land
        ingest_done.wait(timeout=15)

        # Run enrichment — loop until ingestion done + no unenriched remain
        enrich_result = None
        total_enriched = 0
        enrich_rounds = 0
        memory_pause_requested = False
        memory_pause_reason = None

        while not run._cancel_requested:
            stats = _load_enrichment_stats()
            unenriched = stats.get("unenriched_incidents", 0)
            ready = stats.get("ready_for_enrichment", 0)

            if ready == 0:
                if ingest_done.is_set():
                    if unenriched > 0:
                        logger.warning(
                            f"Daily pipeline: ingestion done with {unenriched} unenriched incident(s), "
                            "but none are actionable right now; stopping to avoid a busy loop"
                        )
                    break
                run.progress = {
                    "step": "Waiting for incidents",
                    "detail": "Ingestion in progress, waiting for actionable incidents...",
                    "percent": self._scale_percent(0, 30, 100),
                }
                logger.info(
                    f"Daily pipeline: waiting for actionable incidents "
                    f"({unenriched} unenriched, {ready} ready)"
                )
                time.sleep(15)
                continue

            enrich_rounds += 1
            logger.info(
                f"Daily pipeline: enrichment round {enrich_rounds} — "
                f"{ready} actionable / {unenriched} unenriched"
            )
            # Daily's outer loop already iterates rounds — call the
            # single-batch helper directly so we don't double-loop.
            enrich_result = self._run_one_enrich_batch(run, {
                "limit": enrich_limit,
                "rate_limit_delay": 2.0,
                "export_csv": False,
                "workers": current_workers,
            }, progress_range=(30, 100), step_prefix="Enriching: ")

            if enrich_result and isinstance(enrich_result, dict):
                total_enriched += enrich_result.get("run_stats", {}).get("enriched", 0)
                if enrich_result.get("memory_pause_requested"):
                    if current_workers > 1:
                        next_workers = current_workers - 1
                        logger.warning(
                            "Daily pipeline hit memory pressure with %s worker(s); reducing to %s and resuming from checkpoints",
                            current_workers,
                            next_workers,
                        )
                        current_workers = next_workers
                        time.sleep(5)
                        continue
                    memory_pause_requested = True
                    memory_pause_reason = enrich_result.get("memory_pause_reason")
                    run._cancel_requested = True
                    try:
                        from src.edu_cti.sources.news.common import _cancel_event as news_cancel
                        news_cancel.set()
                    except ImportError:
                        pass
                    break

            if ingest_done.is_set():
                # Final check for stragglers
                final = _load_enrichment_stats()
                if final.get("ready_for_enrichment", 0) == 0:
                    break
            else:
                time.sleep(5)

        ingest_thread.join(timeout=10)

        # Export is opt-in so daily/scheduled runs stay lightweight by default.
        if export_csv and not run._cancel_requested and not memory_pause_requested:
            try:
                from src.edu_cti.pipeline.phase2.csv_export import export_enriched_dataset
                export_enriched_dataset()
            except Exception:
                pass

        if memory_pause_requested:
            run.progress = {
                "step": "Paused for memory pressure",
                "detail": memory_pause_reason or "Daily pipeline paused gracefully",
                "percent": max(run.progress.get("percent", 0), 30),
            }
        else:
            run.progress = {"step": "Complete", "detail": f"Enriched {total_enriched} incidents", "percent": 100}
        return {
            "ingest": ingest_result_box[0],
            "enrich": enrich_result,
            "total_enriched": total_enriched,
            "enrich_rounds": enrich_rounds,
            "memory_pause_requested": memory_pause_requested,
            "memory_pause_reason": memory_pause_reason,
        }

    def _run_ingest_source(self, run: PipelineRun, params: Dict) -> Dict:
        """Run ingestion for a specific source group."""
        group = params.get("group", "curated")
        sources = params.get("sources")
        max_pages = params.get("max_pages")
        full_historical = params.get("full_historical", False)

        return self._run_ingest(run, {
            "full_historical": full_historical,
            "groups": [group],
            "sources": sources,
            "max_pages": max_pages,
            "include_paid_rss": params.get("include_paid_rss", False),
        })

    def _run_rss(self, run: PipelineRun, params: Dict) -> Dict:
        """Run RSS feed ingestion only."""
        return self._run_ingest(run, {
            "full_historical": False,
            "groups": ["rss"],
            "rss_max_age_days": params.get("max_age_days", 7),
            "include_paid_rss": params.get("include_paid_rss", False),
        })

    def _run_weekly(self, run: PipelineRun, params: Dict) -> Dict:
        """Run weekly full ingestion (curated + news)."""
        return self._run_ingest(run, {
            "full_historical": False,
            "groups": ["curated", "news"],
            "max_pages": params.get("max_pages", 20),  # Default 20 pages for weekly
        })


    # ------------------------------------------------------------------
    # Scheduler — continuous real-time intelligence pipeline
    # ------------------------------------------------------------------

    @property
    def scheduler_running(self) -> bool:
        return self._scheduler_running

    def start_scheduler(
        self,
        rss_interval_hours: int = 1,
        api_interval_hours: int = 6,
        enrich_interval_minutes: int = 30,
        daily_interval_hours: int = 24,
        catch_up: bool = True,
    ) -> Dict[str, Any]:
        """
        Start the real-time intelligence pipeline scheduler.

        Runs recurring jobs:
        - RSS feeds: every rss_interval_hours (default 1h) — ingest only
        - API sources: every api_interval_hours (default 6h) — ingest only
        - Enrichment: every enrich_interval_minutes (default 30min) — enrich any unenriched
        - Daily pipeline (all sources + enrich): every daily_interval_hours (default 24h)

        Enrichment runs frequently so new incidents from RSS/API are enriched
        within 30 minutes and appear on the dashboard in near real-time.
        """
        if self._scheduler_running:
            return {"status": "already_running", "started_at": self._scheduler_started_at}

        self._scheduler_running = True
        self._scheduler_stop_event.clear()
        self._scheduler_started_at = datetime.utcnow().isoformat()
        self._scheduler_total_new = 0

        # Reset last run timestamps
        for key in self._scheduler_last_runs:
            self._scheduler_last_runs[key] = None
        self._scheduler_last_runs["enrich"] = None

        # Clear any previous jobs and register new ones.
        # APScheduler manages its own thread pool — no manual loop needed.
        if self._scheduler_schedule.running:
            self._scheduler_schedule.remove_all_jobs()
        else:
            self._scheduler_schedule.start()

        self._scheduler_schedule.add_job(
            self._scheduler_run_job, "interval", hours=rss_interval_hours,
            args=["rss", {"max_age_days": 7}], id="rss_ingest", replace_existing=True,
        )
        self._scheduler_schedule.add_job(
            self._scheduler_run_job, "interval", hours=api_interval_hours,
            args=["ingest_source", {"group": "api"}], id="api_ingest", replace_existing=True,
        )
        self._scheduler_schedule.add_job(
            self._scheduler_run_enrich_if_needed, "interval", minutes=enrich_interval_minutes,
            id="enrichment", replace_existing=True,
        )
        self._scheduler_schedule.add_job(
            self._scheduler_run_job, "interval", hours=daily_interval_hours,
            args=["daily", {}], id="daily_pipeline", replace_existing=True,
        )
        # Data-quality sweep: find rows with bad dates / headline-as-institution
        # and queue them for re-enrichment (or flag for manual review).
        self._scheduler_schedule.add_job(
            self._scheduler_run_data_quality_sweep, "interval", hours=6,
            id="data_quality_sweep", replace_existing=True,
        )

        logger.info(
            f"[SCHEDULER] Started — RSS every {rss_interval_hours}h, "
            f"API every {api_interval_hours}h, Enrich every {enrich_interval_minutes}min, "
            f"Daily every {daily_interval_hours}h, Data-quality sweep every 6h"
        )

        # Run initial catch-up in yet another thread so start_scheduler returns immediately
        if catch_up:
            threading.Thread(
                target=self._scheduler_catchup,
                daemon=True,
                name="scheduler-catchup",
            ).start()

        return {
            "status": "started",
            "started_at": self._scheduler_started_at,
            "rss_interval_hours": rss_interval_hours,
            "api_interval_hours": api_interval_hours,
            "daily_interval_hours": daily_interval_hours,
        }

    def stop_scheduler(self) -> Dict[str, Any]:
        """Stop the scheduler. Does NOT cancel the currently running pipeline phase."""
        if not self._scheduler_running:
            return {"status": "not_running"}

        self._scheduler_running = False
        self._scheduler_stop_event.set()
        if self._scheduler_schedule.running:
            self._scheduler_schedule.remove_all_jobs()
            self._scheduler_schedule.shutdown(wait=False)

        logger.info("[SCHEDULER] Stopped")
        return {"status": "stopped"}

    def get_scheduler_status(self) -> Dict[str, Any]:
        """Return scheduler status for the admin API."""
        jobs = []
        for job in self._scheduler_schedule.get_jobs():
            jobs.append({
                "id": job.id,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            })

        return {
            "running": self._scheduler_running,
            "started_at": self._scheduler_started_at,
            "last_runs": dict(self._scheduler_last_runs),
            "total_new_incidents": self._scheduler_total_new,
            "jobs": jobs,
        }

    # --- internal helpers ---

    def _scheduler_catchup(self):
        """Run an initial catch-up: daily pipeline to ingest recent incidents."""
        logger.info("[SCHEDULER] Running initial catch-up cycle...")
        self._scheduler_run_job("daily", {})

    def _scheduler_run_data_quality_sweep(self):
        """
        Periodic sweep that finds enriched rows with bad dates or
        headline-as-institution and queues them for re-enrichment.

        Safe to run alongside any pipeline phase — it only updates flag
        columns, never the enrichment payload itself.
        """
        if not self._scheduler_running:
            return
        try:
            from src.edu_cti.pipeline.data_quality import sweep_invalid_data
            from src.edu_cti.core.db import get_connection
            conn = get_connection()
            try:
                result = sweep_invalid_data(conn)
            finally:
                conn.close()
            logger.info(
                "[SCHEDULER] data_quality_sweep complete: %s",
                result,
            )
        except Exception as exc:
            logger.warning("[SCHEDULER] data_quality_sweep failed: %s", exc, exc_info=True)

    def _scheduler_run_enrich_if_needed(self):
        """
        Check for unenriched incidents and run enrichment if any exist.

        This runs frequently (every 30 min) to ensure new incidents from
        RSS/API feeds are enriched quickly and appear on the dashboard.
        Skips if another pipeline is already running or no work to do.
        """
        if not self._scheduler_running:
            return
        if self.is_running:
            logger.debug("[SCHEDULER] Enrichment skipped — another pipeline is running")
            return

        # Check if there's work to do
        try:
            stats = _load_enrichment_stats()
            unenriched = stats.get("unenriched_incidents", 0)
            ready = stats.get("ready_for_enrichment", 0)
            if ready == 0:
                if unenriched > 0:
                    logger.debug(
                        f"[SCHEDULER] Enrichment skipped — {unenriched} unenriched incidents exist, "
                        "but none are actionable yet"
                    )
                else:
                    logger.debug("[SCHEDULER] Enrichment skipped — no unenriched incidents")
                return

            logger.info(
                f"[SCHEDULER] {ready} actionable incident(s) found "
                f"({unenriched} unenriched total) — starting enrichment"
            )
        except Exception as e:
            logger.warning(f"[SCHEDULER] Failed to check enrichment stats: {e}")
            return

        # Run enrichment
        self._scheduler_run_job("enrich", {
            "rate_limit_delay": 2.0,
            "export_csv": False,
        })
        self._scheduler_last_runs["enrich"] = datetime.utcnow().isoformat()

    def _scheduler_run_job(self, phase: str, params: Dict[str, Any]):
        """
        Execute a scheduled job. Waits if another pipeline is already running.
        """
        if not self._scheduler_running:
            return

        # Determine the job key for tracking
        if phase == "ingest_source" and params.get("group") == "api":
            job_key = "api"
        elif phase in self._scheduler_last_runs:
            job_key = phase
        else:
            job_key = phase

        logger.info(f"[SCHEDULER] Job triggered: {phase} (params={params})")

        # Wait for any running pipeline to finish (up to 30 min)
        wait_start = time.time()
        max_wait = 1800  # 30 minutes
        while self.is_running:
            if not self._scheduler_running:
                logger.info(f"[SCHEDULER] Scheduler stopped while waiting for {phase}")
                return
            if time.time() - wait_start > max_wait:
                logger.warning(f"[SCHEDULER] Timed out waiting for pipeline to finish, skipping {phase}")
                return
            time.sleep(10)

        # Start the phase
        try:
            run = self.start_phase(phase, params)
            logger.info(f"[SCHEDULER] Started {phase} (run_id={run.run_id})")

            # Wait for it to complete
            while self.is_running:
                if not self._scheduler_running:
                    break
                time.sleep(5)

            # Record results
            self._scheduler_last_runs[job_key] = datetime.utcnow().isoformat()
            if run.result and isinstance(run.result, dict):
                new_incidents = run.result.get("new_incidents", 0)
                if isinstance(run.result.get("ingest"), dict):
                    new_incidents = run.result["ingest"].get("new_incidents", 0)
                self._scheduler_total_new += new_incidents

            logger.info(f"[SCHEDULER] Completed {phase} — status={run.status.value}")

        except RuntimeError as e:
            logger.warning(f"[SCHEDULER] Could not start {phase}: {e}")
        except Exception as e:
            logger.error(f"[SCHEDULER] Error running {phase}: {e}", exc_info=True)


def get_pipeline_manager() -> PipelineManager:
    """Get the singleton PipelineManager instance."""
    return PipelineManager()
