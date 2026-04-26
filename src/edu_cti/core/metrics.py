"""
Prometheus-style metrics for monitoring ingestion and enrichment.

Provides counters, gauges, and histograms for:
- Article fetch metrics per tier (newspaper3k, httpclient, oxylabs, archive_org)
- LLM extraction quality (attempts, timeouts, invalid JSON, confidence)
- Dataset completeness (field fill rates, completeness scores)
- Source novelty and deduplication quality
- Pipeline performance (throughput, queue depth, cost)
"""

import atexit
import sqlite3
import threading
import time
import logging
import statistics
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

_METRICS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric_key    TEXT PRIMARY KEY,
    metric_type   TEXT NOT NULL CHECK(metric_type IN ('counter','gauge','histogram')),
    counter_value INTEGER DEFAULT 0,
    gauge_value   REAL,
    hist_sum      REAL DEFAULT 0.0,
    hist_count    INTEGER DEFAULT 0,
    hist_min      REAL,
    hist_max      REAL,
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
)
"""


def _percentile(values: List[float], p: float) -> float:
    """Return the p-th percentile (0–100) of values using linear interpolation."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    rank = (p / 100.0) * (n - 1)
    lo = int(rank)
    hi = lo + 1
    if hi >= n:
        return sorted_vals[-1]
    frac = rank - lo
    return sorted_vals[lo] + frac * (sorted_vals[hi] - sorted_vals[lo])


class MetricsCollector:
    """Prometheus-compatible metrics collector with research summary helpers."""

    def __init__(self):
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self.start_times: Dict[str, float] = {}
        self._label_registry: Dict[str, set] = defaultdict(set)

        # Persistence state (populated by configure())
        self._db_path: Optional[Path] = None
        self._db_lock = threading.Lock()
        # Per histogram key: cumulative totals from previous runs stored in DB,
        # *excluding* the current session's observations (which are in self.histograms).
        self._hist_baseline: Dict[str, Dict[str, float]] = {}
        self._flush_thread: Optional[threading.Thread] = None
        self._configured = False

    # ------------------------------------------------------------------
    # Persistence — configure / load / flush
    # ------------------------------------------------------------------

    def configure(self, db_path: Path, flush_interval_seconds: int = 60) -> None:
        """Connect this collector to a SQLite DB for cross-restart persistence.

        Must be called once at process startup (e.g. from the API lifespan).
        Subsequent calls are no-ops so double-initialisation is safe.
        """
        if self._configured:
            return
        self._db_path = Path(db_path)
        self._configured = True
        try:
            self._ensure_table()
            self._load_from_db()
        except Exception as exc:
            logger.error(f"Metrics: failed to load from DB ({exc}); starting fresh")

        # Background periodic flush
        def _flush_loop():
            while True:
                time.sleep(flush_interval_seconds)
                try:
                    self.flush_to_db()
                except Exception as exc:
                    logger.warning(f"Metrics background flush failed: {exc}")

        self._flush_thread = threading.Thread(target=_flush_loop, daemon=True, name="metrics-flush")
        self._flush_thread.start()

        # Best-effort flush on clean shutdown
        atexit.register(self._atexit_flush)
        logger.info(f"Metrics persistence configured — DB: {self._db_path}, flush every {flush_interval_seconds}s")

    def _atexit_flush(self) -> None:
        try:
            self.flush_to_db()
        except Exception as exc:
            logger.warning(f"Metrics atexit flush failed: {exc}")

    def _db_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self) -> None:
        with self._db_lock:
            conn = self._db_conn()
            try:
                conn.execute(_METRICS_TABLE_DDL)
                conn.commit()
            finally:
                conn.close()

    def _load_from_db(self) -> None:
        """Seed in-memory counters/gauges/histogram baselines from DB."""
        with self._db_lock:
            conn = self._db_conn()
            try:
                rows = conn.execute("SELECT * FROM pipeline_metrics").fetchall()
            finally:
                conn.close()

        for row in rows:
            key = row["metric_key"]
            mtype = row["metric_type"]
            if mtype == "counter":
                # Seed counter so new increments are additive on top of the DB total
                self.counters[key] = int(row["counter_value"] or 0)
            elif mtype == "gauge":
                # Don't restore gauges — they represent current state, stale values mislead
                pass
            elif mtype == "histogram":
                self._hist_baseline[key] = {
                    "sum": float(row["hist_sum"] or 0.0),
                    "count": int(row["hist_count"] or 0),
                    "min": row["hist_min"],
                    "max": row["hist_max"],
                }
        logger.info(f"Metrics: loaded {len(rows)} metric keys from DB")

    def flush_to_db(self) -> None:
        """Write current in-memory state to DB, merging with existing rows."""
        if not self._db_path:
            return

        now = datetime.utcnow().isoformat() + "Z"
        rows_to_upsert: List[tuple] = []

        with self._db_lock:
            # --- Counters ---
            for key, value in list(self.counters.items()):
                rows_to_upsert.append((key, "counter", int(value), None, None, None, None, None, now))

            # --- Gauges ---
            for key, value in list(self.gauges.items()):
                rows_to_upsert.append((key, "gauge", None, float(value), None, None, None, None, now))

            # --- Histograms: merge session observations with DB baseline ---
            all_hist_keys = set(self.histograms.keys()) | set(self._hist_baseline.keys())
            for key in all_hist_keys:
                session_vals = self.histograms.get(key, [])
                baseline = self._hist_baseline.get(key, {"sum": 0.0, "count": 0, "min": None, "max": None})

                total_sum = baseline["sum"] + sum(session_vals)
                total_count = baseline["count"] + len(session_vals)
                session_min = min(session_vals) if session_vals else None
                session_max = max(session_vals) if session_vals else None
                total_min = min(v for v in [baseline["min"], session_min] if v is not None) if (baseline["min"] is not None or session_min is not None) else None
                total_max = max(v for v in [baseline["max"], session_max] if v is not None) if (baseline["max"] is not None or session_max is not None) else None

                rows_to_upsert.append((key, "histogram", None, None, total_sum, total_count, total_min, total_max, now))
                # Update in-memory baseline so the next flush doesn't double-count
                self._hist_baseline[key] = {"sum": total_sum, "count": total_count, "min": total_min, "max": total_max}
                # Clear current session observations — they're now baked into the baseline
                self.histograms[key] = []

            conn = self._db_conn()
            try:
                conn.executemany(
                    """INSERT INTO pipeline_metrics
                       (metric_key, metric_type, counter_value, gauge_value,
                        hist_sum, hist_count, hist_min, hist_max, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(metric_key) DO UPDATE SET
                         metric_type   = excluded.metric_type,
                         counter_value = CASE WHEN excluded.metric_type='counter' THEN excluded.counter_value ELSE counter_value END,
                         gauge_value   = CASE WHEN excluded.metric_type='gauge'   THEN excluded.gauge_value   ELSE gauge_value   END,
                         hist_sum      = CASE WHEN excluded.metric_type='histogram' THEN excluded.hist_sum    ELSE hist_sum      END,
                         hist_count    = CASE WHEN excluded.metric_type='histogram' THEN excluded.hist_count  ELSE hist_count    END,
                         hist_min      = CASE WHEN excluded.metric_type='histogram' THEN excluded.hist_min    ELSE hist_min      END,
                         hist_max      = CASE WHEN excluded.metric_type='histogram' THEN excluded.hist_max    ELSE hist_max      END,
                         updated_at    = excluded.updated_at
                    """,
                    rows_to_upsert,
                )
                conn.commit()
            finally:
                conn.close()

    # ------------------------------------------------------------------
    # Core recording API
    # ------------------------------------------------------------------

    def increment(self, metric_name: str, value: int = 1, labels: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, labels)
        self.counters[key] += value
        if labels:
            self._label_registry[metric_name].add(frozenset(labels.keys()))

    def set_gauge(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, labels)
        self.gauges[key] = value
        if labels:
            self._label_registry[metric_name].add(frozenset(labels.keys()))

    def observe(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, labels)
        self.histograms[key].append(value)
        if labels:
            self._label_registry[metric_name].add(frozenset(labels.keys()))

    def start_timer(self, timer_key: str):
        """Start a named timer. timer_key should be unique per call site."""
        self.start_times[timer_key] = time.time()

    def stop_timer(self, timer_key: str) -> Optional[float]:
        """Stop a named timer and return elapsed seconds, or None if not started."""
        if timer_key in self.start_times:
            dur = time.time() - self.start_times.pop(timer_key)
            return dur
        return None

    # ------------------------------------------------------------------
    # Prometheus text format
    # ------------------------------------------------------------------

    def _make_key(self, metric_name: str, labels: Optional[Dict[str, str]]) -> str:
        if not labels:
            return metric_name
        # Prometheus requires quoted label values: key="value"
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{metric_name}{{{label_str}}}"

    def _base_name(self, key: str) -> str:
        return key.split("{")[0]

    def format_prometheus(self) -> str:
        lines = []
        lines.append("# EduThreat-CTI Prometheus Metrics")
        lines.append(f"# Generated at {datetime.utcnow().isoformat()}Z")
        lines.append("")

        # --- Counters ---
        seen_types: set = set()
        for key, value in sorted(self.counters.items()):
            base = self._base_name(key)
            if base not in seen_types:
                lines.append(f"# TYPE {base} counter")
                seen_types.add(base)
            lines.append(f"{key} {value}")

        lines.append("")

        # --- Gauges ---
        seen_types = set()
        for key, value in sorted(self.gauges.items()):
            base = self._base_name(key)
            if base not in seen_types:
                lines.append(f"# TYPE {base} gauge")
                seen_types.add(base)
            lines.append(f"{key} {value}")

        lines.append("")

        # --- Histograms (as summaries with percentiles) ---
        # _sum and _count are cumulative across all runs (session + DB baseline).
        # Percentile quantiles are from the current session only (in-memory observations).
        seen_types = set()
        all_hist_keys = set(self.histograms.keys()) | set(self._hist_baseline.keys())
        for key in sorted(all_hist_keys):
            values = self.histograms.get(key, [])
            baseline = self._hist_baseline.get(key, {"sum": 0.0, "count": 0, "min": None, "max": None})
            total_count = baseline["count"] + len(values)
            total_sum = baseline["sum"] + (sum(values) if values else 0.0)
            if total_count == 0:
                continue

            base = self._base_name(key)
            if base not in seen_types:
                lines.append(f"# TYPE {base} summary")
                seen_types.add(base)
            label_part = key[len(base):]
            if label_part:
                inner = label_part[1:-1]
                q50_key = f'{base}{{{inner},quantile="0.5"}}'
                q95_key = f'{base}{{{inner},quantile="0.95"}}'
                q99_key = f'{base}{{{inner},quantile="0.99"}}'
            else:
                q50_key = f'{base}{{quantile="0.5"}}'
                q95_key = f'{base}{{quantile="0.95"}}'
                q99_key = f'{base}{{quantile="0.99"}}'

            if values:
                lines.append(f"{q50_key} {_percentile(values, 50):.4f}")
                lines.append(f"{q95_key} {_percentile(values, 95):.4f}")
                lines.append(f"{q99_key} {_percentile(values, 99):.4f}")
            lines.append(f"{base}_count{label_part} {total_count}")
            lines.append(f"{base}_sum{label_part} {total_sum:.4f}")

            all_mins = [v for v in [baseline.get("min"), (min(values) if values else None)] if v is not None]
            all_maxs = [v for v in [baseline.get("max"), (max(values) if values else None)] if v is not None]
            if all_mins:
                lines.append(f"{base}_min{label_part} {min(all_mins):.4f}")
            if all_maxs:
                lines.append(f"{base}_max{label_part} {max(all_maxs):.4f}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Research summary helpers (for JSON API endpoints)
    # ------------------------------------------------------------------

    def fetch_stats_by_tier(self) -> Dict[str, Any]:
        """Return per-tier fetch statistics for /api/metrics/fetch-stats."""
        tiers = ["newspaper3k", "httpclient", "oxylabs", "archive_org"]
        result: Dict[str, Any] = {"by_tier": {}, "by_source": {}, "serp": {}, "rate_limiting": {}}

        for tier in tiers:
            attempts = sum(
                v for k, v in self.counters.items()
                if self._base_name(k) == "article_fetch_attempts_total"
                and f'tier="{tier}"' in k
            )
            successes = sum(
                v for k, v in self.counters.items()
                if self._base_name(k) == "article_fetch_success_total"
                and f'tier="{tier}"' in k
            )
            failures: Dict[str, int] = {}
            for k, v in self.counters.items():
                if self._base_name(k) == "article_fetch_failure_total" and f'tier="{tier}"' in k:
                    # Extract reason label value
                    import re
                    m = re.search(r'reason="([^"]+)"', k)
                    reason = m.group(1) if m else "unknown"
                    failures[reason] = failures.get(reason, 0) + v

            dur_key_prefix = f'article_fetch_duration_seconds{{tier="{tier}"'
            dur_vals = []
            for k, vs in self.histograms.items():
                if k.startswith(f'article_fetch_duration_seconds{{') and f'tier="{tier}"' in k:
                    dur_vals.extend(vs)

            content_vals = []
            for k, vs in self.histograms.items():
                if k.startswith("article_content_length_chars{") and f'tier="{tier}"' in k:
                    content_vals.extend(vs)

            result["by_tier"][tier] = {
                "attempts": attempts,
                "successes": successes,
                "failures": sum(failures.values()),
                "success_rate": round(successes / attempts, 4) if attempts else 0,
                "failure_breakdown": failures,
                "duration_s": {
                    "count": len(dur_vals),
                    "avg": round(sum(dur_vals) / len(dur_vals), 3) if dur_vals else None,
                    "p50": round(_percentile(dur_vals, 50), 3) if dur_vals else None,
                    "p95": round(_percentile(dur_vals, 95), 3) if dur_vals else None,
                    "p99": round(_percentile(dur_vals, 99), 3) if dur_vals else None,
                    "min": round(min(dur_vals), 3) if dur_vals else None,
                    "max": round(max(dur_vals), 3) if dur_vals else None,
                },
                "content_length_chars": {
                    "avg": round(sum(content_vals) / len(content_vals)) if content_vals else None,
                    "p50": round(_percentile(content_vals, 50)) if content_vals else None,
                    "p95": round(_percentile(content_vals, 95)) if content_vals else None,
                },
            }

        # SERP stats
        result["serp"] = {
            "queries": sum(v for k, v in self.counters.items() if self._base_name(k) == "serp_queries_total"),
            "urls_returned": sum(v for k, v in self.counters.items() if self._base_name(k) == "serp_urls_returned_total"),
            "zero_results": sum(v for k, v in self.counters.items() if self._base_name(k) == "serp_zero_results_total"),
        }

        # Rate limiting stats
        result["rate_limiting"] = {
            "delays": sum(v for k, v in self.counters.items() if self._base_name(k) == "domain_rate_limit_delays_total"),
            "perm_blocks": sum(v for k, v in self.counters.items() if self._base_name(k) == "domain_perm_blocked_total"),
        }

        # Top domains by attempts
        domain_attempts: Dict[str, int] = {}
        domain_successes: Dict[str, int] = {}
        for k, v in self.counters.items():
            import re
            if self._base_name(k) == "article_fetch_attempts_total":
                m = re.search(r'source="([^"]+)"', k)
                if m:
                    domain_attempts[m.group(1)] = domain_attempts.get(m.group(1), 0) + v
            elif self._base_name(k) == "article_fetch_success_total":
                m = re.search(r'source="([^"]+)"', k)
                if m:
                    domain_successes[m.group(1)] = domain_successes.get(m.group(1), 0) + v

        top_sources = sorted(
            [
                {
                    "domain": domain,
                    "attempts": attempts,
                    "successes": domain_successes.get(domain, 0),
                    "success_rate": round(domain_successes.get(domain, 0) / attempts, 4) if attempts else 0,
                }
                for domain, attempts in domain_attempts.items()
            ],
            key=lambda x: -x["attempts"],
        )[:20]
        result["top_sources"] = top_sources

        return result

    def research_summary(self) -> Dict[str, Any]:
        """Return paper-ready summary for /api/metrics/research-summary."""
        import re

        def _counter_total(base: str) -> int:
            return sum(v for k, v in self.counters.items() if self._base_name(k) == base)

        def _gauge_value(base: str) -> Optional[float]:
            for k, v in self.gauges.items():
                if self._base_name(k) == base:
                    return v
            return None

        def _histogram_summary(base: str) -> Dict[str, Any]:
            vals: List[float] = []
            for k, vs in self.histograms.items():
                if self._base_name(k) == base:
                    vals.extend(vs)
            if not vals:
                return {"count": 0}
            return {
                "count": len(vals),
                "avg": round(sum(vals) / len(vals), 4),
                "p50": round(_percentile(vals, 50), 4),
                "p95": round(_percentile(vals, 95), 4),
                "p99": round(_percentile(vals, 99), 4),
                "min": round(min(vals), 4),
                "max": round(max(vals), 4),
            }

        # Field fill rates
        field_fill_rates: Dict[str, float] = {}
        field_populated: Dict[str, int] = {}
        field_null: Dict[str, int] = {}
        for k, v in self.counters.items():
            if self._base_name(k) == "field_populated_total":
                m = re.search(r'field="([^"]+)"', k)
                if m:
                    field_populated[m.group(1)] = field_populated.get(m.group(1), 0) + v
            elif self._base_name(k) == "field_null_total":
                m = re.search(r'field="([^"]+)"', k)
                if m:
                    field_null[m.group(1)] = field_null.get(m.group(1), 0) + v
        for field, pop in field_populated.items():
            total = pop + field_null.get(field, 0)
            if total:
                field_fill_rates[field] = round(pop / total, 4)

        # Source novelty
        source_novel: Dict[str, int] = {}
        source_dup: Dict[str, int] = {}
        for k, v in self.counters.items():
            if self._base_name(k) == "source_novel_incident_total":
                m = re.search(r'source="([^"]+)"', k)
                if m:
                    source_novel[m.group(1)] = source_novel.get(m.group(1), 0) + v
            elif self._base_name(k) == "source_duplicate_total":
                m = re.search(r'source="([^"]+)"', k)
                if m:
                    source_dup[m.group(1)] = source_dup.get(m.group(1), 0) + v
        source_novelty = []
        all_sources = set(source_novel.keys()) | set(source_dup.keys())
        for src in sorted(all_sources):
            novel = source_novel.get(src, 0)
            dup = source_dup.get(src, 0)
            total = novel + dup
            source_novelty.append({
                "source": src,
                "novel": novel,
                "duplicates": dup,
                "total": total,
                "novel_rate": round(novel / total, 4) if total else 0,
            })

        return {
            "extraction_quality": {
                "llm_first_attempt_success_rate": _gauge_value("llm_first_attempt_success_rate"),
                "llm_timeout_total": _counter_total("llm_timeout_total"),
                "llm_invalid_json_total": _counter_total("llm_invalid_json_total"),
                "llm_edu_relevance_pass": _counter_total("llm_education_relevance_pass_total"),
                "llm_edu_relevance_fail": _counter_total("llm_education_relevance_fail_total"),
                "llm_duration_seconds": _histogram_summary("llm_duration_seconds"),
                "llm_confidence_score": _histogram_summary("llm_confidence_score"),
            },
            "dataset_completeness": {
                "incident_completeness_score": _histogram_summary("incident_completeness_score"),
                "field_fill_rates": dict(sorted(field_fill_rates.items(), key=lambda x: -x[1])),
            },
            "source_novelty": sorted(source_novelty, key=lambda x: -x["total"]),
            "deduplication": {
                "dedup_events_total": _counter_total("dedup_events_total"),
                "cross_source_agreement": {
                    field: v for k, v in self.counters.items()
                    if self._base_name(k) == "dedup_cross_source_agreement_total"
                    for field in [re.search(r'field="([^"]+)"', k).group(1)]
                    if re.search(r'field="([^"]+)"', k)
                },
                "field_gain_total": _counter_total("dedup_merge_field_gain_total"),
            },
            "fetch_performance": self.fetch_stats_by_tier(),
            "pipeline": {
                "throughput_per_hour": _gauge_value("pipeline_throughput_per_hour"),
                "queue_depth": _gauge_value("pipeline_queue_depth"),
                "memory_restarts": _counter_total("pipeline_memory_restart_total"),
                "cost_per_incident_usd": _histogram_summary("enrichment_cost_per_incident_usd"),
            },
        }

    def log_summary(self):
        """Log a human-readable summary of all metrics."""
        logger.info("=" * 70)
        logger.info("METRICS SUMMARY")
        logger.info("=" * 70)
        if self.counters:
            logger.info("Counters:")
            for k, v in sorted(self.counters.items()):
                logger.info(f"  {k}: {v}")
        if self.gauges:
            logger.info("Gauges:")
            for k, v in sorted(self.gauges.items()):
                logger.info(f"  {k}: {v}")
        if self.histograms:
            logger.info("Histograms:")
            for k, vals in sorted(self.histograms.items()):
                if vals:
                    logger.info(
                        f"  {k}: count={len(vals)} avg={sum(vals)/len(vals):.2f} "
                        f"p50={_percentile(vals,50):.2f} p95={_percentile(vals,95):.2f} "
                        f"min={min(vals):.2f} max={max(vals):.2f}"
                    )
        logger.info("=" * 70)

    def reset(self):
        self.counters.clear()
        self.gauges.clear()
        self.histograms.clear()
        self.start_times.clear()
        self._label_registry.clear()
        self._hist_baseline.clear()


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_metrics = MetricsCollector()


def get_metrics() -> MetricsCollector:
    return _metrics


# Convenience module-level functions

def increment(metric_name: str, value: int = 1, labels: Optional[Dict[str, str]] = None):
    _metrics.increment(metric_name, value, labels)


def set_gauge(metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
    _metrics.set_gauge(metric_name, value, labels)


def observe(metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
    _metrics.observe(metric_name, value, labels)


def start_timer(timer_key: str):
    _metrics.start_timer(timer_key)


def stop_timer(timer_key: str) -> Optional[float]:
    return _metrics.stop_timer(timer_key)
