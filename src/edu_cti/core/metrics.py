"""
Prometheus-style metrics for monitoring ingestion and enrichment.

Provides counters, gauges, and histograms for:
- Article fetch metrics per tier (newspaper3k, httpclient, oxylabs, archive_org)
- LLM extraction quality (attempts, timeouts, invalid JSON, confidence)
- Dataset completeness (field fill rates, completeness scores)
- Source novelty and deduplication quality
- Pipeline performance (throughput, queue depth, cost)
"""

import time
import logging
import statistics
from typing import Dict, List, Optional, Any
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


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
        # label registry: metric_base_name -> {label_key_set}
        self._label_registry: Dict[str, set] = defaultdict(set)

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
        seen_types = set()
        for key, values in sorted(self.histograms.items()):
            if not values:
                continue
            base = self._base_name(key)
            if base not in seen_types:
                lines.append(f"# TYPE {base} summary")
                seen_types.add(base)
            # Extract label portion for quantile lines
            label_part = key[len(base):]  # e.g. '{tier="newspaper3k"}'
            if label_part:
                # Insert quantile label into existing set
                inner = label_part[1:-1]  # strip { }
                q50_key = f'{base}{{{inner},quantile="0.5"}}'
                q95_key = f'{base}{{{inner},quantile="0.95"}}'
                q99_key = f'{base}{{{inner},quantile="0.99"}}'
            else:
                q50_key = f'{base}{{quantile="0.5"}}'
                q95_key = f'{base}{{quantile="0.95"}}'
                q99_key = f'{base}{{quantile="0.99"}}'

            lines.append(f"{q50_key} {_percentile(values, 50):.4f}")
            lines.append(f"{q95_key} {_percentile(values, 95):.4f}")
            lines.append(f"{q99_key} {_percentile(values, 99):.4f}")
            lines.append(f"{base}_count{label_part} {len(values)}")
            lines.append(f"{base}_sum{label_part} {sum(values):.4f}")
            lines.append(f"{base}_min{label_part} {min(values):.4f}")
            lines.append(f"{base}_max{label_part} {max(values):.4f}")

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
