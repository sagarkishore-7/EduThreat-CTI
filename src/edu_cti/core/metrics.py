"""
Prometheus-style metrics for monitoring ingestion and enrichment.

Provides counters, gauges, and histograms for:
- Ingestion metrics (sources, incidents, errors)
- Enrichment metrics (LLM calls, success/failure rates)
- Scheduler metrics (job runs, durations)
"""

import time
import logging
from typing import Dict, Optional
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Simple metrics collector with Prometheus-style output."""
    
    def __init__(self):
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, list] = defaultdict(list)
        self.start_times: Dict[str, float] = {}
        self.labels: Dict[str, Dict[str, str]] = {}
    
    def increment(self, metric_name: str, value: int = 1, labels: Optional[Dict[str, str]] = None):
        """Increment a counter metric."""
        key = self._make_key(metric_name, labels)
        self.counters[key] += value
        logger.info(f"[METRIC] {metric_name} += {value} (total: {self.counters[key]})")
    
    def set_gauge(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge metric."""
        key = self._make_key(metric_name, labels)
        self.gauges[key] = value
        logger.info(f"[METRIC] {metric_name} = {value}")
    
    def observe(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a histogram observation."""
        key = self._make_key(metric_name, labels)
        self.histograms[key].append(value)
        logger.info(f"[METRIC] {metric_name} observed: {value}")
    
    def start_timer(self, metric_name: str, labels: Optional[Dict[str, str]] = None):
        """Start a timer for duration measurement."""
        key = self._make_key(metric_name, labels)
        self.start_times[key] = time.time()
    
    def stop_timer(self, metric_name: str, labels: Optional[Dict[str, str]] = None):
        """Stop a timer and record duration."""
        key = self._make_key(metric_name, labels)
        if key in self.start_times:
            duration = time.time() - self.start_times[key]
            self.observe(f"{metric_name}_duration_seconds", duration, labels)
            del self.start_times[key]
            return duration
        return None
    
    def _make_key(self, metric_name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create a key from metric name and labels."""
        if labels:
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{metric_name}{{{label_str}}}"
        return metric_name
    
    def format_prometheus(self) -> str:
        """Format metrics in Prometheus text format."""
        lines = []
        lines.append("# EduThreat-CTI Metrics")
        lines.append(f"# Generated at {datetime.now().isoformat()}")
        lines.append("")
        
        # Counters
        for key, value in sorted(self.counters.items()):
            lines.append(f"# TYPE {key.split('{')[0]} counter")
            lines.append(f"{key} {value}")
        
        # Gauges
        for key, value in sorted(self.gauges.items()):
            lines.append(f"# TYPE {key.split('{')[0]} gauge")
            lines.append(f"{key} {value}")
        
        # Histograms (as summaries for simplicity)
        for key, values in sorted(self.histograms.items()):
            if values:
                lines.append(f"# TYPE {key.split('{')[0]} summary")
                lines.append(f"{key}_count {len(values)}")
                lines.append(f"{key}_sum {sum(values)}")
                lines.append(f"{key}_avg {sum(values) / len(values)}")
                lines.append(f"{key}_min {min(values)}")
                lines.append(f"{key}_max {max(values)}")
        
        return "\n".join(lines)
    
    def log_summary(self):
        """Log a summary of all metrics."""
        logger.info("="*70)
        logger.info("METRICS SUMMARY")
        logger.info("="*70)
        
        if self.counters:
            logger.info("\nCounters:")
            for key, value in sorted(self.counters.items()):
                logger.info(f"  {key}: {value}")
        
        if self.gauges:
            logger.info("\nGauges:")
            for key, value in sorted(self.gauges.items()):
                logger.info(f"  {key}: {value}")
        
        if self.histograms:
            logger.info("\nHistograms:")
            for key, values in sorted(self.histograms.items()):
                if values:
                    logger.info(f"  {key}:")
                    logger.info(f"    count: {len(values)}")
                    logger.info(f"    avg: {sum(values) / len(values):.2f}")
                    logger.info(f"    min: {min(values):.2f}")
                    logger.info(f"    max: {max(values):.2f}")
        
        logger.info("="*70)
    
    def reset(self):
        """Reset all metrics."""
        self.counters.clear()
        self.gauges.clear()
        self.histograms.clear()
        self.start_times.clear()
        self.labels.clear()


# Global metrics instance
_metrics = MetricsCollector()


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    return _metrics


# Convenience functions
def increment(metric_name: str, value: int = 1, labels: Optional[Dict[str, str]] = None):
    """Increment a counter."""
    _metrics.increment(metric_name, value, labels)


def set_gauge(metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
    """Set a gauge."""
    _metrics.set_gauge(metric_name, value, labels)


def observe(metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
    """Record a histogram observation."""
    _metrics.observe(metric_name, value, labels)


def start_timer(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """Start a timer."""
    _metrics.start_timer(metric_name, labels)


def stop_timer(metric_name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
    """Stop a timer and return duration."""
    return _metrics.stop_timer(metric_name, labels)
