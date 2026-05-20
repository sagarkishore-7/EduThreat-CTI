"""
Tests for Prometheus metrics collection, formatting, and pipeline instrumentation.

Covers:
- MetricsCollector core API (increment, observe, set_gauge, timers)
- Prometheus text format correctness (label quoting, TYPE dedup, percentiles)
- fetch_stats_by_tier() and research_summary() JSON helpers
- Article fetcher tier instrumentation (Scrapling-first chain + blocked domain)
- Enrichment LLM instrumentation (timeouts, invalid JSON, edu relevance, confidence)
- Deduplication metrics (events, cross-source agreement, field gain)
- SERP and rate-limit metrics
- _emit_enrichment_metrics field completeness and source novelty
- normalize_institution_type (canonical values + free-text normalization)
- API endpoints /metrics, /api/metrics/fetch-stats, /api/metrics/research-summary
"""

import re
import time
import sqlite3
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import replace

import pytest

from src.edu_cti.core.metrics import MetricsCollector, _percentile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh() -> MetricsCollector:
    """Return a new isolated MetricsCollector for each test."""
    return MetricsCollector()


# ---------------------------------------------------------------------------
# 1. MetricsCollector core API
# ---------------------------------------------------------------------------

class TestMetricsCollectorCore:
    def test_increment_basic(self):
        m = _fresh()
        m.increment("my_counter")
        assert m.counters["my_counter"] == 1
        m.increment("my_counter", value=5)
        assert m.counters["my_counter"] == 6

    def test_increment_with_labels(self):
        m = _fresh()
        m.increment("fetch_total", labels={"tier": "newspaper3k", "source": "example.com"})
        key = 'fetch_total{source="example.com",tier="newspaper3k"}'
        assert m.counters[key] == 1

    def test_set_gauge(self):
        m = _fresh()
        m.set_gauge("queue_depth", 42.0)
        assert m.gauges["queue_depth"] == 42.0
        m.set_gauge("queue_depth", 0.0)
        assert m.gauges["queue_depth"] == 0.0

    def test_observe_histogram(self):
        m = _fresh()
        for v in [1.0, 2.0, 3.0]:
            m.observe("latency", v)
        assert m.histograms["latency"] == [1.0, 2.0, 3.0]

    def test_timer_start_stop(self):
        m = _fresh()
        m.start_timer("t1")
        time.sleep(0.05)
        dur = m.stop_timer("t1")
        assert dur is not None
        assert 0.04 <= dur <= 0.5

    def test_timer_stop_without_start_returns_none(self):
        m = _fresh()
        assert m.stop_timer("nonexistent") is None

    def test_reset_clears_everything(self):
        m = _fresh()
        m.increment("c", value=10)
        m.set_gauge("g", 5.0)
        m.observe("h", 1.0)
        m.reset()
        assert len(m.counters) == 0
        assert len(m.gauges) == 0
        assert len(m.histograms) == 0

    def test_clear_persistence_removes_stored_metrics(self, tmp_path):
        m = _fresh()
        db_path = tmp_path / "metrics.db"
        m.configure(db_path, flush_interval_seconds=3600)
        m.increment("fetch_total", value=3)
        m.flush_to_db()

        conn = sqlite3.connect(db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM pipeline_metrics").fetchone()[0]
            assert count > 0
        finally:
            conn.close()

        m.clear_persistence()

        conn = sqlite3.connect(db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM pipeline_metrics").fetchone()[0]
            assert count == 0
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# 2. _percentile helper
# ---------------------------------------------------------------------------

class TestPercentileHelper:
    def test_median_odd(self):
        assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == 3.0

    def test_p95_basic(self):
        vals = list(range(1, 101))  # 1..100
        p95 = _percentile([float(v) for v in vals], 95)
        assert 94.0 <= p95 <= 96.0

    def test_single_value(self):
        assert _percentile([7.5], 50) == 7.5
        assert _percentile([7.5], 99) == 7.5

    def test_empty_returns_zero(self):
        assert _percentile([], 50) == 0.0


# ---------------------------------------------------------------------------
# 3. Prometheus text format
# ---------------------------------------------------------------------------

class TestPrometheusFormat:
    def test_label_values_are_quoted(self):
        m = _fresh()
        m.increment("fetch_total", labels={"tier": "newspaper3k", "source": "example.com"})
        output = m.format_prometheus()
        assert 'tier="newspaper3k"' in output
        assert 'source="example.com"' in output
        # Old unquoted format must not appear
        assert "tier=newspaper3k" not in output

    def test_type_line_appears_once_per_metric_family(self):
        m = _fresh()
        for src in ["a.com", "b.com", "c.com"]:
            m.increment("fetch_total", labels={"source": src})
        output = m.format_prometheus()
        type_lines = [ln for ln in output.splitlines() if ln.startswith("# TYPE fetch_total")]
        assert len(type_lines) == 1

    def test_counter_type_declared(self):
        m = _fresh()
        m.increment("my_counter")
        output = m.format_prometheus()
        assert "# TYPE my_counter counter" in output

    def test_gauge_type_declared(self):
        m = _fresh()
        m.set_gauge("my_gauge", 1.0)
        output = m.format_prometheus()
        assert "# TYPE my_gauge gauge" in output

    def test_histogram_emits_quantiles_and_stats(self):
        m = _fresh()
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            m.observe("latency_seconds", v)
        output = m.format_prometheus()
        assert 'quantile="0.5"' in output
        assert 'quantile="0.95"' in output
        assert 'quantile="0.99"' in output
        assert "latency_seconds_count" in output
        assert "latency_seconds_sum" in output
        assert "latency_seconds_min" in output
        assert "latency_seconds_max" in output

    def test_histogram_with_labels_quantile_insertion(self):
        m = _fresh()
        m.observe("dur", 1.5, labels={"tier": "newspaper3k"})
        output = m.format_prometheus()
        # Quantile label should be added inside the existing label set
        assert 'dur{tier="newspaper3k",quantile="0.5"}' in output

    def test_multiple_metric_families_no_cross_type_contamination(self):
        m = _fresh()
        m.increment("counter_a")
        m.set_gauge("gauge_b", 1.0)
        m.observe("histogram_c", 2.0)
        output = m.format_prometheus()
        assert "# TYPE counter_a counter" in output
        assert "# TYPE gauge_b gauge" in output
        assert "# TYPE histogram_c summary" in output

    def test_empty_metrics_produces_header_only(self):
        m = _fresh()
        output = m.format_prometheus()
        assert "EduThreat-CTI" in output

    def test_correct_counter_value_in_output(self):
        m = _fresh()
        m.increment("c", value=42)
        output = m.format_prometheus()
        assert "c 42" in output


# ---------------------------------------------------------------------------
# 4. fetch_stats_by_tier()
# ---------------------------------------------------------------------------

class TestFetchStatsByTier:
    def _populated(self) -> MetricsCollector:
        m = _fresh()
        for tier, attempts, successes in [
            ("scrapling",   120, 100),
            ("newspaper3k", 100, 80),
            ("httpclient",  20,  15),
            ("oxylabs",     5,   4),
            ("archive_org", 2,   2),
        ]:
            lbl = {"tier": tier, "source": "example.com"}
            m.increment("article_fetch_attempts_total", value=attempts, labels=lbl)
            m.increment("article_fetch_success_total", value=successes, labels=lbl)
            m.increment("article_fetch_failure_total", value=attempts - successes,
                        labels={**lbl, "reason": "403"})
            for d in [1.0, 2.0, 3.0]:
                m.observe("article_fetch_duration_seconds", d, labels=lbl)
            for _ in range(successes):
                m.observe("article_content_length_chars", 3000.0, labels=lbl)
        m.increment("serp_queries_total", value=10, labels={"source": "x"})
        m.increment("serp_urls_returned_total", value=30, labels={"source": "x"})
        m.increment("serp_zero_results_total", value=2, labels={"source": "x"})
        m.increment("domain_rate_limit_delays_total", value=5, labels={"domain": "example.com"})
        m.increment("domain_perm_blocked_total", value=1, labels={"domain": "slow.com"})
        return m

    def test_all_tiers_present(self):
        stats = self._populated().fetch_stats_by_tier()
        for tier in ["scrapling", "newspaper3k", "httpclient", "oxylabs", "archive_org", "precheck"]:
            assert tier in stats["by_tier"]

    def test_success_rate_calculation(self):
        stats = self._populated().fetch_stats_by_tier()
        assert stats["by_tier"]["newspaper3k"]["success_rate"] == pytest.approx(0.8, abs=0.01)

    def test_duration_percentiles_populated(self):
        stats = self._populated().fetch_stats_by_tier()
        dur = stats["by_tier"]["newspaper3k"]["duration_s"]
        assert dur["p50"] is not None
        assert dur["p95"] is not None
        assert dur["min"] == pytest.approx(1.0)
        assert dur["max"] == pytest.approx(3.0)

    def test_serp_counts(self):
        stats = self._populated().fetch_stats_by_tier()
        assert stats["serp"]["queries"] == 10
        assert stats["serp"]["urls_returned"] == 30
        assert stats["serp"]["zero_results"] == 2

    def test_rate_limiting_counts(self):
        stats = self._populated().fetch_stats_by_tier()
        assert stats["rate_limiting"]["delays"] == 5
        assert stats["rate_limiting"]["perm_blocks"] == 1

    def test_top_sources_present(self):
        stats = self._populated().fetch_stats_by_tier()
        assert isinstance(stats["top_sources"], list)
        assert len(stats["top_sources"]) >= 1
        top = stats["top_sources"][0]
        assert "domain" in top
        assert "success_rate" in top

    def test_zero_attempts_success_rate_is_zero(self):
        m = _fresh()
        stats = m.fetch_stats_by_tier()
        for tier in ["scrapling", "newspaper3k", "httpclient", "oxylabs", "archive_org", "precheck"]:
            assert stats["by_tier"][tier]["success_rate"] == 0


# ---------------------------------------------------------------------------
# 5. research_summary()
# ---------------------------------------------------------------------------

class TestResearchSummary:
    def _with_llm_metrics(self) -> MetricsCollector:
        m = _fresh()
        m.set_gauge("llm_first_attempt_success_rate", 0.87)
        m.increment("llm_timeout_total", value=3)
        m.increment("llm_invalid_json_total", value=2)
        m.increment("llm_education_relevance_pass_total", value=72, labels={"source": "kb"})
        m.increment("llm_education_relevance_fail_total", value=15, labels={"source": "kb"})
        for c in [0.8, 0.9, 0.7, 0.85, 0.95]:
            m.observe("llm_confidence_score", c)
        return m

    def test_extraction_quality_keys(self):
        rs = self._with_llm_metrics().research_summary()
        eq = rs["extraction_quality"]
        assert eq["llm_first_attempt_success_rate"] == pytest.approx(0.87)
        assert eq["llm_timeout_total"] == 3
        assert eq["llm_invalid_json_total"] == 2
        assert eq["llm_edu_relevance_pass"] == 72
        assert eq["llm_edu_relevance_fail"] == 15

    def test_confidence_score_histogram(self):
        rs = self._with_llm_metrics().research_summary()
        conf = rs["extraction_quality"]["llm_confidence_score"]
        assert conf["count"] == 5
        assert conf["p50"] is not None
        assert conf["min"] == pytest.approx(0.7)
        assert conf["max"] == pytest.approx(0.95)

    def test_field_fill_rates(self):
        m = _fresh()
        m.increment("field_populated_total", value=90, labels={"field": "attack_category"})
        m.increment("field_null_total", value=10, labels={"field": "attack_category"})
        m.increment("field_populated_total", value=40, labels={"field": "threat_actor_name"})
        m.increment("field_null_total", value=60, labels={"field": "threat_actor_name"})
        rs = m.research_summary()
        rates = rs["dataset_completeness"]["field_fill_rates"]
        assert rates["attack_category"] == pytest.approx(0.9)
        assert rates["threat_actor_name"] == pytest.approx(0.4)

    def test_source_novelty(self):
        m = _fresh()
        m.increment("source_novel_incident_total", value=80, labels={"source": "konbriefing"})
        m.increment("source_duplicate_total", value=20, labels={"source": "konbriefing"})
        rs = m.research_summary()
        novelty = rs["source_novelty"]
        assert len(novelty) == 1
        entry = novelty[0]
        assert entry["source"] == "konbriefing"
        assert entry["novel"] == 80
        assert entry["duplicates"] == 20
        assert entry["novel_rate"] == pytest.approx(0.8)

    def test_deduplication_summary(self):
        m = _fresh()
        m.increment("dedup_events_total", value=15,
                    labels={"survivor_source": "konbriefing", "duplicate_source": "ransomwarelive"})
        m.increment("dedup_cross_source_agreement_total", value=10, labels={"field": "attack_category"})
        m.increment("dedup_merge_field_gain_total", value=5, labels={"field": "ransom_amount"})
        rs = m.research_summary()
        ded = rs["deduplication"]
        assert ded["dedup_events_total"] == 15
        assert ded["field_gain_total"] == 5
        assert ded["cross_source_agreement"]["attack_category"] == 10

    def test_empty_metrics_no_crash(self):
        rs = _fresh().research_summary()
        assert "extraction_quality" in rs
        assert "dataset_completeness" in rs
        assert "source_novelty" in rs
        assert "deduplication" in rs


# ---------------------------------------------------------------------------
# 6. Article fetcher tier instrumentation
# ---------------------------------------------------------------------------

class TestArticleFetcherMetrics:
    """Test that fetch_article() increments the right counters and records durations."""

    def _make_fetcher(self, m: MetricsCollector):
        """Build an ArticleFetcher with the metrics singleton patched to m."""
        from src.edu_cti.pipeline.phase2.storage import article_fetcher as af_module
        from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher, ArticleContent

        patched = patch.object(af_module, "_metrics", m)
        patched.start()
        af_module._DYNAMIC_FAILED_DOMAINS.clear()
        fetcher = ArticleFetcher.__new__(ArticleFetcher)
        fetcher.http_client = MagicMock()
        fetcher.oxylabs_client = MagicMock()
        return fetcher, patched, ArticleContent

    def _ok(self, url="http://x.com/a") -> "ArticleContent":
        from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
        return ArticleContent(url=url, title="T", content="x" * 200, fetch_successful=True, content_length=200)

    def _fail(self, url="http://x.com/a", msg="403 Forbidden") -> "ArticleContent":
        from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
        return ArticleContent(url=url, title="", content="", fetch_successful=False,
                              error_message=msg, content_length=0)

    def test_scrapling_success_records_metrics(self, monkeypatch):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            monkeypatch.delenv("EDU_CTI_FETCH_ENABLE_LEGACY_TIERS", raising=False)
            with patch.object(fetcher, "_fetch_with_scrapling", return_value=self._ok()):
                fetcher.fetch_article("http://example-news.com/story")
            assert m.counters['article_fetch_attempts_total{source="example-news.com",tier="scrapling"}'] == 1
            assert m.counters['article_fetch_success_total{source="example-news.com",tier="scrapling"}'] == 1
            assert len(m.histograms['article_fetch_duration_seconds{source="example-news.com",tier="scrapling"}']) == 1
        finally:
            patch_obj.stop()

    def test_scrapling_fail_falls_through_to_oxylabs(self, monkeypatch):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            monkeypatch.setenv("EDU_CTI_OXYLABS_ENABLED", "1")
            monkeypatch.delenv("EDU_CTI_FETCH_ENABLE_LEGACY_TIERS", raising=False)
            with patch.object(fetcher, "_fetch_with_scrapling", return_value=self._fail()), \
                 patch.object(fetcher, "_fetch_with_oxylabs", return_value=self._ok()):
                fetcher.fetch_article("http://bleepingcomputer.com/news/a")
            assert m.counters.get('article_fetch_failure_total{reason="403",source="bleepingcomputer.com",tier="scrapling"}', 0) == 1
            assert m.counters.get('article_fetch_success_total{source="bleepingcomputer.com",tier="oxylabs"}', 0) == 1
        finally:
            patch_obj.stop()

    def test_default_tiers_fail_records_enabled_tier_failures(self, monkeypatch):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            monkeypatch.setenv("EDU_CTI_OXYLABS_ENABLED", "1")
            monkeypatch.delenv("EDU_CTI_FETCH_ENABLE_LEGACY_TIERS", raising=False)
            with patch.object(fetcher, "_fetch_with_scrapling", return_value=self._fail()), \
                 patch.object(fetcher, "_fetch_with_oxylabs", return_value=self._fail()), \
                 patch.object(fetcher, "_fetch_from_archive", return_value=self._fail()):
                result = fetcher.fetch_article("http://example.com/article")
            assert not result.fetch_successful
            total_failures = sum(v for k, v in m.counters.items()
                                 if "article_fetch_failure_total" in k and "example.com" in k)
            assert total_failures == 4
        finally:
            patch_obj.stop()

    def test_blocked_domain_emits_failure_counter(self):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            with patch("src.edu_cti.pipeline.phase2.storage.article_fetcher.BLOCKED_FETCH_DOMAINS",
                       {"twitter.com"}):
                fetcher.fetch_article("http://twitter.com/status/123")
            blocked_failures = sum(v for k, v in m.counters.items()
                                   if "blocked_domain" in k)
            assert blocked_failures >= 1
        finally:
            patch_obj.stop()

    def test_securityweek_is_not_preblocked(self, monkeypatch):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            monkeypatch.delenv("EDU_CTI_FETCH_ENABLE_LEGACY_TIERS", raising=False)
            with patch.object(fetcher, "_fetch_with_scrapling", return_value=self._ok("https://www.securityweek.com/story")):
                result = fetcher.fetch_article("https://www.securityweek.com/story")
            assert result.fetch_successful is True
            assert result.fetch_metadata["selected_tier"] == "scrapling"
            assert not any("blocked_domain" in key for key in m.counters)
        finally:
            patch_obj.stop()

    def test_oxylabs_success_emits_correct_tier_label(self, monkeypatch):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            monkeypatch.setenv("EDU_CTI_OXYLABS_ENABLED", "1")
            with patch.object(fetcher, "_fetch_with_scrapling", return_value=self._fail()), \
                 patch.object(fetcher, "_fetch_with_oxylabs", return_value=self._ok("http://example.com/x")):
                fetcher.fetch_article("http://example.com/article")
            assert m.counters.get('article_fetch_success_total{source="example.com",tier="oxylabs"}', 0) == 1
        finally:
            patch_obj.stop()

    def test_archive_org_success_records_duration(self, monkeypatch):
        m = _fresh()
        fetcher, patch_obj, ArticleContent = self._make_fetcher(m)
        try:
            monkeypatch.setenv("EDU_CTI_OXYLABS_ENABLED", "1")
            with patch.object(fetcher, "_fetch_with_scrapling", return_value=self._fail()), \
                 patch.object(fetcher, "_fetch_with_oxylabs", return_value=self._fail()), \
                 patch.object(fetcher, "_fetch_from_archive", return_value=self._ok("http://example.com/x")):
                fetcher.fetch_article("http://example.com/article")
            dur_key = 'article_fetch_duration_seconds{source="example.com",tier="archive_org"}'
            assert len(m.histograms.get(dur_key, [])) == 1
        finally:
            patch_obj.stop()


# ---------------------------------------------------------------------------
# 7. Enrichment LLM instrumentation
# ---------------------------------------------------------------------------

class TestEnrichmentMetrics:
    def test_llm_timeout_increments_counter(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import enrichment as enrichment_module
        with patch.object(enrichment_module, "_metrics", m):
            enricher = enrichment_module.IncidentEnricher.__new__(enrichment_module.IncidentEnricher)
            enricher.llm_client = MagicMock()
            enricher.llm_client.extract_json.side_effect = TimeoutError("timed out")
            incident = MagicMock()
            article_contents = {"http://x.com": MagicMock(
                title="T", content="x" * 200, publish_date=None, author=None
            )}
            incident.incident_id = "test_001"
            incident.notes = ""
            incident.institution_name = "Test University"
            incident.source_published_date = None
            try:
                enricher._enrich_article(incident, article_contents)
            except Exception:
                pass
            assert m.counters.get("llm_timeout_total", 0) >= 1

    def test_invalid_json_increments_counter(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import enrichment as enrichment_module
        with patch.object(enrichment_module, "_metrics", m):
            enricher = enrichment_module.IncidentEnricher.__new__(enrichment_module.IncidentEnricher)
            enricher.llm_client = MagicMock()
            enricher.llm_client.extract_json.return_value = "not json at all !!!"
            incident = MagicMock()
            incident.incident_id = "test_002"
            incident.notes = ""
            incident.institution_name = "Test University"
            incident.source_published_date = None
            article_contents = {"http://x.com": MagicMock(
                title="T", content="x" * 200, publish_date=None, author=None
            )}
            with patch.object(enricher, "_parse_json_response", return_value=None):
                enricher._enrich_article(incident, article_contents)
            assert m.counters.get("llm_invalid_json_total", 0) >= 1

    def test_confidence_score_observed(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import enrichment as enrichment_module
        with patch.object(enrichment_module, "_metrics", m):
            enricher = enrichment_module.IncidentEnricher.__new__(enrichment_module.IncidentEnricher)
            enricher.llm_client = MagicMock()
            enricher.llm_client.extract_json.return_value = "{}"
            enricher.llm_client.chat.return_value = MagicMock(message=MagicMock(content="summary"))

            incident = MagicMock()
            incident.incident_id = "test_003"
            incident.notes = ""
            incident.institution_name = "Test University"
            incident.source_published_date = None
            article_contents = {"http://x.com": MagicMock(
                title="T", content="x" * 200, publish_date=None, author=None
            )}

            mock_result = MagicMock()
            mock_result.education_relevance = MagicMock(is_education_related=True)
            mock_result.primary_url = "http://x.com"

            with patch.object(enricher, "_parse_json_response",
                              return_value={"is_edu_cyber_incident": True, "confidence_score": 0.88}), \
                 patch("src.edu_cti.pipeline.phase2.enrichment.json_to_cti_enrichment",
                       return_value=mock_result), \
                 patch("src.edu_cti.pipeline.phase2.enrichment._coerce_bool_like", return_value=True):
                enricher._enrich_article(incident, article_contents)

            assert 0.88 in m.histograms.get("llm_confidence_score", [])


# ---------------------------------------------------------------------------
# 8. Deduplication metrics
# ---------------------------------------------------------------------------

class TestDeduplicationMetrics:
    @pytest.fixture
    def temp_db(self, tmp_path):
        from src.edu_cti.core.db import get_connection, init_db
        from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
        from src.edu_cti.pipeline.phase2.storage.article_storage import init_articles_table
        db_path = tmp_path / "test_dedup.db"
        conn = get_connection(db_path)
        init_db(conn)
        init_incident_enrichments_table(conn)
        init_articles_table(conn)
        yield conn
        conn.close()

    def _insert_bare_incident(self, conn, incident_id, source="konbriefing"):
        conn.execute("""
            INSERT INTO incidents (incident_id, institution_name, victim_raw_name, country,
                institution_type, incident_date, date_precision, ingested_at, last_updated_at,
                title, primary_url, status, source_confidence)
            VALUES (?, 'Test University', 'Test University', 'United States',
                'university', '2024-01-15', 'day',
                '2024-01-16T00:00:00', '2024-01-16T00:00:00',
                'Test incident', 'https://example.com/1', 'confirmed', 'high')
        """, (incident_id,))
        conn.execute("""
            INSERT INTO incident_sources (incident_id, source, source_event_id, first_seen_at, confidence)
            VALUES (?, ?, ?, '2024-01-16T00:00:00', 0.9)
        """, (incident_id, source, f"{source}_{incident_id}"))
        conn.commit()

    def test_dedup_events_total_increments(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import deduplication as dedup_module
        with patch.object(dedup_module, "_metrics", m):
            self._insert_bare_incident(temp_db, "konbriefing_001", "konbriefing")
            self._insert_bare_incident(temp_db, "ransomwarelive_001", "ransomwarelive")
            dedup_module._merge_duplicate_into_keeper(temp_db, "konbriefing_001", "ransomwarelive_001")
            temp_db.commit()
        assert m.counters.get(
            'dedup_events_total{duplicate_source="ransomwarelive",survivor_source="konbriefing"}', 0
        ) == 1

    def test_cross_source_agreement_for_matching_fields(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import deduplication as dedup_module
        from src.edu_cti.pipeline.phase2.storage.db import init_incident_enrichments_table
        # Insert flat rows with matching attack_category for both incidents
        for inc_id in ["konbriefing_002", "ransomwarelive_002"]:
            self._insert_bare_incident(temp_db, inc_id,
                                       "konbriefing" if "konbriefing" in inc_id else "ransomwarelive")
            temp_db.execute("""
                INSERT INTO incident_enrichments_flat
                    (incident_id, attack_category, created_at, updated_at)
                VALUES (?, 'ransomware', '2024-01-16T00:00:00', '2024-01-16T00:00:00')
            """, (inc_id,))
        temp_db.commit()
        with patch.object(dedup_module, "_metrics", m):
            dedup_module._merge_duplicate_into_keeper(temp_db, "konbriefing_002", "ransomwarelive_002")
            temp_db.commit()
        agreement_count = m.counters.get(
            'dedup_cross_source_agreement_total{field="attack_category"}', 0
        )
        assert agreement_count == 1

    def test_field_gain_when_survivor_null_dup_has_value(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import deduplication as dedup_module
        self._insert_bare_incident(temp_db, "konbriefing_003", "konbriefing")
        self._insert_bare_incident(temp_db, "ransomwarelive_003", "ransomwarelive")
        # Survivor has no threat_actor_name; duplicate has it
        temp_db.execute("""
            INSERT INTO incident_enrichments_flat
                (incident_id, threat_actor_name, created_at, updated_at)
            VALUES ('konbriefing_003', NULL, '2024-01-16T00:00:00', '2024-01-16T00:00:00')
        """)
        # Note: threat_actor_name is not in _COMPARABLE_FLAT_FIELDS so use attack_category
        temp_db.execute("""
            INSERT INTO incident_enrichments_flat
                (incident_id, attack_category, created_at, updated_at)
            VALUES ('ransomwarelive_003', 'ransomware', '2024-01-16T00:00:00', '2024-01-16T00:00:00')
        """)
        temp_db.commit()
        with patch.object(dedup_module, "_metrics", m):
            dedup_module._merge_duplicate_into_keeper(temp_db, "konbriefing_003", "ransomwarelive_003")
            temp_db.commit()
        # konbriefing_003 had no attack_category in flat table → field gain
        gain_keys = [k for k in m.counters if "dedup_merge_field_gain_total" in k]
        assert len(gain_keys) >= 1


# ---------------------------------------------------------------------------
# 9. SERP and rate-limit metrics
# ---------------------------------------------------------------------------

class TestSerpMetrics:
    def test_serp_query_increments_counter(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs_module
        with patch.object(fs_module, "_metrics", m), \
             patch.object(fs_module.OxylabsClient, "search_news", return_value=[
                 {"url": "https://bleepingcomputer.com/news/a"}
             ]):
            incident = {
                "incident_id": "konbriefing_99",
                "institution_name": "Test University",
                "attack_type_hint": "ransomware",
                "incident_date": "2024-03-15",
                "title": "Test University ransomware attack",
            }
            fs_module.discover_articles_via_serp(incident)
        assert m.counters.get('serp_queries_total{source="konbriefing"}', 0) == 1

    def test_serp_urls_returned_increments(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs_module
        # Use two non-blocked domains. databreaches.net is in BLOCKED_FETCH_DOMAINS
        # (consistently fails all fetch tiers) so it is filtered out by SERP.
        with patch.object(fs_module, "_metrics", m), \
             patch.object(fs_module.OxylabsClient, "search_news", return_value=[
                 {"url": "https://bleepingcomputer.com/news/a"},
                 {"url": "https://therecord.media/b"},
             ]):
            incident = {
                "incident_id": "konbriefing_100",
                "institution_name": "Test University",
                "attack_type_hint": "ransomware",
                "incident_date": "2024-03-15",
                "title": "",
            }
            fs_module.discover_articles_via_serp(incident)
        assert m.counters.get('serp_urls_returned_total{source="konbriefing"}', 0) == 2

    def test_serp_zero_results_increments(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs_module
        with patch.object(fs_module, "_metrics", m), \
             patch.object(fs_module.OxylabsClient, "search_news", return_value=[]):
            incident = {
                "incident_id": "konbriefing_101",
                "institution_name": "Test University",
                "attack_type_hint": "ransomware",
                "incident_date": "2024-03-15",
                "title": "",
            }
            fs_module.discover_articles_via_serp(incident)
        assert m.counters.get('serp_zero_results_total{source="konbriefing"}', 0) == 1

    def test_rate_limit_delay_increments(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs_module
        with patch.object(fs_module, "_metrics", m), \
             patch("time.sleep"):  # don't actually sleep
            limiter = fs_module.DomainRateLimiter(min_delay_seconds=0.001, max_delay_seconds=0.002)
            limiter.domain_last_fetch["example.com"] = __import__("datetime").datetime.utcnow()
            limiter.wait_if_needed("example.com")
        assert m.counters.get('domain_rate_limit_delays_total{domain="example.com"}', 0) == 1

    def test_perm_blocked_increments_on_rate_limit_exceeded(self):
        m = _fresh()
        from src.edu_cti.pipeline.phase2.utils import fetching_strategy as fs_module
        with patch.object(fs_module, "_metrics", m):
            limiter = fs_module.DomainRateLimiter(max_fetches_per_hour=2)
            import datetime
            now = datetime.datetime.utcnow()
            limiter.domain_fetch_counts["hot.com"] = [now, now]  # already at limit
            result = limiter.can_fetch_from_domain("hot.com")
        assert not result
        assert m.counters.get('domain_perm_blocked_total{domain="hot.com"}', 0) == 1


# ---------------------------------------------------------------------------
# 10. _emit_enrichment_metrics
# ---------------------------------------------------------------------------

class TestEmitEnrichmentMetrics:
    @pytest.fixture
    def temp_db(self, tmp_path):
        from src.edu_cti.core.db import get_connection, init_db
        db_path = tmp_path / "test_emit.db"
        conn = get_connection(db_path)
        init_db(conn)
        conn.execute("""
            INSERT INTO incidents (incident_id, institution_name, victim_raw_name, country,
                institution_type, incident_date, date_precision, ingested_at, last_updated_at,
                title, primary_url, status, source_confidence)
            VALUES ('kb_001', 'Test', 'Test', 'US', 'university', '2024-01-01', 'day',
                    '2024-01-02', '2024-01-02', 'T', 'http://x.com', 'confirmed', 'high')
        """)
        conn.execute("""
            INSERT INTO incident_sources (incident_id, source, source_event_id, first_seen_at, confidence)
            VALUES ('kb_001', 'konbriefing', 'kb_ev_001', '2024-01-02', 0.9)
        """)
        conn.commit()
        yield conn
        conn.close()

    def test_field_populated_counter_increments(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import __main__ as main_module
        with patch.object(main_module, "_metrics", m):
            raw = {
                "attack_category": "ransomware",
                "institution_name": "Test University",
                "incident_date": "2024-01-15",
                "country": "United States",
                "ransomware_family": "LockBit",
                "confidence_score": 0.9,
            }
            main_module._emit_enrichment_metrics("kb_001", MagicMock(), raw, temp_db)
        assert m.counters.get('field_populated_total{field="attack_category"}', 0) == 1
        assert m.counters.get('field_populated_total{field="ransomware_family"}', 0) == 1

    def test_field_null_counter_for_missing_field(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import __main__ as main_module
        with patch.object(main_module, "_metrics", m):
            raw = {"attack_category": "ransomware"}  # most fields missing
            main_module._emit_enrichment_metrics("kb_001", MagicMock(), raw, temp_db)
        # threat_actor_name should be null
        assert m.counters.get('field_null_total{field="threat_actor_name"}', 0) == 1

    def test_completeness_score_observed(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import __main__ as main_module
        with patch.object(main_module, "_metrics", m):
            raw = {
                "attack_category": "ransomware",
                "institution_name": "Test University",
                "incident_date": "2024-01-15",
                "country": "United States",
                "ransomware_family": "LockBit",
            }
            main_module._emit_enrichment_metrics("kb_001", MagicMock(), raw, temp_db)
        scores = m.histograms.get("incident_completeness_score", [])
        assert len(scores) == 1
        assert scores[0] == 5.0  # 5 of 10 key fields populated

    def test_source_novel_increments_for_single_source(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import __main__ as main_module
        with patch.object(main_module, "_metrics", m):
            main_module._emit_enrichment_metrics("kb_001", MagicMock(), {}, temp_db)
        assert m.counters.get('source_novel_incident_total{source="kb"}', 0) == 1

    def test_cost_per_incident_observed(self, temp_db):
        m = _fresh()
        from src.edu_cti.pipeline.phase2 import __main__ as main_module
        with patch.object(main_module, "_metrics", m):
            main_module._emit_enrichment_metrics("kb_001", MagicMock(), {}, temp_db)
        costs = m.histograms.get("enrichment_cost_per_incident_usd", [])
        assert len(costs) == 1
        assert costs[0] == pytest.approx(0.0135, abs=0.005)  # ~$0.013 per incident


# ---------------------------------------------------------------------------
# 11. normalize_institution_type
# ---------------------------------------------------------------------------

class TestNormalizeInstitutionType:
    def _norm(self, v):
        from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import normalize_institution_type
        return normalize_institution_type(v)

    # Already-canonical values pass through unchanged
    @pytest.mark.parametrize("canonical", [
        "university",
        "community_college", "technical_college", "vocational_school",
        "k12_school", "school_district", "research_institute", "research_center",
        "medical_school", "university_hospital", "teaching_hospital",
        "online_university", "library", "tribal_college", "military_academy",
        "edtech_platform", "tutoring_service", "consortium",
        "education_department", "education_ministry", "student_loan_servicer",
        "education_nonprofit", "education_vendor", "unknown",
    ])
    def test_canonical_passthrough(self, canonical):
        assert self._norm(canonical) == canonical

    # Free-text source labels normalize correctly
    @pytest.mark.parametrize("raw,expected", [
        ("University", "university"),
        ("university", "university"),
        ("School", "k12_school"),
        ("school", "k12_school"),
        ("Research Institute", "research_institute"),
        ("research institute", "research_institute"),
        ("school_district", "school_district"),
        ("School District", "school_district"),
        ("edtech", "edtech_platform"),
        ("library", "library"),
        ("Unknown", "unknown"),
        ("N/A", "unknown"),
        ("", "unknown"),
        ("community college", "community_college"),
        ("Department of Education", "education_department"),
        ("tribal college", "tribal_college"),
        ("military academy", "military_academy"),
        ("tutoring", "tutoring_service"),
    ])
    def test_freetext_normalization(self, raw, expected):
        assert self._norm(raw) == expected

    def test_none_returns_none(self):
        assert self._norm(None) is None

    def test_non_string_returns_unknown(self):
        assert self._norm(123) == "unknown"

    def test_unknown_vendor_name_returns_unknown(self):
        # "PowerSchool" is a vendor name — not "school"
        assert self._norm("PowerSchool") == "unknown"

    def test_case_insensitive(self):
        assert self._norm("UNIVERSITY") == "university"
        assert self._norm("SCHOOL DISTRICT") == "school_district"


# ---------------------------------------------------------------------------
# 12. API endpoint smoke tests
# ---------------------------------------------------------------------------

class TestMetricsAPIEndpoints:
    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from src.edu_cti.api.main import app
        return TestClient(app)

    def test_prometheus_metrics_endpoint_returns_200(self, client):
        r = client.get("/metrics")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/plain")

    def test_prometheus_output_contains_header(self, client):
        r = client.get("/metrics")
        assert "EduThreat-CTI" in r.text

    def test_fetch_stats_endpoint_returns_200_json(self, client):
        r = client.get("/api/metrics/fetch-stats")
        assert r.status_code == 200
        data = r.json()
        assert "by_tier" in data
        assert "serp" in data
        assert "rate_limiting" in data
        for tier in ["scrapling", "newspaper3k", "httpclient", "oxylabs", "archive_org", "precheck"]:
            assert tier in data["by_tier"]

    def test_research_summary_endpoint_returns_200_json(self, client):
        r = client.get("/api/metrics/research-summary")
        assert r.status_code == 200
        data = r.json()
        assert "extraction_quality" in data
        assert "dataset_completeness" in data
        assert "source_novelty" in data
        assert "deduplication" in data
        assert "fetch_performance" in data
        assert "pipeline" in data

    def test_fetch_stats_tier_has_required_keys(self, client):
        r = client.get("/api/metrics/fetch-stats")
        tier = r.json()["by_tier"]["scrapling"]
        for key in ["attempts", "successes", "failures", "success_rate",
                    "failure_breakdown", "duration_s", "content_length_chars"]:
            assert key in tier

    def test_research_summary_field_fill_rates_is_dict(self, client):
        r = client.get("/api/metrics/research-summary")
        rates = r.json()["dataset_completeness"]["field_fill_rates"]
        assert isinstance(rates, dict)
