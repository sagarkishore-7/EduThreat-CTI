"""Read-only source coverage and data-quality audit for the v2 runtime."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.edu_cti.core.config import (
    GOOGLE_NEWS_RSS_COUNTRIES_BY_LANG,
    GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS,
    GOOGLE_NEWS_RSS_QUERIES,
    NEWS_SEARCH_QUERIES_ALL,
    NEWS_SEARCH_SITE_RESTRICTED_QUERIES,
)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _mapping_rows(session: Session, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows = session.execute(text(sql), params or {}).mappings().all()
    return [{str(key): _json_safe(value) for key, value in row.items()} for row in rows]


def _single_mapping(session: Session, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    row = session.execute(text(sql), params or {}).mappings().first()
    if row is None:
        return {}
    return {str(key): _json_safe(value) for key, value in row.items()}


class V2SourceHealthService:
    """Build a source-by-source health snapshot without mutating production data."""

    def get_source_health(self, session: Session, *, sample_limit: int = 25) -> dict[str, Any]:
        sample_limit = max(1, min(int(sample_limit), 200))
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "totals": self._totals(session),
            "source_rows": self._source_rows(session),
            "task_backlog": self._task_backlog(session),
            "source_task_backlog": self._source_task_backlog(session),
            "fetch_tiers": self._fetch_tiers(session),
            "latest_task_markers": self._latest_task_markers(session, limit=sample_limit),
            "quality": self._quality(session, limit=sample_limit),
            "google_news_rss_config": self._google_news_config(),
            "latest_collection_discovery_metrics": self._latest_collection_discovery_metrics(session),
        }

    def _totals(self, session: Session) -> dict[str, Any]:
        return _single_mapping(
            session,
            """
            SELECT
              (SELECT COUNT(*) FROM source_incidents WHERE is_deleted IS FALSE) AS source_incidents,
              (SELECT COUNT(*) FROM source_incident_urls) AS source_url_rows,
              (SELECT COUNT(*) FROM source_incident_urls WHERE url_kind = 'article') AS article_url_rows,
              (SELECT COUNT(*) FROM source_incident_urls WHERE is_wrapper IS TRUE) AS wrapper_url_rows,
              (SELECT COUNT(*) FROM source_incident_urls WHERE is_wrapper IS TRUE AND resolved_url IS NOT NULL AND resolved_url <> '') AS resolved_wrapper_url_rows,
              (SELECT COUNT(*) FROM article_fetch_attempts) AS article_fetch_attempts,
              (SELECT COUNT(*) FROM article_fetch_attempts WHERE success IS TRUE) AS successful_article_fetch_attempts,
              (SELECT COUNT(*) FROM article_documents) AS article_documents,
              (SELECT COUNT(DISTINCT source_incident_id) FROM article_documents WHERE is_selected_for_enrichment IS TRUE) AS selected_article_sources,
              (SELECT COUNT(*) FROM source_enrichments) AS source_enrichments,
              (SELECT COUNT(*) FROM source_enrichments WHERE manual_review_required IS TRUE) AS manual_review_rows,
              (SELECT COUNT(*) FROM source_enrichments WHERE is_education_related IS FALSE AND manual_review_required IS FALSE) AS hard_reject_rows,
              (SELECT COUNT(*) FROM canonical_incidents) AS canonical_incidents,
              (SELECT COUNT(*) FROM canonical_incidents WHERE status = 'open') AS open_canonical_incidents
            """,
        )

    def _source_rows(self, session: Session) -> list[dict[str, Any]]:
        return _mapping_rows(
            session,
            """
            SELECT
              si.source_name,
              MIN(si.source_group) AS source_group,
              COUNT(DISTINCT si.id) AS rows_collected,
              COUNT(DISTINCT siu.id) AS url_rows,
              COUNT(DISTINCT CASE WHEN siu.url_kind = 'article' THEN siu.id END) AS article_url_rows,
              COUNT(DISTINCT CASE WHEN siu.is_wrapper IS TRUE THEN siu.id END) AS wrapper_url_rows,
              COUNT(DISTINCT CASE WHEN siu.is_wrapper IS TRUE AND siu.resolved_url IS NOT NULL AND siu.resolved_url <> '' THEN siu.id END) AS resolved_wrapper_url_rows,
              COUNT(DISTINCT afa.id) AS fetch_attempts,
              COUNT(DISTINCT CASE WHEN afa.success IS TRUE THEN afa.id END) AS fetch_successes,
              COUNT(DISTINCT ad.id) AS article_documents,
              COUNT(DISTINCT CASE WHEN ad.is_selected_for_enrichment IS TRUE THEN ad.source_incident_id END) AS selected_article_sources,
              COUNT(DISTINCT se.id) AS enrichment_count,
              COUNT(DISTINCT CASE WHEN se.manual_review_required IS TRUE THEN se.id END) AS manual_review_count,
              COUNT(DISTINCT CASE WHEN se.is_education_related IS FALSE AND se.manual_review_required IS FALSE THEN se.id END) AS hard_reject_count,
              COUNT(DISTINCT ci.id) AS canonical_yield,
              COUNT(DISTINCT CASE WHEN ci.status = 'open' THEN ci.id END) AS open_canonical_yield,
              MAX(si.collected_at) AS latest_source_collected_at,
              MAX(afa.attempted_at) AS latest_fetch_attempt_at,
              MAX(se.updated_at) AS latest_enrichment_updated_at
            FROM source_incidents si
            LEFT JOIN source_incident_urls siu ON siu.source_incident_id = si.id
            LEFT JOIN article_fetch_attempts afa ON afa.source_incident_id = si.id
            LEFT JOIN article_documents ad ON ad.source_incident_id = si.id
            LEFT JOIN source_enrichments se ON se.source_incident_id = si.id
            LEFT JOIN canonical_memberships cm ON cm.source_incident_id = si.id
            LEFT JOIN canonical_incidents ci ON ci.id = cm.canonical_incident_id
            WHERE si.is_deleted IS FALSE
            GROUP BY si.source_name
            ORDER BY rows_collected DESC, si.source_name ASC
            """,
        )

    def _task_backlog(self, session: Session) -> list[dict[str, Any]]:
        return _mapping_rows(
            session,
            """
            SELECT task_type, status, COUNT(*) AS count
            FROM pipeline_tasks
            GROUP BY task_type, status
            ORDER BY task_type, status
            """,
        )

    def _source_task_backlog(self, session: Session) -> list[dict[str, Any]]:
        return _mapping_rows(
            session,
            """
            SELECT
              COALESCE(si.source_name, pt.payload->>'source_name', 'unknown') AS source_name,
              pt.task_type,
              pt.status,
              COUNT(*) AS count
            FROM pipeline_tasks pt
            LEFT JOIN source_incidents si
              ON pt.target_table = 'source_incidents'
             AND pt.target_id = si.id
            WHERE pt.status IN ('queued', 'leased', 'failed', 'dead_letter')
            GROUP BY COALESCE(si.source_name, pt.payload->>'source_name', 'unknown'), pt.task_type, pt.status
            ORDER BY count DESC, source_name ASC, pt.task_type ASC, pt.status ASC
            """,
        )

    def _fetch_tiers(self, session: Session) -> list[dict[str, Any]]:
        return _mapping_rows(
            session,
            """
            SELECT
              si.source_name,
              afa.fetch_tier,
              afa.success,
              COUNT(*) AS attempts,
              AVG(afa.content_length) AS avg_content_length,
              MAX(afa.attempted_at) AS latest_attempt_at
            FROM article_fetch_attempts afa
            JOIN source_incidents si ON si.id = afa.source_incident_id
            GROUP BY si.source_name, afa.fetch_tier, afa.success
            ORDER BY si.source_name ASC, afa.fetch_tier ASC, afa.success DESC
            """,
        )

    def _latest_task_markers(self, session: Session, *, limit: int) -> list[dict[str, Any]]:
        return _mapping_rows(
            session,
            """
            SELECT
              COALESCE(si.source_name, pt.payload->>'source_name', 'unknown') AS source_name,
              pt.task_type,
              pt.status,
              pt.error,
              pt.attempt_count,
              pt.max_attempts,
              pt.updated_at,
              pt.payload
            FROM pipeline_tasks pt
            LEFT JOIN source_incidents si
              ON pt.target_table = 'source_incidents'
             AND pt.target_id = si.id
            WHERE pt.error IS NOT NULL OR pt.status IN ('failed', 'dead_letter')
            ORDER BY pt.updated_at DESC NULLS LAST, pt.created_at DESC NULLS LAST
            LIMIT :limit
            """,
            {"limit": limit},
        )

    def _quality(self, session: Session, *, limit: int) -> dict[str, Any]:
        return {
            "future_dated_canonicals": _mapping_rows(
                session,
                """
                SELECT id AS canonical_incident_id, institution_name, vendor_name, incident_date, source_published_at, attack_category
                FROM canonical_incidents
                WHERE status = 'open' AND incident_date > CURRENT_DATE + INTERVAL '3 days'
                ORDER BY incident_date DESC
                LIMIT :limit
                """,
                {"limit": limit},
            ),
            "monthly_peaks": _mapping_rows(
                session,
                """
                SELECT DATE_TRUNC('month', incident_date)::date AS incident_month, COUNT(*) AS incident_count
                FROM canonical_incidents
                WHERE status = 'open' AND incident_date IS NOT NULL
                GROUP BY DATE_TRUNC('month', incident_date)::date
                ORDER BY incident_count DESC, incident_month DESC
                LIMIT :limit
                """,
                {"limit": limit},
            ),
            "manual_review_sample": _mapping_rows(
                session,
                """
                SELECT
                  si.id AS source_incident_id,
                  si.source_name,
                  si.raw_title,
                  COALESCE(si.raw_institution_name, si.raw_victim_name) AS source_identity,
                  se.manual_review_reason,
                  se.updated_at
                FROM source_enrichments se
                JOIN source_incidents si ON si.id = se.source_incident_id
                WHERE se.manual_review_required IS TRUE
                ORDER BY se.updated_at DESC NULLS LAST, se.created_at DESC NULLS LAST
                LIMIT :limit
                """,
                {"limit": limit},
            ),
            "hard_reject_sample": _mapping_rows(
                session,
                """
                SELECT
                  si.id AS source_incident_id,
                  si.source_name,
                  si.raw_title,
                  COALESCE(si.raw_institution_name, si.raw_victim_name) AS source_identity,
                  se.failed_reason,
                  se.updated_at
                FROM source_enrichments se
                JOIN source_incidents si ON si.id = se.source_incident_id
                WHERE se.is_education_related IS FALSE
                  AND se.manual_review_required IS FALSE
                ORDER BY se.updated_at DESC NULLS LAST, se.created_at DESC NULLS LAST
                LIMIT :limit
                """,
                {"limit": limit},
            ),
        }

    def _latest_collection_discovery_metrics(self, session: Session) -> dict[str, Any]:
        row = _single_mapping(
            session,
            """
            SELECT id AS run_id, status, started_at, finished_at, result
            FROM pipeline_runs
            WHERE run_type = 'collect'
              AND result ? 'source_discovery_metrics'
            ORDER BY COALESCE(finished_at, updated_at, created_at) DESC NULLS LAST
            LIMIT 1
            """,
        )
        result = row.get("result")
        if not isinstance(result, dict):
            return row
        return {
            "run_id": row.get("run_id"),
            "status": row.get("status"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "per_source_counts": result.get("per_source_counts") or {},
            "source_discovery_policies": result.get("source_discovery_policies") or {},
            "source_discovery_metrics": result.get("source_discovery_metrics") or {},
        }

    def _google_news_config(self) -> dict[str, Any]:
        countries = sorted({country for values in GOOGLE_NEWS_RSS_COUNTRIES_BY_LANG.values() for country in values})
        return {
            "configured_query_tuples": len(GOOGLE_NEWS_RSS_QUERIES),
            "unique_countries": len(countries),
            "countries": countries,
            "language_groups": sorted(GOOGLE_NEWS_RSS_COUNTRIES_BY_LANG.keys()),
            "language_group_count": len(GOOGLE_NEWS_RSS_COUNTRIES_BY_LANG),
            "historical_window_days": GOOGLE_NEWS_RSS_HISTORICAL_WINDOW_DAYS,
            "base_query_count": len(NEWS_SEARCH_QUERIES_ALL),
            "site_restricted_query_count": len(NEWS_SEARCH_SITE_RESTRICTED_QUERIES),
        }
