"""v2 fetch-task processing using the existing article extraction stack."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from src.edu_cti.pipeline.phase2.storage import ArticleContent, ArticleFetcher
from src.edu_cti_v2.models import ArticleDocument, ArticleFetchAttempt, PipelineTask, SourceIncident
from src.edu_cti_v2.repositories import ArticleRepository, PipelineTaskRepository


def _parse_publish_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None

    text = value.strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed_datetime = datetime.fromisoformat(text)
        return parsed_datetime.date()
    except ValueError:
        pass

    if len(text) >= 10:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None

    return None


def _extract_tier_attempts(article: ArticleContent) -> list[dict[str, Any]]:
    metadata = article.fetch_metadata if isinstance(article.fetch_metadata, dict) else {}
    attempts = metadata.get("tier_attempts")
    if isinstance(attempts, list) and attempts:
        return [attempt for attempt in attempts if isinstance(attempt, dict)]
    return [
        {
            "tier": metadata.get("selected_tier") or "fetch_chain",
            "success": bool(article.fetch_successful),
            "latency_ms": None,
            "content_length": article.content_length,
            "error_code": None if article.fetch_successful else "fetch_chain_failed",
            "error_message": article.error_message,
        }
    ]


class V2FetchService:
    """Handles `fetch_article` tasks for source incidents."""

    def __init__(
        self,
        *,
        article_fetcher: Optional[ArticleFetcher] = None,
        article_repository: Optional[ArticleRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.article_fetcher = article_fetcher or ArticleFetcher()
        self.article_repository = article_repository or ArticleRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def fetch_articles_for_source_incident(
        self,
        session: Session,
        source_incident: SourceIncident,
        *,
        worker_id: str,
    ) -> Dict[str, int]:
        fetchable_urls = [
            url_row
            for url_row in (source_incident.urls or [])
            if url_row.url_kind == "article" and not url_row.is_wrapper
        ]

        now = datetime.now(timezone.utc)
        success_count = 0
        failure_count = 0
        selected_written = False

        for url_row in fetchable_urls:
            article = self.article_fetcher.fetch_article(url_row.url)
            tier_attempts = _extract_tier_attempts(article)
            is_successful = bool(article.fetch_successful and article.content)
            if not is_successful:
                for attempt_index, attempt_payload in enumerate(tier_attempts, start=1):
                    attempt = ArticleFetchAttempt(
                        source_incident_id=source_incident.id,
                        source_incident_url_id=url_row.id,
                        fetch_tier=str(attempt_payload.get("tier") or "fetch_chain"),
                        attempted_at=now + timedelta(milliseconds=attempt_index - 1),
                        worker_id=worker_id,
                        success=bool(attempt_payload.get("success")),
                        http_status=None,
                        latency_ms=attempt_payload.get("latency_ms"),
                        content_length=attempt_payload.get("content_length"),
                        error_code=attempt_payload.get("error_code"),
                        error_message=attempt_payload.get("error_message"),
                        response_metadata={
                            "fetched_url": article.url,
                            "selected_for_enrichment": False,
                            "attempt_index": attempt_index,
                            "selected_tier": (
                                article.fetch_metadata.get("selected_tier")
                                if isinstance(article.fetch_metadata, dict)
                                else None
                            ),
                        },
                    )
                    self.article_repository.add_fetch_attempt(session, attempt)
                failure_count += 1
                continue

            success_count += 1
            existing_document = self.article_repository.get_document_by_source_url(session, url_row.id)
            is_selected = not selected_written
            selected_tier = (
                article.fetch_metadata.get("selected_tier")
                if isinstance(article.fetch_metadata, dict)
                else None
            ) or str((tier_attempts[-1].get("tier") if tier_attempts else "fetch_chain") or "fetch_chain")

            for attempt_index, attempt_payload in enumerate(tier_attempts, start=1):
                attempt_tier = str(attempt_payload.get("tier") or "fetch_chain")
                success_flag = bool(attempt_payload.get("success"))
                is_selected_attempt = bool(
                    is_selected
                    and success_flag
                    and selected_tier
                    and attempt_tier == selected_tier
                )
                attempt = ArticleFetchAttempt(
                    source_incident_id=source_incident.id,
                    source_incident_url_id=url_row.id,
                    fetch_tier=attempt_tier,
                    attempted_at=now + timedelta(milliseconds=attempt_index - 1),
                    worker_id=worker_id,
                    success=success_flag,
                    http_status=None,
                    latency_ms=attempt_payload.get("latency_ms"),
                    content_length=attempt_payload.get("content_length"),
                    error_code=attempt_payload.get("error_code"),
                    error_message=attempt_payload.get("error_message"),
                    response_metadata={
                        "fetched_url": article.url,
                        "selected_for_enrichment": is_selected_attempt,
                        "attempt_index": attempt_index,
                        "selected_tier": selected_tier,
                    },
                )
                self.article_repository.add_fetch_attempt(session, attempt)

            if existing_document is None:
                existing_document = ArticleDocument(
                    source_incident_id=source_incident.id,
                    source_incident_url_id=url_row.id,
                    title=article.title or None,
                    author=article.author,
                    publish_date=_parse_publish_date(article.publish_date),
                    content_text=article.content,
                    content_hash=hashlib.sha256(article.content.encode("utf-8")).hexdigest(),
                    content_language=None,
                    document_metadata={
                        "source_url": url_row.url,
                        "fetched_url": article.url,
                        "selected_fetch_tier": selected_tier,
                        "fetch_attempt_count": len(tier_attempts),
                    },
                    is_selected_for_enrichment=is_selected,
                    fetched_at=now,
                )
                self.article_repository.add_document(session, existing_document)
            else:
                existing_document.title = article.title or existing_document.title
                existing_document.author = article.author or existing_document.author
                existing_document.publish_date = _parse_publish_date(article.publish_date) or existing_document.publish_date
                existing_document.content_text = article.content
                existing_document.content_hash = hashlib.sha256(article.content.encode("utf-8")).hexdigest()
                existing_document.document_metadata = {
                    **(existing_document.document_metadata or {}),
                    "source_url": url_row.url,
                    "fetched_url": article.url,
                    "selected_fetch_tier": selected_tier,
                    "fetch_attempt_count": len(tier_attempts),
                }
                existing_document.is_selected_for_enrichment = is_selected
                existing_document.fetched_at = now
                self.article_repository.add_document(session, existing_document)

            if is_selected:
                selected_written = True

        enrich_task_enqueued = 0
        if success_count > 0:
            existing_enrich_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="enrich_source",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_enrich_task is None:
                enrich_task = PipelineTask(
                    run_id=None,
                    task_type="enrich_source",
                    target_table="source_incidents",
                    target_id=source_incident.id,
                    status="queued",
                    priority=80,
                    payload={
                        "source_incident_id": str(source_incident.id),
                        "source_name": source_incident.source_name,
                    },
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=5,
                )
                self.pipeline_task_repository.enqueue(session, enrich_task)
                enrich_task_enqueued = 1

        return {
            "urls_total": len(fetchable_urls),
            "articles_saved": success_count,
            "articles_failed": failure_count,
            "enrich_tasks_enqueued": enrich_task_enqueued,
        }
