"""v2 fetch-task processing using the existing article extraction stack."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Dict, Optional

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
            attempt = ArticleFetchAttempt(
                source_incident_id=source_incident.id,
                source_incident_url_id=url_row.id,
                fetch_tier="fetch_chain",
                attempted_at=now,
                worker_id=worker_id,
                success=article.fetch_successful,
                http_status=None,
                latency_ms=None,
                content_length=article.content_length,
                error_code=None,
                error_message=article.error_message,
                response_metadata={
                    "fetched_url": article.url,
                    "selected_for_enrichment": False,
                },
            )
            self.article_repository.add_fetch_attempt(session, attempt)

            if not article.fetch_successful or not article.content:
                failure_count += 1
                continue

            success_count += 1
            existing_document = self.article_repository.get_document_by_source_url(session, url_row.id)
            is_selected = not selected_written

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
