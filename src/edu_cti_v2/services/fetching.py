"""v2 fetch-task processing using the existing article extraction stack."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse

from sqlalchemy.orm import Session

from src.edu_cti.pipeline.phase2.storage import ArticleContent, ArticleFetcher
from src.edu_cti_v2.models import ArticleDocument, ArticleFetchAttempt, PipelineTask, SourceIncident
from src.edu_cti_v2.repositories import ArticleRepository, PipelineTaskRepository

_TITLE_SOURCE_SUFFIX_RE = re.compile(r"\s+-\s+([^-\n]+)$")
_SOURCE_NOTE_RE = re.compile(r"(?:^|;)\s*source=([^;]+)")
_URL_YEAR_RE = re.compile(r"/(20\d{2})(?:/|$)")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOP_TOKENS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "after",
    "over",
    "into",
    "amid",
    "says",
    "said",
    "what",
    "have",
    "been",
    "will",
    "they",
    "their",
    "area",
    "county",
    "school",
    "schools",
    "college",
    "colleges",
    "student",
    "students",
    "district",
    "system",
    "public",
    "university",
    "attack",
    "cyberattack",
    "cyber",
    "data",
    "breach",
    "ransomware",
    "hacked",
}
_MIN_SELECTED_ARTICLE_SCORE = 12.0


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


def _tokenize(value: Optional[str]) -> set[str]:
    text = (value or "").lower()
    return {
        token
        for token in _TOKEN_RE.findall(text)
        if len(token) >= 3 and token not in _STOP_TOKENS
    }


def _extract_publisher_hint(source_incident: SourceIncident) -> Optional[str]:
    title = (source_incident.raw_title or "").strip()
    match = _TITLE_SOURCE_SUFFIX_RE.search(title)
    if match:
        return match.group(1).strip()
    notes = (source_incident.raw_notes or "").strip()
    note_match = _SOURCE_NOTE_RE.search(notes)
    if note_match:
        return note_match.group(1).strip()
    return None


def _source_reference_tokens(source_incident: SourceIncident) -> set[str]:
    tokens: set[str] = set()
    title = (source_incident.raw_title or "").strip()
    match = _TITLE_SOURCE_SUFFIX_RE.search(title)
    if match:
        title = title[: match.start()].strip()
    for value in (
        title,
        source_incident.raw_subtitle,
        source_incident.raw_institution_name,
        source_incident.raw_victim_name,
    ):
        tokens.update(_tokenize(value))
    return tokens


def _source_year_hint(source_incident: SourceIncident) -> Optional[int]:
    if source_incident.source_published_at is not None:
        return source_incident.source_published_at.year
    value = (source_incident.raw_incident_date or "").strip()
    if len(value) >= 4 and value[:4].isdigit():
        return int(value[:4])
    return None


def _extract_url_year(url: str) -> Optional[int]:
    match = _URL_YEAR_RE.search(urlparse(url).path or "")
    if not match:
        return None
    return int(match.group(1))


def _domain_matches_publisher(url: str, publisher_hint: Optional[str]) -> bool:
    if not publisher_hint:
        return False
    publisher_tokens = _tokenize(publisher_hint)
    if not publisher_tokens:
        return False
    host = (urlparse(url).netloc or "").lower()
    return all(token in host for token in publisher_tokens if token not in {"the", "news"})


def _score_url_candidate(source_incident: SourceIncident, url: str) -> float:
    score = 0.0
    publisher_hint = _extract_publisher_hint(source_incident)
    if _domain_matches_publisher(url, publisher_hint):
        score += 18.0

    source_tokens = _source_reference_tokens(source_incident)
    url_text = f"{urlparse(url).netloc} {unquote(urlparse(url).path)}"
    url_tokens = _tokenize(url_text)
    overlap = source_tokens & url_tokens
    score += min(len(overlap) * 4.0, 24.0)

    source_year = _source_year_hint(source_incident)
    url_year = _extract_url_year(url)
    if source_year and url_year:
        if source_year == url_year:
            score += 10.0
        elif abs(source_year - url_year) >= 2:
            score -= 12.0

    return score


def _score_article_candidate(
    source_incident: SourceIncident,
    *,
    article: ArticleContent,
    source_url: str,
) -> float:
    score = _score_url_candidate(source_incident, source_url)
    source_tokens = _source_reference_tokens(source_incident)
    article_title_tokens = _tokenize(article.title)
    preview_tokens = _tokenize((article.content or "")[:1200])

    title_overlap = source_tokens & article_title_tokens
    preview_overlap = source_tokens & preview_tokens
    score += min(len(title_overlap) * 5.0, 30.0)
    score += min(len(preview_overlap) * 2.5, 15.0)

    if source_tokens and not title_overlap and not preview_overlap:
        score -= 10.0

    source_year = _source_year_hint(source_incident)
    publish_year = _parse_publish_date(article.publish_date)
    publish_year = publish_year.year if publish_year else None
    if source_year and publish_year:
        if source_year == publish_year:
            score += 12.0
        elif abs(source_year - publish_year) >= 2:
            score -= 16.0

    if _domain_matches_publisher(article.url or source_url, _extract_publisher_hint(source_incident)):
        score += 10.0

    return score


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
        fetchable_urls.sort(key=lambda row: _score_url_candidate(source_incident, row.url), reverse=True)

        now = datetime.now(timezone.utc)
        success_count = 0
        failure_count = 0
        selected_candidates: list[dict[str, Any]] = []

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
            selected_tier = (
                article.fetch_metadata.get("selected_tier")
                if isinstance(article.fetch_metadata, dict)
                else None
            ) or str((tier_attempts[-1].get("tier") if tier_attempts else "fetch_chain") or "fetch_chain")

            attempt_records: list[ArticleFetchAttempt] = []
            for attempt_index, attempt_payload in enumerate(tier_attempts, start=1):
                attempt_tier = str(attempt_payload.get("tier") or "fetch_chain")
                success_flag = bool(attempt_payload.get("success"))
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
                        "selected_for_enrichment": False,
                        "attempt_index": attempt_index,
                        "selected_tier": selected_tier,
                    },
                )
                self.article_repository.add_fetch_attempt(session, attempt)
                attempt_records.append(attempt)

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
                    is_selected_for_enrichment=False,
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
                existing_document.is_selected_for_enrichment = False
                existing_document.fetched_at = now
                self.article_repository.add_document(session, existing_document)

            selected_candidates.append(
                {
                    "document": existing_document,
                    "attempts": attempt_records,
                    "selected_tier": selected_tier,
                    "score": _score_article_candidate(
                        source_incident,
                        article=article,
                        source_url=url_row.url,
                    ),
                }
            )

        enrich_task_enqueued = 0
        if selected_candidates:
            best_candidate = max(selected_candidates, key=lambda candidate: float(candidate["score"]))
            best_score = float(best_candidate["score"])
            if best_score >= _MIN_SELECTED_ARTICLE_SCORE:
                for candidate in selected_candidates:
                    is_selected_candidate = candidate is best_candidate
                    candidate["document"].is_selected_for_enrichment = is_selected_candidate
                    if not is_selected_candidate:
                        continue
                    for attempt in candidate["attempts"]:
                        response_metadata = dict(attempt.response_metadata or {})
                        if attempt.success and attempt.fetch_tier == candidate["selected_tier"]:
                            response_metadata["selected_for_enrichment"] = True
                        attempt.response_metadata = response_metadata
            else:
                for candidate in selected_candidates:
                    candidate["document"].is_selected_for_enrichment = False

        if any(candidate["document"].is_selected_for_enrichment for candidate in selected_candidates):
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
