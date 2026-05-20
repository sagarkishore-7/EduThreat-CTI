"""v2 URL resolution services for source incidents."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Callable, Dict, List, Optional
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from src.edu_cti.core.deduplication import is_google_news_wrapper_url, normalize_url
from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti.sources.rss.googlenews_rss import _resolve_google_news_article_url
from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp
from src.edu_cti_v2.models import PipelineTask, SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository
from src.edu_cti_v2.services.fetching import _score_url_candidate

_INVALID_DISCOVERY_NAMES = {
    "",
    "?",
    "-",
    "unknown",
    "unknown institution",
    "n/a",
    "none",
    "unnamed",
    "undisclosed",
    "not disclosed",
}
_MIN_DISCOVERED_URL_SCORE = 4.0
_MAX_DISCOVERED_URLS = 5
_BLOCKED_DISCOVERY_HOSTS = (
    "wikipedia.org",
    "threads.com",
    "instagram.com",
    "facebook.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "tiktok.com",
    "youtube.com",
    "reddit.com",
    "intellibot.app",
)
_COLLECTIVE_DISCOVERY_RE = re.compile(
    r"^(?:\d+\s+)?(?:universities|colleges|schools|school districts?|districts|campuses|providers|students)\b",
    re.IGNORECASE,
)
_COMMENTARY_DISCOVERY_RE = re.compile(
    r"^(?:the\s+cyber\s+threat\s+to|who\s+are|what\s+are|old-school|cyber\s+threat\s+to)\b",
    re.IGNORECASE,
)


def _clean_discovery_name(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered in _INVALID_DISCOVERY_NAMES:
        return None
    if not any(char.isalnum() for char in text):
        return None
    return text


def _normalize_identity_for_comparison(value: Optional[str]) -> Optional[str]:
    text = (value or "").strip().lower()
    if not text:
        return None
    return " ".join(text.split())


def _filter_source_institution_name(source_incident: SourceIncident) -> Optional[str]:
    institution_name = _clean_discovery_name(source_incident.raw_institution_name)
    if not institution_name:
        return None
    if _normalize_identity_for_comparison(institution_name) == _normalize_identity_for_comparison(source_incident.raw_title):
        return None
    if is_headline_format(institution_name, source_incident.raw_title):
        return None
    if _COLLECTIVE_DISCOVERY_RE.match(institution_name):
        return None
    if _COMMENTARY_DISCOVERY_RE.match(institution_name):
        return None
    return institution_name


def _is_disallowed_discovered_url(url: str) -> bool:
    host = (urlparse(url).netloc or "").lower()
    return any(host == blocked or host.endswith(f".{blocked}") for blocked in _BLOCKED_DISCOVERY_HOSTS)


def _build_discovered_article_row(
    *,
    source_incident: SourceIncident,
    url: str,
    created_at: datetime,
    is_primary_from_source: bool,
) -> Optional[SourceIncidentUrl]:
    normalized = normalize_url(url)
    if not normalized:
        return None
    return SourceIncidentUrl(
        source_incident_id=source_incident.id,
        url=url,
        normalized_url=normalized,
        resolved_url=url,
        url_kind="article",
        is_wrapper=False,
        is_primary_from_source=is_primary_from_source,
        is_resolved_primary=is_primary_from_source,
        created_at=created_at,
    )


def _resolve_google_wrapper_urls(source_incident: SourceIncident) -> list[tuple[Optional[SourceIncidentUrl], str]]:
    candidates: list[tuple[Optional[SourceIncidentUrl], str]] = []
    seen_wrappers: set[str] = set()

    for row in source_incident.urls or []:
        wrapper_url = (row.url or "").strip()
        if not wrapper_url or not is_google_news_wrapper_url(wrapper_url) or wrapper_url in seen_wrappers:
            continue
        seen_wrappers.add(wrapper_url)
        candidates.append((row, wrapper_url))

    source_event_key = (source_incident.source_event_key or "").strip()
    if source_event_key and is_google_news_wrapper_url(source_event_key) and source_event_key not in seen_wrappers:
        candidates.append((None, source_event_key))

    resolved_pairs: list[tuple[Optional[SourceIncidentUrl], str]] = []
    for wrapper_row, wrapper_url in candidates:
        resolved_url = _resolve_google_news_article_url(wrapper_url)
        if not resolved_url or is_google_news_wrapper_url(resolved_url):
            continue
        resolved_pairs.append((wrapper_row, resolved_url))
    return resolved_pairs


def source_incident_to_discovery_payload(source_incident: SourceIncident) -> Dict[str, object]:
    """Map a v2 source incident into the news-discovery payload shape."""
    institution_name = _filter_source_institution_name(source_incident)
    victim_raw_name = _clean_discovery_name(source_incident.raw_victim_name)
    return {
        "incident_id": str(source_incident.id),
        "institution_name": institution_name,
        "victim_raw_name": victim_raw_name,
        "title": source_incident.raw_title,
        "attack_type_hint": source_incident.raw_attack_hint,
        "incident_date": source_incident.raw_incident_date,
    }


class V2ResolveUrlService:
    """Resolve dedicated article URLs for source incidents that only have wrappers/stubs."""

    def __init__(
        self,
        *,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        article_discovery: Optional[Callable[[Dict[str, object]], List[str]]] = None,
    ) -> None:
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.article_discovery = article_discovery or discover_articles_via_serp

    def resolve_source_incident_urls(
        self,
        session: Session,
        source_incident: SourceIncident,
        *,
        force_discovery: bool = False,
    ) -> Dict[str, int]:
        existing_urls = list(source_incident.urls or [])
        existing_normalized = {row.normalized_url for row in existing_urls}
        existing_fetchable = [row for row in existing_urls if row.url_kind == "article" and not row.is_wrapper]
        had_existing_fetchable = bool(existing_fetchable)

        wrapper_resolutions = _resolve_google_wrapper_urls(source_incident)
        direct_urls_discovered = 0
        added_count = 0
        now = datetime.now(timezone.utc)

        for wrapper_row, resolved_url in wrapper_resolutions:
            normalized = normalize_url(resolved_url)
            if not normalized or normalized in existing_normalized:
                if wrapper_row is not None and not wrapper_row.resolved_url:
                    wrapper_row.resolved_url = resolved_url
                    if wrapper_row.is_primary_from_source:
                        wrapper_row.is_resolved_primary = True
                continue

            is_primary = bool(wrapper_row.is_primary_from_source) if wrapper_row is not None else not existing_fetchable and added_count == 0
            article_row = _build_discovered_article_row(
                source_incident=source_incident,
                url=resolved_url,
                created_at=now,
                is_primary_from_source=is_primary and not existing_fetchable and added_count == 0,
            )
            if article_row is None:
                continue
            source_incident.urls.append(article_row)
            session.add(article_row)
            existing_normalized.add(article_row.normalized_url)
            if wrapper_row is not None:
                wrapper_row.resolved_url = resolved_url
                if article_row.is_primary_from_source:
                    wrapper_row.is_resolved_primary = True
            existing_fetchable.append(article_row)
            direct_urls_discovered += 1
            added_count += 1

        discovered_urls: list[str] = []
        if force_discovery or not existing_fetchable:
            discovered_urls = self.article_discovery(source_incident_to_discovery_payload(source_incident))
        ranked_urls = sorted(
            (
                (url, _score_url_candidate(source_incident, url))
                for url in discovered_urls
                if not _is_disallowed_discovered_url(url)
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        filtered_urls = [
            url
            for url, score in ranked_urls
            if score >= _MIN_DISCOVERED_URL_SCORE
        ][: _MAX_DISCOVERED_URLS]
        for url in filtered_urls:
            normalized = normalize_url(url)
            if not normalized or normalized in existing_normalized:
                continue
            row = _build_discovered_article_row(
                source_incident=source_incident,
                url=url,
                created_at=now,
                is_primary_from_source=not existing_fetchable and added_count == 0,
            )
            if row is None:
                continue
            source_incident.urls.append(row)
            session.add(row)
            existing_normalized.add(normalized)
            added_count += 1
            existing_fetchable.append(row)

        fetch_task_enqueued = 0
        should_enqueue_fetch = bool(existing_fetchable or added_count > 0)
        if force_discovery and had_existing_fetchable:
            # Forced discovery usually follows a fetch pass that found only stale/low-quality
            # pages. Do not loop back into fetch unless discovery actually added a new URL.
            should_enqueue_fetch = added_count > 0

        if should_enqueue_fetch:
            existing_fetch_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="fetch_article",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_fetch_task is None:
                task = PipelineTask(
                    run_id=None,
                    task_type="fetch_article",
                    target_table="source_incidents",
                    target_id=source_incident.id,
                    status="queued",
                    priority=60,
                    payload={
                        "source_incident_id": str(source_incident.id),
                        "source_name": source_incident.source_name,
                        "resolved_via": "serp",
                    },
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=5,
                )
                self.pipeline_task_repository.enqueue(session, task)
                fetch_task_enqueued = 1

        return {
            "urls_discovered": direct_urls_discovered + len(discovered_urls),
            "urls_added": added_count,
            "fetch_tasks_enqueued": fetch_task_enqueued,
        }
