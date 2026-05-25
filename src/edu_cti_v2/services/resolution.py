"""v2 URL resolution services for source incidents."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Callable, Dict, List, Optional
from urllib.parse import urlparse

from sqlalchemy.orm import Session

from src.edu_cti.core.deduplication import is_google_news_wrapper_url, normalize_url
from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti.sources.rss.googlenews_rss import _resolve_google_news_article_url
from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp
from src.edu_cti_v2.models import ArticleDocument, PipelineTask, SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.repositories import ArticleRepository, PipelineTaskRepository, SourceIncidentRepository
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


def _structured_source_url_row(source_incident: SourceIncident) -> Optional[SourceIncidentUrl]:
    for kind in ("detail", "leak_site", "screenshot", "other"):
        for row in source_incident.urls or []:
            if row.url_kind == kind:
                return row
    return None


def _should_use_structured_source_evidence(
    source_incident: SourceIncident,
    *,
    has_fetchable_article: bool,
) -> bool:
    if has_fetchable_article:
        return False
    if source_incident.source_group != "api":
        return False
    if source_incident.source_name not in {"ransomwarelive", "ransomlook"}:
        return False
    return bool(source_incident.raw_payload or _structured_source_url_row(source_incident))


def _structured_source_content(source_incident: SourceIncident) -> str:
    raw_payload = source_incident.raw_payload if isinstance(source_incident.raw_payload, dict) else {}
    nested_payload = raw_payload.get("raw_source_payload")
    if not isinstance(nested_payload, dict):
        nested_payload = {}

    fields = [
        ("Source", source_incident.source_name),
        ("Title", source_incident.raw_title),
        ("Institution", source_incident.raw_institution_name or source_incident.raw_victim_name),
        ("Institution type", source_incident.raw_institution_type),
        ("Country", source_incident.raw_country),
        ("Region", source_incident.raw_region),
        ("City", source_incident.raw_city),
        ("Incident date", source_incident.raw_incident_date),
        ("Date precision", source_incident.raw_date_precision),
        ("Status", source_incident.raw_status),
        ("Attack hint", source_incident.raw_attack_hint),
        ("Threat actor", source_incident.raw_threat_actor),
        ("Subtitle", source_incident.raw_subtitle),
        ("Notes", source_incident.raw_notes),
        ("Leak site URL", next((row.url for row in source_incident.urls or [] if row.url_kind == "leak_site"), None)),
        ("Detail URL", next((row.url for row in source_incident.urls or [] if row.url_kind == "detail"), None)),
        ("Victim website", nested_payload.get("website") or raw_payload.get("victim_website")),
        ("Activity", nested_payload.get("activity") or raw_payload.get("activity")),
        ("Description", nested_payload.get("description") or raw_payload.get("description")),
    ]
    lines = [f"{name}: {value}" for name, value in fields if value]
    if raw_payload:
        lines.append("Structured source payload:")
        lines.append(json.dumps(raw_payload, ensure_ascii=False, sort_keys=True))
    return "\n".join(lines)


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
        article_repository: Optional[ArticleRepository] = None,
        article_discovery: Optional[Callable[[Dict[str, object]], List[str]]] = None,
    ) -> None:
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.article_repository = article_repository or ArticleRepository()
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
        enrich_task_enqueued = 0
        structured_document_created = 0
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

        if (
            not should_enqueue_fetch
            and _should_use_structured_source_evidence(
                source_incident,
                has_fetchable_article=bool(existing_fetchable),
            )
        ):
            selected_document = self.article_repository.get_selected_document(
                session,
                source_incident.id,
            )
            source_url_row = _structured_source_url_row(source_incident)
            source_url = (
                (source_url_row.resolved_url or source_url_row.url)
                if source_url_row is not None
                else f"structured-source:{source_incident.source_name}:{source_incident.id}"
            )
            if selected_document is None:
                content_text = _structured_source_content(source_incident)
                document = ArticleDocument(
                    source_incident_id=source_incident.id,
                    source_incident_url_id=source_url_row.id if source_url_row is not None else None,
                    title=source_incident.raw_title,
                    author=source_incident.source_name,
                    publish_date=(
                        source_incident.source_published_at.date()
                        if source_incident.source_published_at
                        else None
                    ),
                    content_text=content_text,
                    content_hash=hashlib.sha256(content_text.encode("utf-8")).hexdigest(),
                    content_language=None,
                    document_metadata={
                        "fetch_tier": "structured_source",
                        "structured_source": True,
                        "source_url": source_url,
                        "fetched_url": source_url,
                    },
                    is_selected_for_enrichment=True,
                    fetched_at=now,
                )
                self.article_repository.add_document(session, document)
                structured_document_created = 1

            existing_enrich_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="enrich_source",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_enrich_task is None:
                self.pipeline_task_repository.enqueue(
                    session,
                    PipelineTask(
                        run_id=None,
                        task_type="enrich_source",
                        target_table="source_incidents",
                        target_id=source_incident.id,
                        status="queued",
                        priority=85,
                        payload={
                            "source_incident_id": str(source_incident.id),
                            "source_name": source_incident.source_name,
                            "trigger": "structured_source_evidence",
                        },
                        result={},
                        available_at=now,
                        attempt_count=0,
                        max_attempts=5,
                    ),
                )
                enrich_task_enqueued = 1

        result = {
            "urls_discovered": direct_urls_discovered + len(discovered_urls),
            "urls_added": added_count,
            "fetch_tasks_enqueued": fetch_task_enqueued,
        }
        if structured_document_created or enrich_task_enqueued:
            result["structured_documents_created"] = structured_document_created
            result["enrich_tasks_enqueued"] = enrich_task_enqueued
        return result
