"""v2 fetch-task processing using the existing article extraction stack."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse
from uuid import uuid4

from sqlalchemy.orm import Session

from src.edu_cti.core.deduplication import normalize_url
from src.edu_cti.pipeline.phase2.storage import ArticleContent, ArticleFetcher
from src.edu_cti_v2.models import (
    ArticleDocument,
    ArticleFetchAttempt,
    PipelineTask,
    SourceIncident,
    SourceIncidentUrl,
)
from src.edu_cti_v2.repositories import (
    ArticleRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
    SourceIncidentRepository,
)
from src.edu_cti_v2.source_identity import recover_source_identity

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
_IDENTITY_ANCHOR_STOP_TOKENS = _STOP_TOKENS | {
    "academy",
    "board",
    "centre",
    "center",
    "community",
    "department",
    "education",
    "institute",
    "institution",
    "joint",
    "office",
    "township",
    "unified",
}
_MIN_SELECTED_ARTICLE_SCORE = 12.0
_BINARY_CONTENT_PREFIXES = ("%PDF-", "PK\x03\x04")
_CYBER_EVIDENCE_RE = re.compile(
    r"\b("
    r"cyber(?:attack| attack|security| security| incident| incident)?|"
    r"ransomware|malware|phishing|breach(?:ed)?|data breach|"
    r"hacker|hackers|hacked|hacking|ddos|denial[- ]of[- ]service|"
    r"unauthori[sz]ed access|unauthori[sz]ed users?|"
    r"security incident|privacy breach|data leak|exfiltrat(?:e|ed|ion)|"
    r"compromis(?:e|ed)|moveit|cl0p|lockbit"
    r")\b",
    re.IGNORECASE,
)
_HOMEPAGEISH_TITLE_RE = re.compile(
    r"\b("
    r"home|welcome|announcements?|newsroom|aktuelles|portrait|about us|"
    r"studieren|campus|events?|veranstaltungen"
    r")\b",
    re.IGNORECASE,
)
_CURATED_SOURCE_NAMES = {"konbriefing", "comparitech"}
_SOURCE_DATE_RELATIVE_GUARD_GROUPS = {"news", "rss"}
_MAX_ARTICLE_DAYS_AFTER_SOURCE = 90
_FALLBACK_DISCOVERY_SOURCE_NAME = "fallback_news_discovery"
_FALLBACK_DISCOVERY_COLLECTOR_VERSION = "fallback-news-discovery-v1"
_EDU_EVIDENCE_RE = re.compile(
    r"\b("
    r"school|schools|school district|district|k-?12|college|colleges|"
    r"university|universities|campus|students?|faculty|academy|"
    r"higher education|student portal|learning management|lms|canvas|"
    r"instructure|powerschool|blackbaud|ellucian|moodle"
    r")\b",
    re.IGNORECASE,
)


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


def _source_title_core(source_incident: SourceIncident) -> str:
    title = (source_incident.raw_title or "").strip()
    match = _TITLE_SOURCE_SUFFIX_RE.search(title)
    if match:
        title = title[: match.start()].strip()
    return title


def _normalize_title_for_exact_match(value: Optional[str]) -> str:
    return " ".join(_TOKEN_RE.findall((value or "").lower()))


def _article_title_strongly_matches_source(
    source_incident: SourceIncident,
    article_title: Optional[str],
) -> bool:
    source_title = _source_title_core(source_incident)
    normalized_source = _normalize_title_for_exact_match(source_title)
    normalized_article = _normalize_title_for_exact_match(article_title)
    if normalized_source and normalized_source == normalized_article:
        return True

    source_tokens = _tokenize(source_title)
    article_tokens = _tokenize(article_title)
    if len(source_tokens) < 4 or not article_tokens:
        return False
    union = source_tokens | article_tokens
    if not union:
        return False
    return len(source_tokens & article_tokens) / len(union) >= 0.85


def _source_identity_anchor_tokens(source_incident: SourceIncident) -> set[str]:
    """Distinct victim-name tokens that should appear in a relevant fetched article."""
    identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    )
    if not identity:
        return set()
    return {
        token
        for token in _TOKEN_RE.findall(identity.lower())
        if len(token) >= 3 and token not in _IDENTITY_ANCHOR_STOP_TOKENS
    }


def _article_mentions_source_identity(
    source_incident: SourceIncident,
    *,
    article: ArticleContent,
    source_url: str,
) -> bool:
    """Avoid selecting SERP/RSS results that only match the incident year/topic."""
    anchor_tokens = _source_identity_anchor_tokens(source_incident)
    if not anchor_tokens:
        return True
    candidate_text = " ".join(
        value
        for value in (
            urlparse(source_url).netloc,
            unquote(urlparse(source_url).path),
            article.title or "",
            (article.content or "")[:2400],
        )
        if value
    )
    candidate_tokens = _tokenize(candidate_text)
    return bool(anchor_tokens & candidate_tokens)


def _source_year_hint(source_incident: SourceIncident) -> Optional[int]:
    if source_incident.source_published_at is not None:
        return source_incident.source_published_at.year
    value = (source_incident.raw_incident_date or "").strip()
    if len(value) >= 4 and value[:4].isdigit():
        return int(value[:4])
    return None


def _source_date_relative_guard_applies(source_incident: SourceIncident) -> bool:
    return (
        source_incident.source_published_at is not None
        and str(source_incident.source_group or "").strip().lower()
        in _SOURCE_DATE_RELATIVE_GUARD_GROUPS
    )


def _article_publish_date_after_source_window(
    source_incident: SourceIncident,
    publish_date: Optional[str],
) -> bool:
    if not _source_date_relative_guard_applies(source_incident):
        return False
    parsed = _parse_publish_date(publish_date)
    if parsed is None:
        return False
    source_date = source_incident.source_published_at.date()
    return (parsed - source_date).days > _MAX_ARTICLE_DAYS_AFTER_SOURCE


def _extract_url_year(url: str) -> Optional[int]:
    match = _URL_YEAR_RE.search(urlparse(url).path or "")
    if not match:
        return None
    return int(match.group(1))


def _url_year_after_source_window(source_incident: SourceIncident, url: str) -> bool:
    if not _source_date_relative_guard_applies(source_incident):
        return False
    url_year = _extract_url_year(url)
    if url_year is None:
        return False
    # Use Jan 1 as a conservative lower bound for URL-year-only evidence.
    # If even Jan 1 is too far after the source date, the discovered article
    # cannot be the same news item or a near-term follow-up.
    earliest_url_year_date = date(url_year, 1, 1)
    source_date = source_incident.source_published_at.date()
    return (earliest_url_year_date - source_date).days > _MAX_ARTICLE_DAYS_AFTER_SOURCE


def _source_requires_cyber_evidence(source_incident: SourceIncident) -> bool:
    """Return true when the source metadata says this row should be a cyber incident."""
    metadata_text = " ".join(
        value
        for value in (
            source_incident.raw_title,
            source_incident.raw_subtitle,
            source_incident.raw_attack_hint,
            source_incident.raw_notes,
        )
        if value
    )
    return bool(_CYBER_EVIDENCE_RE.search(metadata_text))


def _article_has_cyber_evidence(article: ArticleContent) -> bool:
    candidate_text = " ".join(
        value
        for value in (
            article.title,
            (article.content or "")[:3000],
            article.url,
        )
        if value
    )
    return bool(_CYBER_EVIDENCE_RE.search(candidate_text))


def _article_has_education_evidence(article: ArticleContent) -> bool:
    candidate_text = " ".join(
        value
        for value in (
            article.title,
            (article.content or "")[:3000],
            article.url,
        )
        if value
    )
    return bool(_EDU_EVIDENCE_RE.search(candidate_text))


def _looks_like_current_homepage(article: ArticleContent, source_url: str) -> bool:
    parsed = urlparse(source_url)
    path = (parsed.path or "/").strip().lower()
    title = article.title or ""
    if path in {"", "/", "/home", "/home/", "/index.php", "/index.html", "/announcements", "/announcements/"}:
        return True
    return bool(_HOMEPAGEISH_TITLE_RE.search(title))


def _domain_matches_publisher(url: str, publisher_hint: Optional[str]) -> bool:
    if not publisher_hint:
        return False
    publisher_tokens = _tokenize(publisher_hint)
    if not publisher_tokens:
        return False
    host = (urlparse(url).netloc or "").lower()
    return all(token in host for token in publisher_tokens if token not in {"the", "news"})


def _score_url_candidate(source_incident: SourceIncident, url: str) -> float:
    if _url_year_after_source_window(source_incident, url):
        return -100.0

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
    if (
        _article_publish_date_after_source_window(source_incident, article.publish_date)
        and not _article_title_strongly_matches_source(source_incident, article.title)
    ):
        return -100.0
    if not article.publish_date and _url_year_after_source_window(source_incident, source_url):
        return -100.0

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
    if not _article_mentions_source_identity(
        source_incident,
        article=article,
        source_url=source_url,
    ):
        score -= 40.0

    if _source_requires_cyber_evidence(source_incident) and not _article_has_cyber_evidence(article):
        # Current homepages often retain the victim name but have lost the incident notice.
        # They should not become the selected enrichment article for historical rows.
        score -= 45.0 if _looks_like_current_homepage(article, source_url) else 30.0

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


def _fallback_discovery_event_key(article_url: str) -> str:
    normalized = normalize_url(article_url) or article_url.strip()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"article:{digest}"


def _fallback_source_published_at(article: ArticleContent) -> Optional[datetime]:
    parsed = _parse_publish_date(article.publish_date)
    if parsed is None:
        return None
    return datetime.combine(parsed, time.min, tzinfo=timezone.utc)


def _should_promote_drift_candidate(
    *,
    source_incident: SourceIncident,
    article: ArticleContent,
    source_url: str,
) -> bool:
    if (source_incident.source_name or "").lower() == _FALLBACK_DISCOVERY_SOURCE_NAME:
        return False
    if not article.fetch_successful or not article.content:
        return False
    if not _article_has_cyber_evidence(article):
        return False
    if not _article_has_education_evidence(article):
        return False
    if _looks_like_current_homepage(article, source_url):
        return False
    article_url = (article.url or source_url or "").strip()
    if not article_url.startswith(("http://", "https://")):
        return False
    return bool(normalize_url(article_url))


def _should_retry_discovery_after_low_quality_selection(
    source_incident: SourceIncident,
    selected_candidates: list[dict[str, Any]],
) -> bool:
    """Ask discovery for alternatives when trusted source URLs fetched stale pages."""
    if not selected_candidates:
        return False
    if (source_incident.source_name or "").lower() not in _CURATED_SOURCE_NAMES:
        return False
    if not _source_requires_cyber_evidence(source_incident):
        return False
    return bool(
        recover_source_identity(
            raw_institution_name=source_incident.raw_institution_name,
            raw_victim_name=source_incident.raw_victim_name,
            raw_subtitle=source_incident.raw_subtitle,
            raw_title=source_incident.raw_title,
        )
    )


def _should_retry_discovery_after_fetch_failures(
    source_incident: SourceIncident,
    *,
    fetchable_url_count: int,
    success_count: int,
    failure_count: int,
) -> bool:
    """Ask discovery for alternatives when trusted source URLs are unreachable."""
    if fetchable_url_count <= 0 or failure_count <= 0 or success_count > 0:
        return False
    if (source_incident.source_name or "").lower() not in _CURATED_SOURCE_NAMES:
        return False
    if not _source_requires_cyber_evidence(source_incident):
        return False
    return bool(
        recover_source_identity(
            raw_institution_name=source_incident.raw_institution_name,
            raw_victim_name=source_incident.raw_victim_name,
            raw_subtitle=source_incident.raw_subtitle,
            raw_title=source_incident.raw_title,
        )
    )


def _attempt_response_metadata(
    *,
    article: ArticleContent,
    attempt_payload: dict[str, Any],
    attempt_index: int,
    selected_tier: Optional[str],
    selected_for_enrichment: bool,
) -> dict[str, Any]:
    metadata = {
        "fetched_url": article.url,
        "selected_for_enrichment": selected_for_enrichment,
        "attempt_index": attempt_index,
        "selected_tier": selected_tier,
    }
    for key in ("raw_content_length", "extracted_content_length", "low_content_reason"):
        if key in attempt_payload and attempt_payload.get(key) is not None:
            metadata[key] = attempt_payload.get(key)
    return metadata


def _sanitize_db_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    sanitized = value.replace("\x00", "")
    return sanitized or None


def _validate_article_content(
    article: ArticleContent,
    tier_attempts: list[dict[str, Any]],
) -> tuple[ArticleContent, list[dict[str, Any]]]:
    sanitized_title = _sanitize_db_text(article.title) or ""
    sanitized_author = _sanitize_db_text(article.author)
    sanitized_content = _sanitize_db_text(article.content) or ""
    stripped = sanitized_content.lstrip()

    if any(stripped.startswith(prefix) for prefix in _BINARY_CONTENT_PREFIXES):
        selected_tier = (
            article.fetch_metadata.get("selected_tier")
            if isinstance(article.fetch_metadata, dict)
            else None
        ) or str((tier_attempts[-1].get("tier") if tier_attempts else "fetch_chain") or "fetch_chain")
        normalized_attempts: list[dict[str, Any]] = []
        for attempt in tier_attempts:
            attempt_copy = dict(attempt)
            if str(attempt_copy.get("tier") or "") == selected_tier:
                attempt_copy["success"] = False
                attempt_copy["content_length"] = 0
                attempt_copy["error_code"] = "binary_content"
                attempt_copy["error_message"] = "binary_content_detected"
            normalized_attempts.append(attempt_copy)
        return (
            ArticleContent(
                url=article.url,
                title=sanitized_title,
                content="",
                author=sanitized_author,
                publish_date=article.publish_date,
                fetch_successful=False,
                error_message="binary_content_detected",
                content_length=0,
                fetch_metadata=article.fetch_metadata,
            ),
            normalized_attempts,
        )

    return (
        ArticleContent(
            url=article.url,
            title=sanitized_title,
            content=sanitized_content,
            author=sanitized_author,
            publish_date=article.publish_date,
            fetch_successful=article.fetch_successful,
            error_message=article.error_message,
            content_length=len(sanitized_content),
            fetch_metadata=article.fetch_metadata,
        ),
        tier_attempts,
    )


class V2FetchService:
    """Handles `fetch_article` tasks for source incidents."""

    def __init__(
        self,
        *,
        article_fetcher: Optional[ArticleFetcher] = None,
        article_repository: Optional[ArticleRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.article_fetcher = article_fetcher
        self.article_repository = article_repository or ArticleRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.source_enrichment_repository = (
            source_enrichment_repository or SourceEnrichmentRepository()
        )
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def _article_fetcher(self) -> ArticleFetcher:
        if self.article_fetcher is None:
            self.article_fetcher = ArticleFetcher()
        return self.article_fetcher

    def _promote_drift_candidate(
        self,
        session: Session,
        source_incident: SourceIncident,
        *,
        candidate: dict[str, Any],
        worker_id: str,
        now: datetime,
    ) -> bool:
        article = candidate["article"]
        source_url = candidate["source_url"]
        if not _should_promote_drift_candidate(
            source_incident=source_incident,
            article=article,
            source_url=source_url,
        ):
            return False

        article_url = (article.url or source_url).strip()
        normalized_url = normalize_url(article_url)
        if not normalized_url:
            return False
        event_key = _fallback_discovery_event_key(article_url)
        existing = self.source_incident_repository.get_by_source_event_key(
            session,
            _FALLBACK_DISCOVERY_SOURCE_NAME,
            event_key,
        )
        if existing is not None:
            return False

        source_published_at = _fallback_source_published_at(article)
        source_id = uuid4()
        url_id = uuid4()
        document_id = uuid4()
        source_title = _sanitize_db_text(article.title) or article_url
        content_hash = hashlib.sha256(article.content.encode("utf-8")).hexdigest()
        origin_payload = {
            "generated_by": _FALLBACK_DISCOVERY_COLLECTOR_VERSION,
            "origin_source_incident_id": str(source_incident.id),
            "origin_source_name": source_incident.source_name,
            "origin_source_event_key": source_incident.source_event_key,
            "origin_raw_title": source_incident.raw_title,
            "origin_source_url": source_url,
            "discovered_article_url": article_url,
            "selection_score_for_origin": float(candidate["score"]),
            "reason": "fetched_article_did_not_match_origin_source_but_looks_like_education_cyber_incident",
        }
        generated_source = SourceIncident(
            id=source_id,
            source_name=_FALLBACK_DISCOVERY_SOURCE_NAME,
            source_group="rss",
            source_event_key=event_key,
            collector_version=_FALLBACK_DISCOVERY_COLLECTOR_VERSION,
            collected_at=now,
            source_published_at=source_published_at,
            raw_title=source_title,
            raw_subtitle=None,
            raw_victim_name=None,
            raw_institution_name=None,
            raw_institution_type=None,
            raw_country=None,
            raw_region=None,
            raw_city=None,
            raw_incident_date=source_published_at.date().isoformat() if source_published_at else None,
            raw_date_precision="day" if source_published_at else "unknown",
            raw_status="open",
            raw_attack_hint="candidate education cyber incident",
            raw_threat_actor=None,
            raw_notes=(
                "discovered_via=fallback_article_drift; "
                f"origin_source={source_incident.source_name}; "
                f"origin_source_incident_id={source_incident.id}"
            ),
            source_confidence="low",
            ingest_hash=event_key,
            raw_payload=origin_payload,
            is_deleted=False,
        )
        url_row = SourceIncidentUrl(
            id=url_id,
            source_incident_id=source_id,
            url=article_url,
            normalized_url=normalized_url,
            resolved_url=article_url,
            url_kind="article",
            is_wrapper=False,
            is_primary_from_source=True,
            is_resolved_primary=True,
            created_at=now,
        )
        generated_source.urls = [url_row]
        document = ArticleDocument(
            id=document_id,
            source_incident_id=source_id,
            source_incident_url_id=url_id,
            title=source_title,
            author=article.author,
            publish_date=_parse_publish_date(article.publish_date),
            content_text=article.content,
            content_hash=content_hash,
            content_language=None,
            document_metadata={
                "source_url": article_url,
                "fetched_url": article_url,
                "selected_fetch_tier": "fallback_discovery_reuse",
                "generated_from_drifted_article": True,
                "origin_source_incident_id": str(source_incident.id),
                "origin_source_name": source_incident.source_name,
                "origin_source_url": source_url,
            },
            is_selected_for_enrichment=True,
            fetched_at=now,
        )
        attempt = ArticleFetchAttempt(
            source_incident_id=source_id,
            source_incident_url_id=url_id,
            fetch_tier="fallback_discovery_reuse",
            attempted_at=now,
            worker_id=worker_id,
            success=True,
            http_status=None,
            latency_ms=None,
            content_length=len(article.content),
            error_code=None,
            error_message=None,
            response_metadata={
                "fetched_url": article_url,
                "selected_for_enrichment": True,
                "attempt_index": 1,
                "selected_tier": "fallback_discovery_reuse",
                "origin_source_incident_id": str(source_incident.id),
                "origin_article_document_id": str(candidate["document"].id),
            },
        )
        self.source_incident_repository.add(session, generated_source)
        # The fallback article/document rows are created with explicit FK ids
        # instead of ORM relationships, so flush the generated parent first.
        session.flush()
        self.article_repository.add_document(session, document)
        self.article_repository.add_fetch_attempt(session, attempt)

        if self.pipeline_task_repository.get_active_for_target(
            session,
            task_type="enrich_source",
            target_table="source_incidents",
            target_id=source_id,
        ) is None:
            self.pipeline_task_repository.enqueue(
                session,
                PipelineTask(
                    run_id=None,
                    task_type="enrich_source",
                    target_table="source_incidents",
                    target_id=source_id,
                    status="queued",
                    priority=75,
                    payload={
                        "source_incident_id": str(source_id),
                        "source_name": _FALLBACK_DISCOVERY_SOURCE_NAME,
                        "trigger": "fallback_article_drift_candidate",
                        "origin_source_incident_id": str(source_incident.id),
                    },
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=5,
                ),
            )
        return True

    def promote_existing_unselected_document_as_drift_candidate(
        self,
        session: Session,
        source_incident: SourceIncident,
        document: ArticleDocument,
        *,
        worker_id: str = "data-quality-drift-sweep",
        now: Optional[datetime] = None,
    ) -> bool:
        metadata = document.document_metadata if isinstance(document.document_metadata, dict) else {}
        source_url = str(metadata.get("source_url") or metadata.get("fetched_url") or "")
        article_url = str(metadata.get("fetched_url") or metadata.get("source_url") or source_url)
        article = ArticleContent(
            url=article_url,
            title=document.title or "",
            content=document.content_text or "",
            author=document.author,
            publish_date=document.publish_date.isoformat() if document.publish_date else None,
            fetch_successful=bool(document.content_text),
            error_message=None,
            content_length=len(document.content_text or ""),
            fetch_metadata={
                "selected_tier": metadata.get("selected_fetch_tier") or "existing_document",
            },
        )
        candidate = {
            "document": document,
            "attempts": [],
            "article": article,
            "source_url": source_url or article_url,
            "selected_tier": "existing_document",
            "score": float(metadata.get("selection_score_for_origin") or -1.0),
        }
        return self._promote_drift_candidate(
            session,
            source_incident,
            candidate=candidate,
            worker_id=worker_id,
            now=now or datetime.now(timezone.utc),
        )

    def fetch_articles_for_source_incident(
        self,
        session: Session,
        source_incident: SourceIncident,
        *,
        worker_id: str,
        force_refetch: bool = False,
    ) -> Dict[str, int]:
        existing_enrichment = self.source_enrichment_repository.get_by_source_incident(
            session,
            source_incident.id,
        )
        # force_refetch re-fetches the article so the (improved) extractor re-derives
        # publish_date in place, then re-enriches — used to repair dates corpus-wide.
        had_enrichment = existing_enrichment is not None
        if existing_enrichment is not None and not force_refetch:
            return {
                "urls_total": 0,
                "articles_saved": 0,
                "articles_failed": 0,
                "enrich_tasks_enqueued": 0,
                "resolve_tasks_enqueued": 0,
                "skipped_already_enriched": 1,
            }

        existing_selected_document = self.article_repository.get_selected_document(
            session,
            source_incident.id,
        )
        if existing_selected_document is not None and not force_refetch:
            enrich_task_enqueued = 0
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
                        priority=80,
                        payload={
                            "source_incident_id": str(source_incident.id),
                            "source_name": source_incident.source_name,
                            "trigger": "existing_selected_article",
                        },
                        result={},
                        available_at=datetime.now(timezone.utc),
                        attempt_count=0,
                        max_attempts=5,
                    ),
                )
                enrich_task_enqueued = 1
            return {
                "urls_total": 0,
                "articles_saved": 0,
                "articles_failed": 0,
                "enrich_tasks_enqueued": enrich_task_enqueued,
                "resolve_tasks_enqueued": 0,
                "skipped_existing_selected_article": 1,
            }

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
            article = self._article_fetcher().fetch_article(url_row.url)
            tier_attempts = _extract_tier_attempts(article)
            article, tier_attempts = _validate_article_content(article, tier_attempts)
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
                        response_metadata=_attempt_response_metadata(
                            article=article,
                            attempt_payload=attempt_payload,
                            attempt_index=attempt_index,
                            selected_tier=(
                                article.fetch_metadata.get("selected_tier")
                                if isinstance(article.fetch_metadata, dict)
                                else None
                            ),
                            selected_for_enrichment=False,
                        ),
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
                    response_metadata=_attempt_response_metadata(
                        article=article,
                        attempt_payload=attempt_payload,
                        attempt_index=attempt_index,
                        selected_tier=selected_tier,
                        selected_for_enrichment=False,
                    ),
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
                _new_publish = _parse_publish_date(article.publish_date)
                # On a force re-fetch, trust the re-extracted date even if it is now
                # None (null beats a previously-wrong "today"); otherwise keep prior.
                existing_document.publish_date = (
                    _new_publish if force_refetch else (_new_publish or existing_document.publish_date)
                )
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
                    "article": article,
                    "source_url": url_row.url,
                    "selected_tier": selected_tier,
                    "score": _score_article_candidate(
                        source_incident,
                        article=article,
                        source_url=url_row.url,
                    ),
                }
            )

        enrich_task_enqueued = 0
        resolve_task_enqueued = 0
        drift_candidates_created = 0
        if selected_candidates:
            for candidate in selected_candidates:
                document_metadata = dict(candidate["document"].document_metadata or {})
                document_metadata["selection_score_for_origin"] = float(candidate["score"])
                document_metadata["selection_threshold_for_origin"] = _MIN_SELECTED_ARTICLE_SCORE
                candidate["document"].document_metadata = document_metadata
            # Deterministic selection: highest score, then longer body (more
            # complete extraction), then lexical URL. A bare max() on score alone
            # returns the first maximal element, so equal-scoring candidates were
            # selected by list order, making enrichment non-reproducible.
            def _selection_key(candidate: dict) -> tuple:
                document = candidate["document"]
                content_length = len(document.content_text or "")
                return (
                    float(candidate["score"]),
                    content_length,
                    str(candidate.get("source_url") or ""),
                )

            best_candidate = max(selected_candidates, key=_selection_key)
            best_score = float(best_candidate["score"])
            # On a force re-fetch we are refreshing the SAME article that was already
            # chosen for this incident — select it regardless of score. Otherwise the
            # year-mismatch penalty (a re-extracted historical date disagreeing with a
            # previously-corrupted source year) would reject the corrected article and
            # the date repair would never happen.
            if best_score >= _MIN_SELECTED_ARTICLE_SCORE or force_refetch:
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

        has_selected_candidate = any(
            candidate["document"].is_selected_for_enrichment
            for candidate in selected_candidates
        )
        if selected_candidates and not has_selected_candidate:
            for candidate in selected_candidates:
                if self._promote_drift_candidate(
                    session,
                    source_incident,
                    candidate=candidate,
                    worker_id=worker_id,
                    now=now,
                ):
                    drift_candidates_created += 1

        if has_selected_candidate:
            # On a force re-fetch of an already-enriched incident, re-enrich (which
            # overwrites the enrichment and re-canonicalizes) rather than enrich_source
            # (which would be skipped as already-enriched).
            enrich_task_type = "reenrich" if (force_refetch and had_enrichment) else "enrich_source"
            existing_enrich_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type=enrich_task_type,
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_enrich_task is None:
                enrich_payload = {
                    "source_incident_id": str(source_incident.id),
                    "source_name": source_incident.source_name,
                }
                if enrich_task_type == "reenrich":
                    enrich_payload["re_enrich_reason"] = "force_refetch_date_fix"
                enrich_task = PipelineTask(
                    run_id=None,
                    task_type=enrich_task_type,
                    target_table="source_incidents",
                    target_id=source_incident.id,
                    status="queued",
                    priority=80,
                    payload=enrich_payload,
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=5,
                )
                self.pipeline_task_repository.enqueue(session, enrich_task)
                enrich_task_enqueued = 1
        elif _should_retry_discovery_after_low_quality_selection(
            source_incident,
            selected_candidates,
        ) or _should_retry_discovery_after_fetch_failures(
            source_incident,
            fetchable_url_count=len(fetchable_urls),
            success_count=success_count,
            failure_count=failure_count,
        ):
            reason = (
                "no_relevant_article_selected"
                if selected_candidates
                else "all_fetch_tiers_failed"
            )
            existing_resolve_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="resolve_url",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_resolve_task is None:
                resolve_task = PipelineTask(
                    run_id=None,
                    task_type="resolve_url",
                    target_table="source_incidents",
                    target_id=source_incident.id,
                    status="queued",
                    priority=70,
                    payload={
                        "source_incident_id": str(source_incident.id),
                        "source_name": source_incident.source_name,
                        "force_discovery": True,
                        "reason": reason,
                    },
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=3,
                )
                self.pipeline_task_repository.enqueue(session, resolve_task)
                resolve_task_enqueued = 1

        result = {
            "urls_total": len(fetchable_urls),
            "articles_saved": success_count,
            "articles_failed": failure_count,
            "enrich_tasks_enqueued": enrich_task_enqueued,
            "resolve_tasks_enqueued": resolve_task_enqueued,
        }
        if drift_candidates_created:
            result["drift_candidates_created"] = drift_candidates_created
        return result
