"""Optional Phase 1 dual-write bridge into the Postgres-backed v2 source tables."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy.orm import Session, sessionmaker

from src.edu_cti.core.deduplication import is_google_news_wrapper_url, normalize_url
from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.sources import (
    API_SOURCE_REGISTRY,
    CURATED_SOURCE_REGISTRY,
    NEWS_SOURCE_REGISTRY,
    PAID_RSS_SOURCE_REGISTRY,
    RSS_SOURCE_REGISTRY,
)
from src.edu_cti_v2.db.connection import create_session_factory
from src.edu_cti_v2.models import SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.repositories import SourceIncidentRepository
from src.edu_cti_v2.services import V2IntakeService

logger = logging.getLogger(__name__)

_PHASE1_DUAL_WRITE_ENV = "EDU_CTI_V2_PHASE1_DUAL_WRITE"


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def is_phase1_dual_write_enabled() -> bool:
    """Return True when Phase 1 should also write raw observations into v2."""
    return _env_flag(_PHASE1_DUAL_WRITE_ENV, "0")


def _build_source_group_index() -> Dict[str, str]:
    source_group_map: Dict[str, str] = {}
    for source_name in CURATED_SOURCE_REGISTRY:
        source_group_map[source_name] = "curated"
    for source_name in NEWS_SOURCE_REGISTRY:
        source_group_map[source_name] = "news"
    for source_name in RSS_SOURCE_REGISTRY:
        source_group_map[source_name] = "rss"
    for source_name in PAID_RSS_SOURCE_REGISTRY:
        source_group_map[source_name] = "rss"
    for source_name in API_SOURCE_REGISTRY:
        source_group_map[source_name] = "api"
    return source_group_map


_SOURCE_GROUP_BY_NAME = _build_source_group_index()


def classify_source_group(source_name: str) -> str:
    """Return the registered group for a source name, defaulting to rss."""
    return _SOURCE_GROUP_BY_NAME.get(source_name, "rss")


def parse_datetime_like(value: Optional[str]) -> Optional[datetime]:
    """Parse common v1 incident date/datetime strings into UTC datetimes."""
    if not value:
        return None

    text = value.strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = None

    if parsed is not None:
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    try:
        parsed_date = date.fromisoformat(text)
    except ValueError:
        return None

    return datetime.combine(parsed_date, time.min, tzinfo=timezone.utc)


def _incident_payload(incident: BaseIncident, event_key: str) -> dict:
    payload = incident.to_dict()
    payload["source_event_key"] = event_key
    payload["v1_incident_id"] = incident.incident_id
    payload["discovery_date"] = incident.discovery_date
    payload["threat_actor"] = incident.threat_actor
    return payload


def build_source_incident_record(incident: BaseIncident, event_key: str) -> SourceIncident:
    """Map a BaseIncident into the v2 source observation model."""
    collected_at = parse_datetime_like(incident.ingested_at) or datetime.now(timezone.utc)
    source_published_at = parse_datetime_like(incident.source_published_date)

    return SourceIncident(
        source_name=incident.source,
        source_group=classify_source_group(incident.source),
        source_event_key=event_key,
        collector_version=os.environ.get("EDU_CTI_V2_COLLECTOR_VERSION"),
        collected_at=collected_at,
        source_published_at=source_published_at,
        raw_title=incident.title,
        raw_subtitle=incident.subtitle,
        raw_victim_name=incident.victim_raw_name,
        raw_institution_name=incident.institution_name,
        raw_institution_type=incident.institution_type,
        raw_country=incident.country,
        raw_region=incident.region,
        raw_city=incident.city,
        raw_incident_date=incident.incident_date,
        raw_date_precision=incident.date_precision,
        raw_status=incident.status,
        raw_attack_hint=incident.attack_type_hint,
        raw_threat_actor=incident.threat_actor,
        raw_notes=incident.notes,
        source_confidence=incident.source_confidence,
        ingest_hash=incident.incident_id,
        raw_payload=_incident_payload(incident, event_key),
    )


def _url_tuple(
    *,
    url: str,
    url_kind: str,
    created_at: datetime,
    is_primary_from_source: bool,
) -> Optional[Tuple[str, SourceIncidentUrl]]:
    stripped = (url or "").strip()
    if not stripped:
        return None

    normalized = normalize_url(stripped)
    if not normalized:
        normalized = stripped

    is_wrapper = is_google_news_wrapper_url(stripped)
    return normalized, SourceIncidentUrl(
        url=stripped,
        normalized_url=normalized,
        resolved_url=None if is_wrapper else stripped,
        url_kind=url_kind,
        is_wrapper=is_wrapper,
        is_primary_from_source=is_primary_from_source,
        is_resolved_primary=is_primary_from_source and not is_wrapper,
        created_at=created_at,
    )


def build_source_incident_urls(
    incident: BaseIncident,
    *,
    created_at: Optional[datetime] = None,
) -> List[SourceIncidentUrl]:
    """Build classified URL rows for a raw source incident."""
    created_at = created_at or (parse_datetime_like(incident.ingested_at) or datetime.now(timezone.utc))
    urls: Dict[str, SourceIncidentUrl] = {}

    primary_source_url = None
    if incident.primary_url:
        primary_source_url = incident.primary_url.strip()
    elif incident.all_urls:
        primary_source_url = (incident.all_urls[0] or "").strip() or None

    def _add(url: Optional[str], url_kind: str) -> None:
        if not url:
            return
        built = _url_tuple(
            url=url,
            url_kind=url_kind,
            created_at=created_at,
            is_primary_from_source=((url or "").strip() == primary_source_url),
        )
        if not built:
            return
        normalized, row = built
        if normalized not in urls:
            urls[normalized] = row

    for url in incident.all_urls or []:
        _add(url, "rss_wrapper" if is_google_news_wrapper_url(url) else "article")

    _add(incident.primary_url, "rss_wrapper" if is_google_news_wrapper_url(incident.primary_url or "") else "article")
    _add(incident.source_detail_url, "detail")
    _add(incident.leak_site_url, "leak_site")
    _add(incident.screenshot_url, "screenshot")

    return list(urls.values())


def merge_source_incident_urls(
    existing: SourceIncident,
    new_rows: Iterable[SourceIncidentUrl],
) -> None:
    """Append any new URL rows onto an existing source incident."""
    existing_rows = list(existing.urls or [])
    normalized_index = {row.normalized_url: row for row in existing_rows}

    for row in new_rows:
        current = normalized_index.get(row.normalized_url)
        if current is None:
            existing_rows.append(row)
            normalized_index[row.normalized_url] = row
            continue

        if not current.resolved_url and row.resolved_url:
            current.resolved_url = row.resolved_url
        current.is_primary_from_source = current.is_primary_from_source or row.is_primary_from_source
        current.is_resolved_primary = current.is_resolved_primary or row.is_resolved_primary
        if current.url_kind == "article" and row.url_kind != "article":
            current.url_kind = row.url_kind

    existing.urls = existing_rows


class V2Phase1DualWriter:
    """Best-effort bridge that stores raw Phase 1 observations into v2."""

    def __init__(
        self,
        session_factory: Optional[sessionmaker] = None,
        repository: Optional[SourceIncidentRepository] = None,
        intake_service: Optional[V2IntakeService] = None,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository or SourceIncidentRepository()
        self._intake_service = intake_service or V2IntakeService()

    @property
    def session_factory(self) -> sessionmaker:
        if self._session_factory is None:
            self._session_factory = create_session_factory()
        return self._session_factory

    def write_observation(self, incident: BaseIncident, event_key: str) -> Optional[str]:
        if not is_phase1_dual_write_enabled():
            return None

        session: Session = self.session_factory()
        try:
            existing = self._repository.get_by_source_event_key(session, incident.source, event_key)
            if existing is None:
                source_incident = build_source_incident_record(incident, event_key)
                source_incident.urls = build_source_incident_urls(incident, created_at=source_incident.collected_at)
                self._repository.add(session, source_incident)
                session.flush()
                self._intake_service.record_incremental_state(session, source_incident)
                self._intake_service.ensure_initial_processing_task(session, source_incident)
                session.commit()
                return source_incident.id

            existing.source_published_at = existing.source_published_at or parse_datetime_like(
                incident.source_published_date
            )
            existing.raw_title = existing.raw_title or incident.title
            existing.raw_subtitle = existing.raw_subtitle or incident.subtitle
            existing.raw_victim_name = existing.raw_victim_name or incident.victim_raw_name
            existing.raw_institution_name = existing.raw_institution_name or incident.institution_name
            existing.raw_institution_type = existing.raw_institution_type or incident.institution_type
            existing.raw_country = existing.raw_country or incident.country
            existing.raw_region = existing.raw_region or incident.region
            existing.raw_city = existing.raw_city or incident.city
            existing.raw_incident_date = existing.raw_incident_date or incident.incident_date
            existing.raw_date_precision = existing.raw_date_precision or incident.date_precision
            existing.raw_status = existing.raw_status or incident.status
            existing.raw_attack_hint = existing.raw_attack_hint or incident.attack_type_hint
            existing.raw_threat_actor = existing.raw_threat_actor or incident.threat_actor
            existing.raw_notes = existing.raw_notes or incident.notes
            existing.source_confidence = existing.source_confidence or incident.source_confidence
            existing.raw_payload = _incident_payload(incident, event_key)
            merge_source_incident_urls(
                existing,
                build_source_incident_urls(
                    incident,
                    created_at=parse_datetime_like(incident.ingested_at) or datetime.now(timezone.utc),
                ),
            )
            session.flush()
            self._intake_service.record_incremental_state(session, existing)
            self._intake_service.ensure_initial_processing_task(session, existing)
            session.commit()
            return existing.id
        except Exception:
            session.rollback()
            logger.exception(
                "Phase 1 v2 dual-write failed for %s (%s)",
                incident.incident_id,
                incident.source,
            )
            return None
        finally:
            session.close()


_PHASE1_DUAL_WRITER: Optional[V2Phase1DualWriter] = None


def get_phase1_dual_writer() -> V2Phase1DualWriter:
    global _PHASE1_DUAL_WRITER
    if _PHASE1_DUAL_WRITER is None:
        _PHASE1_DUAL_WRITER = V2Phase1DualWriter()
    return _PHASE1_DUAL_WRITER


def write_phase1_source_observation(incident: BaseIncident, event_key: str) -> Optional[str]:
    """Write a raw Phase 1 observation into v2 when dual-write is enabled."""
    return get_phase1_dual_writer().write_observation(incident, event_key)
