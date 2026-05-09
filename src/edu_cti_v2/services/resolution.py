"""v2 URL resolution services for source incidents."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from sqlalchemy.orm import Session

from src.edu_cti.core.deduplication import normalize_url
from src.edu_cti.pipeline.phase2.utils.fetching_strategy import discover_articles_via_serp
from src.edu_cti_v2.models import PipelineTask, SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository

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


def source_incident_to_discovery_payload(source_incident: SourceIncident) -> Dict[str, object]:
    """Map a v2 source incident into the SERP discovery payload shape."""
    institution_name = _clean_discovery_name(source_incident.raw_institution_name)
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
    ) -> Dict[str, int]:
        existing_urls = list(source_incident.urls or [])
        existing_normalized = {row.normalized_url for row in existing_urls}
        existing_fetchable = [row for row in existing_urls if row.url_kind == "article" and not row.is_wrapper]

        discovered_urls = self.article_discovery(source_incident_to_discovery_payload(source_incident))
        added_count = 0
        now = datetime.now(timezone.utc)

        for url in discovered_urls:
            normalized = normalize_url(url)
            if not normalized or normalized in existing_normalized:
                continue
            row = SourceIncidentUrl(
                source_incident_id=source_incident.id,
                url=url,
                normalized_url=normalized,
                resolved_url=url,
                url_kind="article",
                is_wrapper=False,
                is_primary_from_source=not existing_fetchable and added_count == 0,
                is_resolved_primary=not existing_fetchable and added_count == 0,
                created_at=now,
            )
            source_incident.urls.append(row)
            session.add(row)
            existing_normalized.add(normalized)
            added_count += 1

        fetch_task_enqueued = 0
        if existing_fetchable or added_count > 0:
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
                    priority=40,
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
            "urls_discovered": len(discovered_urls),
            "urls_added": added_count,
            "fetch_tasks_enqueued": fetch_task_enqueued,
        }
