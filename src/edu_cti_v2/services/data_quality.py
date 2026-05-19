"""Data-quality sweep and re-enrichment helpers for the v2 runtime."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy.orm import Session

from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti_v2.models import PipelineTask, SourceEnrichment
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceEnrichmentRepository, SourceIncidentRepository
from src.edu_cti_v2.source_identity import looks_geographic_only_identity

MIN_DATE = date(1990, 1, 1)
FUTURE_TOLERANCE_DAYS = 3
MAX_REENRICH_ATTEMPTS = 3

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T ]|$)")
_GENERIC_EDU_ENTITY_RE = (
    r"(?:university|college|school|academy|institute|polytechnic|library|district|"
    r"school district|community college|technical college|research university|research institute)"
)
_GENERIC_INSTITUTION_RE = re.compile(
    r"^(?:(?:a|an)\s+)?"
    r"(?:public\s+|private\s+|state\s+|local\s+|regional\s+|major\s+|leading\s+)?"
    rf"(?:{_GENERIC_EDU_ENTITY_RE})(?:\s+{_GENERIC_EDU_ENTITY_RE})*"
    r"(?:\s+in\b.*)?$",
    re.IGNORECASE,
)


def _today_plus_buffer() -> date:
    return date.today() + timedelta(days=FUTURE_TOLERANCE_DAYS)


def _is_safe_date(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    if not _ISO_DATE_RE.match(text):
        return False
    try:
        parsed = date.fromisoformat(text[:10])
    except (TypeError, ValueError):
        return False
    if parsed < MIN_DATE:
        return False
    if parsed > _today_plus_buffer():
        return False
    return True


def _iter_timeline_dates(payload: dict[str, Any] | None) -> list[str]:
    timeline = []
    if isinstance(payload, dict):
        raw = payload.get("timeline")
        if isinstance(raw, list):
            timeline = raw
    dates: list[str] = []
    for item in timeline:
        if isinstance(item, dict) and item.get("date"):
            dates.append(str(item["date"]))
    return dates


def _looks_generic_institution(text: Any) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    if looks_geographic_only_identity(value):
        return True
    if _GENERIC_INSTITUTION_RE.match(value):
        return True
    lowered = value.lower()
    words = value.split()
    if lowered.startswith(("several ", "multiple ", "various ", "few ", "many ", "some ")):
        return True
    if "website of" in lowered or "websites of" in lowered:
        return True
    if re.search(r"\b(?:few|several|multiple|various|many|some)\s+(?:colleges?|schools?|universities?|districts?)\b", value, re.IGNORECASE):
        return True
    if len(words) >= 10:
        return True
    if value.endswith("?"):
        return True
    if len(words) >= 6 and any(punct in value for punct in (":", ";")):
        return True
    return False


def _candidate_institution_name(enrichment: SourceEnrichment, source_incident) -> Optional[str]:
    for payload in (enrichment.typed_enrichment, enrichment.raw_extraction):
        if isinstance(payload, dict):
            for key in ("institution_name", "institution_name_en", "vendor_name"):
                value = payload.get(key)
                if value:
                    return str(value)
    return source_incident.raw_institution_name or source_incident.raw_victim_name or source_incident.raw_title


def _diagnose_source_enrichment(enrichment: SourceEnrichment, source_incident) -> Optional[str]:
    reasons: list[str] = []
    typed = enrichment.typed_enrichment if isinstance(enrichment.typed_enrichment, dict) else {}
    raw = enrichment.raw_extraction if isinstance(enrichment.raw_extraction, dict) else {}

    incident_date = typed.get("incident_date") or raw.get("incident_date") or source_incident.raw_incident_date
    if not _is_safe_date(incident_date):
        reasons.append(f"incident_date={incident_date!r}")

    source_published = (
        typed.get("source_published_date")
        or raw.get("source_published_date")
        or (source_incident.source_published_at.date().isoformat() if source_incident.source_published_at else None)
    )
    if not _is_safe_date(source_published):
        reasons.append(f"source_published_date={source_published!r}")

    discovery_date = typed.get("discovery_date") or raw.get("discovery_date")
    if not _is_safe_date(discovery_date):
        reasons.append(f"discovery_date={discovery_date!r}")

    timeline_dates = [value for value in (_iter_timeline_dates(typed) + _iter_timeline_dates(raw)) if not _is_safe_date(value)]
    if timeline_dates:
        reasons.append(f"timeline_dates={timeline_dates[:3]!r}")

    institution_name = _candidate_institution_name(enrichment, source_incident)
    title = source_incident.raw_title
    if institution_name and is_headline_format(institution_name, title):
        reasons.append(f"institution_name_looks_like_headline={institution_name!r}")
    elif institution_name and _looks_generic_institution(institution_name):
        reasons.append(f"institution_name_too_generic={institution_name!r}")

    return "; ".join(reasons) if reasons else None


class V2DataQualityService:
    """Sweep v2 source enrichments for bad dates and headline-style victim names."""

    def __init__(
        self,
        *,
        session_factory: Optional[Callable] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.session_factory = session_factory
        self.source_enrichment_repository = source_enrichment_repository or SourceEnrichmentRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def sweep_invalid_source_enrichments(
        self,
        session: Session,
        *,
        limit: int | None = None,
    ) -> dict[str, Any]:
        candidates = self.source_enrichment_repository.list_for_quality_sweep(session, limit=limit)
        requeued = 0
        already_queued = 0
        flagged = 0
        cleared = 0
        skipped_missing = 0

        now = datetime.now(timezone.utc)
        for enrichment in candidates:
            source_incident = self.source_incident_repository.get_by_id(session, enrichment.source_incident_id)
            if source_incident is None:
                skipped_missing += 1
                continue

            reason = _diagnose_source_enrichment(enrichment, source_incident)
            if not reason:
                if (
                    enrichment.re_enrich_attempts
                    or enrichment.re_enrich_reason
                    or enrichment.manual_review_required
                    or enrichment.manual_review_reason
                ):
                    enrichment.re_enrich_attempts = 0
                    enrichment.re_enrich_reason = None
                    enrichment.manual_review_required = False
                    enrichment.manual_review_reason = None
                    self.source_enrichment_repository.add(session, enrichment)
                    cleared += 1
                continue

            attempts = int(enrichment.re_enrich_attempts or 0) + 1
            enrichment.re_enrich_attempts = attempts
            enrichment.re_enrich_reason = reason

            if attempts >= MAX_REENRICH_ATTEMPTS:
                enrichment.manual_review_required = True
                enrichment.manual_review_reason = reason
                self.source_enrichment_repository.add(session, enrichment)
                flagged += 1
                continue

            enrichment.manual_review_required = False
            enrichment.manual_review_reason = None
            self.source_enrichment_repository.add(session, enrichment)

            existing_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="reenrich",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_task is not None:
                already_queued += 1
                continue

            self.pipeline_task_repository.enqueue(
                session,
                PipelineTask(
                    run_id=None,
                    task_type="reenrich",
                    target_table="source_incidents",
                    target_id=source_incident.id,
                    status="queued",
                    priority=160,
                    payload={
                        "source_incident_id": str(source_incident.id),
                        "source_name": source_incident.source_name,
                        "re_enrich_attempts": attempts,
                        "re_enrich_reason": reason,
                    },
                    result={},
                    available_at=now,
                    attempt_count=0,
                    max_attempts=MAX_REENRICH_ATTEMPTS,
                ),
            )
            requeued += 1

        return {
            "scanned": len(candidates),
            "requeued_for_reenrichment": requeued,
            "already_queued": already_queued,
            "flagged_for_manual_review": flagged,
            "cleared_clean_state": cleared,
            "skipped_missing_source_incidents": skipped_missing,
            "max_reenrich_attempts": MAX_REENRICH_ATTEMPTS,
            "checked_at": now.isoformat(),
        }

    def run_sweep(self, *, limit: int | None = None) -> dict[str, Any]:
        if self.session_factory is None:
            raise RuntimeError("session_factory is required for run_sweep")
        with self.session_factory() as session:
            result = self.sweep_invalid_source_enrichments(session, limit=limit)
            session.commit()
            return result

    def list_manual_review_queue(
        self,
        session: Session,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for enrichment in self.source_enrichment_repository.list_manual_review_queue(session, limit=limit):
            source_incident = self.source_incident_repository.get_by_id(session, enrichment.source_incident_id)
            items.append(
                {
                    "source_incident_id": str(enrichment.source_incident_id),
                    "source_name": source_incident.source_name if source_incident else None,
                    "title": source_incident.raw_title if source_incident else None,
                    "institution_name": _candidate_institution_name(enrichment, source_incident) if source_incident else None,
                    "manual_review_reason": enrichment.manual_review_reason,
                    "re_enrich_attempts": int(enrichment.re_enrich_attempts or 0),
                    "updated_at": enrichment.updated_at.isoformat() if enrichment.updated_at else None,
                }
            )
        return items
