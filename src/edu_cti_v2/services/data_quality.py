"""Data-quality sweep and re-enrichment helpers for the v2 runtime."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti_v2.models import ArticleDocument, PipelineTask, SourceEnrichment, SourceIncident
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceEnrichmentRepository, SourceIncidentRepository
from src.edu_cti_v2.services.fetching import V2FetchService
from src.edu_cti_v2.source_identity import looks_broad_collective_identity, looks_geographic_only_identity

MIN_DATE = date(1990, 1, 1)
FUTURE_TOLERANCE_DAYS = 3
MAX_REENRICH_ATTEMPTS = 3
SOURCE_DATE_RELATIVE_GUARD_GROUPS = {"news", "rss"}
FALLBACK_NEWS_DISCOVERY_SOURCE_NAME = "fallback_news_discovery"

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[T ]|$)")
_GENERIC_EDU_ENTITY_RE = (
    r"(?:university|college|school|academy|institute|polytechnic|library|district|"
    r"school district|community college|technical college|research university|"
    r"research institute|health center)"
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


def _is_safe_incident_date_for_source(value: Any, source_published: Any) -> bool:
    if value is None or not str(value).strip():
        return True
    if source_published is None or not str(source_published).strip():
        return _is_safe_date(value)
    if not _is_safe_date(value) or not _is_safe_date(source_published):
        return False
    incident_date = date.fromisoformat(str(value)[:10])
    published_date = date.fromisoformat(str(source_published)[:10])
    return (incident_date - published_date).days <= 90


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
    if looks_broad_collective_identity(value):
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

    # "Defaulted to today/collection date" pollution: for discovery/search sources,
    # an incident_date equal to the collection date is almost always a missed
    # article publish date (the old behaviour), not a genuine same-day disclosure.
    collected_day = (
        source_incident.collected_at.date().isoformat()
        if getattr(source_incident, "collected_at", None) is not None
        else None
    )
    # An incident_date equal to the collection date is only suspicious when NO real
    # article publication date backs it — that is the "defaulted to today" signature.
    # A genuine same-day incident has a real extracted publication_date (== today),
    # so it is NOT flagged.
    publication_date = (
        typed.get("publication_date")
        or raw.get("publication_date")
        or typed.get("source_published_date")
        or raw.get("source_published_date")
    )
    if (
        collected_day is not None
        and _is_safe_date(incident_date)
        and str(incident_date)[:10] == collected_day
        and not _is_safe_date(publication_date)
        and str(source_incident.source_group or "").strip().lower()
        in SOURCE_DATE_RELATIVE_GUARD_GROUPS
    ):
        reasons.append(f"incident_date_defaulted_to_collection_date={incident_date!r}")

    source_published = (
        typed.get("source_published_date")
        or raw.get("source_published_date")
        or (source_incident.source_published_at.date().isoformat() if source_incident.source_published_at else None)
    )
    if not _is_safe_date(source_published):
        reasons.append(f"source_published_date={source_published!r}")
    elif (
        str(source_incident.source_group or "").strip().lower()
        in SOURCE_DATE_RELATIVE_GUARD_GROUPS
        and not _is_safe_incident_date_for_source(incident_date, source_published)
    ):
        reasons.append(
            f"incident_date_after_source_published_date={incident_date!r}>{source_published!r}"
        )

    discovery_date = typed.get("discovery_date") or raw.get("discovery_date")
    if not _is_safe_date(discovery_date):
        reasons.append(f"discovery_date={discovery_date!r}")

    timeline_dates = [
        value
        for value in (_iter_timeline_dates(typed) + _iter_timeline_dates(raw))
        if not _is_safe_date(value)
        or (
            str(source_incident.source_group or "").strip().lower()
            in SOURCE_DATE_RELATIVE_GUARD_GROUPS
            and not _is_safe_incident_date_for_source(value, source_published)
        )
    ]
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
        fetch_service: Optional[V2FetchService] = None,
    ) -> None:
        self.session_factory = session_factory
        self.source_enrichment_repository = source_enrichment_repository or SourceEnrichmentRepository()
        self.source_incident_repository = source_incident_repository or SourceIncidentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.fetch_service = fetch_service or V2FetchService(
            article_fetcher=None,
            source_incident_repository=self.source_incident_repository,
            source_enrichment_repository=self.source_enrichment_repository,
            pipeline_task_repository=self.pipeline_task_repository,
        )

    def sweep_invalid_source_enrichments(
        self,
        session: Session,
        *,
        limit: int | None = None,
    ) -> dict[str, Any]:
        candidates = self.source_enrichment_repository.list_for_quality_sweep(session, limit=limit)
        requeued = 0
        already_queued = 0
        canonicalize_requeued = 0
        canonicalize_already_queued = 0
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
                existing_canonicalize_task = self.pipeline_task_repository.get_active_for_target(
                    session,
                    task_type="canonicalize",
                    target_table="source_incidents",
                    target_id=source_incident.id,
                )
                if existing_canonicalize_task is not None:
                    canonicalize_already_queued += 1
                    continue
                self.pipeline_task_repository.enqueue(
                    session,
                    PipelineTask(
                        run_id=None,
                        task_type="canonicalize",
                        target_table="source_incidents",
                        target_id=source_incident.id,
                        status="queued",
                        priority=115,
                        payload={
                            "source_incident_id": str(source_incident.id),
                            "source_name": source_incident.source_name,
                            "trigger": "manual_review_quality_sweep",
                            "manual_review_reason": reason,
                        },
                        result={},
                        available_at=now,
                        attempt_count=0,
                        max_attempts=5,
                    ),
                )
                canonicalize_requeued += 1
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
            "requeued_for_canonical_cleanup": canonicalize_requeued,
            "canonical_cleanup_already_queued": canonicalize_already_queued,
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

    def normalize_actor_names(
        self, session: Session, *, limit: int | None = None
    ) -> dict[str, Any]:
        """Re-apply actor normalization to stored canonical `threat_actor_name` values.

        The threat-actor analytics already normalize at read time, but the raw column
        still carries generic/junk labels (`criminal`, `Russian cyber-extortion`, …) and
        un-canonicalised aliases. This recomputes `normalize_threat_actor_name` for every
        open canonical incident and writes the result back — nulling generic labels and
        collapsing aliases to their canonical form — so the stored data matches what the
        UI shows. Idempotent: re-running it is a no-op once clean."""
        from src.edu_cti_v2.models import CanonicalIncident
        from src.edu_cti_v2.normalization import normalize_threat_actor_name

        stmt = (
            select(CanonicalIncident)
            .where(CanonicalIncident.status == "open")
            .where(CanonicalIncident.threat_actor_name.is_not(None))
            .where(CanonicalIncident.threat_actor_name != "")
        )
        if limit is not None:
            stmt = stmt.limit(limit)

        scanned = 0
        nulled = 0
        renamed = 0
        unchanged = 0
        samples: list[dict[str, Any]] = []
        for incident in session.execute(stmt).scalars():
            scanned += 1
            original = incident.threat_actor_name
            normalized = normalize_threat_actor_name(original)
            if normalized == original:
                unchanged += 1
                continue
            incident.threat_actor_name = normalized
            if normalized is None:
                nulled += 1
            else:
                renamed += 1
            if len(samples) < 25:
                samples.append({"from": original, "to": normalized})

        return {
            "scanned": scanned,
            "nulled": nulled,
            "renamed": renamed,
            "unchanged": unchanged,
            "changed": nulled + renamed,
            "samples": samples,
        }

    def run_actor_normalization(self, *, limit: int | None = None) -> dict[str, Any]:
        if self.session_factory is None:
            raise RuntimeError("session_factory is required for run_actor_normalization")
        with self.session_factory() as session:
            result = self.normalize_actor_names(session, limit=limit)
            session.commit()
            return result

    def promote_drifted_unselected_articles(
        self,
        session: Session,
        *,
        limit: int = 500,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        stmt = (
            select(ArticleDocument, SourceIncident)
            .join(SourceIncident, SourceIncident.id == ArticleDocument.source_incident_id)
            .where(ArticleDocument.is_selected_for_enrichment.is_(False))
            .where(SourceIncident.source_name != FALLBACK_NEWS_DISCOVERY_SOURCE_NAME)
            .order_by(ArticleDocument.fetched_at.desc())
            .limit(limit)
        )
        scanned = 0
        promoted = 0
        skipped_source_already_has_selected_article = 0
        skipped_not_candidate = 0
        skipped_already_enriched = 0
        for document, source_incident in session.execute(stmt).all():
            scanned += 1
            if self.source_enrichment_repository.get_by_source_incident(
                session,
                source_incident.id,
            ) is not None:
                skipped_already_enriched += 1
                continue
            selected_exists = session.execute(
                select(ArticleDocument.id)
                .where(ArticleDocument.source_incident_id == source_incident.id)
                .where(ArticleDocument.is_selected_for_enrichment.is_(True))
                .limit(1)
            ).first()
            if selected_exists is not None:
                skipped_source_already_has_selected_article += 1
                continue
            if self.fetch_service.promote_existing_unselected_document_as_drift_candidate(
                session,
                source_incident,
                document,
                now=now,
            ):
                promoted += 1
            else:
                skipped_not_candidate += 1

        return {
            "scanned": scanned,
            "promoted": promoted,
            "skipped_already_enriched": skipped_already_enriched,
            "skipped_source_already_has_selected_article": skipped_source_already_has_selected_article,
            "skipped_not_candidate": skipped_not_candidate,
            "checked_at": now.isoformat(),
        }

    def run_drift_promotion_sweep(self, *, limit: int = 500) -> dict[str, Any]:
        if self.session_factory is None:
            raise RuntimeError("session_factory is required for run_drift_promotion_sweep")
        with self.session_factory() as session:
            result = self.promote_drifted_unselected_articles(session, limit=limit)
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

    def list_rejected_enrichments(
        self,
        session: Session,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for enrichment in self.source_enrichment_repository.list_rejected_enrichments(session, limit=limit):
            source_incident = self.source_incident_repository.get_by_id(session, enrichment.source_incident_id)
            items.append(
                {
                    "source_incident_id": str(enrichment.source_incident_id),
                    "source_name": source_incident.source_name if source_incident else None,
                    "title": source_incident.raw_title if source_incident else None,
                    "institution_name": _candidate_institution_name(enrichment, source_incident) if source_incident else None,
                    "failed_reason": enrichment.failed_reason,
                    "updated_at": enrichment.updated_at.isoformat() if enrichment.updated_at else None,
                }
            )
        return items

    def purge_non_education_incidents(
        self,
        session: Session,
        *,
        confirm: bool = False,
        limit: Optional[int] = None,
    ) -> dict[str, Any]:
        """Hard-delete keyword-era junk: source incidents whose enrichment rejected
        them as not education-related (``is_education_related = false``).

        These rows were fetched + enriched but never canonicalized — they only
        inflate the fetched/enriched funnel counts that the paper reports. Child
        rows (urls, article_documents, article_fetch_attempts, source_enrichments)
        are removed automatically via ``ON DELETE CASCADE``. A hard delete is
        required because the counts are raw ``COUNT(*)`` with no is_deleted filter.

        With ``confirm=False`` this only counts (dry run) and deletes nothing.
        Returns a before/after integrity report.
        """
        junk_ids = list(
            session.execute(
                select(SourceEnrichment.source_incident_id).where(
                    SourceEnrichment.is_education_related.is_(False)
                )
            ).scalars().all()
        )
        before_src = int(session.execute(select(func.count(SourceIncident.id))).scalar_one() or 0)
        report: dict[str, Any] = {
            "junk_candidates": len(junk_ids),
            "source_incidents_before": before_src,
            "confirmed": confirm,
            "deleted": 0,
        }
        if not confirm or not junk_ids:
            return report

        if limit is not None:
            junk_ids = junk_ids[:limit]

        deleted = 0
        chunk_size = 500
        for start in range(0, len(junk_ids), chunk_size):
            chunk = junk_ids[start : start + chunk_size]
            res = session.execute(
                SourceIncident.__table__.delete().where(SourceIncident.id.in_(chunk))
            )
            deleted += int(res.rowcount or 0)
        session.flush()

        report["deleted"] = deleted
        report["source_incidents_after"] = int(
            session.execute(select(func.count(SourceIncident.id))).scalar_one() or 0
        )
        return report
