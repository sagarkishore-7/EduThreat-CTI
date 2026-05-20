"""v2 source enrichment service backed by the existing Phase 2 extractor."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Dict, Literal, Optional, Tuple

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage import ArticleContent
from src.edu_cti.pipeline.phase2.utils.deduplication import clean_institution_name, institution_names_match
from src.edu_cti.pipeline.phase2.utils.post_processing import is_headline_format
from src.edu_cti_v2.models import PipelineTask, SourceEnrichment
from src.edu_cti_v2.repositories import (
    ArticleRepository,
    PipelineTaskRepository,
    SourceEnrichmentRepository,
)
from src.edu_cti_v2.source_identity import (
    identity_matches_source_anchor,
    looks_geographic_only_identity,
    recover_source_identity,
)

_COLLECTIVE_IDENTITY_RE = re.compile(
    r"^(?:\d+\s+)?(?:universities|colleges|schools|school districts?|districts|campuses|providers|students)\b",
    re.IGNORECASE,
)
_GENERIC_EDU_ENTITY_RE = (
    r"(?:university|college|school|academy|institute|polytechnic|district|"
    r"school district|community college|technical college|research university|research institute)"
)
_GENERIC_SINGLE_IDENTITY_RE = re.compile(
    r"^(?:(?:the\s+website\s+of\s+)?(?:a|an|the)\s+)?"
    r"(?:public\s+|private\s+|state\s+|local\s+|regional\s+)?"
    rf"(?:{_GENERIC_EDU_ENTITY_RE})(?:\s+{_GENERIC_EDU_ENTITY_RE})*"
    r"(?:\s+in\b.*)?$",
    re.IGNORECASE,
)
_COMMENTARY_IDENTITY_RE = re.compile(
    r"^(?:the\s+cyber\s+threat\s+to|who\s+are|what\s+are|old-school|cyber\s+threat\s+to)\b",
    re.IGNORECASE,
)
_GENERIC_INDUSTRY_RE = re.compile(r"\bindustry\b", re.IGNORECASE)


def _coerce_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def source_incident_to_base_incident(
    source_incident,
    article_url: str,
    *,
    re_enrich_attempts: int | None = None,
    re_enrich_reason: str | None = None,
) -> BaseIncident:
    """Adapt a v2 source incident into the current Phase 2 BaseIncident shape."""
    source_identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    )
    all_urls = [
        row.url
        for row in (source_incident.urls or [])
        if row.url_kind == "article" and not row.is_wrapper
    ]
    if article_url and article_url not in all_urls:
        all_urls.insert(0, article_url)

    return BaseIncident(
        incident_id=str(source_incident.id),
        source=source_incident.source_name,
        source_event_id=source_incident.source_event_key,
        institution_name=source_identity or "",
        victim_raw_name=source_incident.raw_victim_name,
        institution_type=source_incident.raw_institution_type,
        country=source_incident.raw_country,
        region=source_incident.raw_region,
        city=source_incident.raw_city,
        incident_date=source_incident.raw_incident_date,
        date_precision=source_incident.raw_date_precision or "unknown",
        source_published_date=(
            source_incident.source_published_at.date().isoformat()
            if source_incident.source_published_at
            else None
        ),
        ingested_at=source_incident.collected_at.isoformat(),
        title=source_incident.raw_title,
        subtitle=source_incident.raw_subtitle,
        primary_url=None,
        all_urls=all_urls,
        leak_site_url=next((row.url for row in (source_incident.urls or []) if row.url_kind == "leak_site"), None),
        source_detail_url=next((row.url for row in (source_incident.urls or []) if row.url_kind == "detail"), None),
        screenshot_url=next((row.url for row in (source_incident.urls or []) if row.url_kind == "screenshot"), None),
        attack_type_hint=source_incident.raw_attack_hint,
        status=source_incident.raw_status or "suspected",
        source_confidence=source_incident.source_confidence or "medium",
        notes=source_incident.raw_notes,
        threat_actor=source_incident.raw_threat_actor,
        re_enrich_attempts=re_enrich_attempts,
        re_enrich_reason=re_enrich_reason,
    )


def _strip_storage_debug(raw_json_data: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in raw_json_data.items() if key != "_storage_debug"}


def _clean_identity(value: Optional[str]) -> Optional[str]:
    cleaned = clean_institution_name(value).strip()
    return cleaned or None


def _looks_invalid_primary_identity(
    value: Optional[str],
    *,
    title: Optional[str],
    cleaned_value: Optional[str] = None,
) -> bool:
    raw_text = str(value or "").strip()
    text = str(cleaned_value or value or "").strip()
    if not text:
        return True
    if raw_text and is_headline_format(raw_text, title):
        return True
    if is_headline_format(text, title):
        return True
    if _COLLECTIVE_IDENTITY_RE.match(text):
        return True
    if _GENERIC_SINGLE_IDENTITY_RE.match(text):
        return True
    if looks_geographic_only_identity(text):
        return True
    if _COMMENTARY_IDENTITY_RE.match(text):
        return True
    lowered = text.lower()
    if any(marker in lowered for marker in ("websites of", "website of", "multiple universities", "many universities")):
        return True
    if _GENERIC_INDUSTRY_RE.search(text) and "university" not in lowered and "college" not in lowered and "school" not in lowered:
        return True
    words = text.split()
    if len(words) >= 10:
        return True
    if len(words) >= 6 and any(punct in text for punct in (":", ";")):
        return True
    return False


def _mark_non_specific_victim(
    raw_json_data: Dict[str, Any],
    *,
    reason: str,
) -> Dict[str, Any]:
    updated = dict(raw_json_data)
    updated["is_edu_cyber_incident"] = False
    updated["_not_education_related"] = True
    updated["_reason"] = reason
    existing_reasoning = str(updated.get("education_relevance_reasoning") or "").strip()
    if reason not in existing_reasoning:
        updated["education_relevance_reasoning"] = (
            f"{existing_reasoning} {reason}".strip() if existing_reasoning else reason
        )
    return updated


def _mark_victim_review_required(
    raw_json_data: Dict[str, Any],
    *,
    reason: str,
) -> Dict[str, Any]:
    updated = dict(raw_json_data)
    updated["_manual_review_required"] = True
    updated["_reason"] = reason
    existing_reasoning = str(updated.get("education_relevance_reasoning") or "").strip()
    if reason not in existing_reasoning:
        updated["education_relevance_reasoning"] = (
            f"{existing_reasoning} {reason}".strip() if existing_reasoning else reason
        )
    return updated


def _repair_or_reject_primary_identity(
    source_incident,
    *,
    raw_json_data: Dict[str, Any],
    typed_enrichment: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Literal["ok", "reject", "review"]]:
    source_identity = recover_source_identity(
        raw_institution_name=source_incident.raw_institution_name,
        raw_victim_name=source_incident.raw_victim_name,
        raw_subtitle=source_incident.raw_subtitle,
        raw_title=source_incident.raw_title,
    )
    if _looks_invalid_primary_identity(source_identity, title=source_incident.raw_title):
        source_identity = None
    extracted_identity = (
        raw_json_data.get("institution_name")
        or raw_json_data.get("institution_name_en")
        or raw_json_data.get("vendor_name")
        or raw_json_data.get("vendor_name_en")
        or (typed_enrichment or {}).get("institution_name")
        or (typed_enrichment or {}).get("vendor_name")
    )
    cleaned_extracted = _clean_identity(extracted_identity)
    title = source_incident.raw_title

    if _looks_invalid_primary_identity(extracted_identity, title=title, cleaned_value=cleaned_extracted):
        if source_identity:
            raw_json_data = dict(raw_json_data)
            raw_json_data["institution_name"] = source_identity
            raw_json_data["institution_name_basis"] = "source_anchor_fallback"
            if typed_enrichment is not None:
                typed_enrichment = dict(typed_enrichment)
                typed_enrichment["institution_name"] = source_identity
            return raw_json_data, typed_enrichment, "ok"
        reason = "Article does not identify a specific victim institution or vendor."
        return _mark_non_specific_victim(raw_json_data, reason=reason), None, "reject"

    extracted_aliases = _coerce_string_list(raw_json_data.get("institution_aliases"))
    source_aliases = [
        candidate
        for candidate in (
            source_incident.raw_institution_name,
            source_incident.raw_victim_name,
            source_incident.raw_subtitle,
        )
        if candidate
    ]
    if source_identity and cleaned_extracted and not identity_matches_source_anchor(
        cleaned_extracted,
        source_identity,
        extracted_aliases=extracted_aliases,
        source_aliases=source_aliases,
        threshold=80,
    ):
        reason = (
            f"Extracted victim '{cleaned_extracted}' drifted from source anchor "
            f"'{source_identity}'."
        )
        return _mark_victim_review_required(raw_json_data, reason=reason), None, "review"

    return raw_json_data, typed_enrichment, "ok"


class V2EnrichmentService:
    """Persist source-level enrichments using the existing LLM extraction stack."""

    def __init__(
        self,
        *,
        article_repository: Optional[ArticleRepository] = None,
        source_enrichment_repository: Optional[SourceEnrichmentRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        enricher: Optional[IncidentEnricher] = None,
        llm_client: Optional[OllamaLLMClient] = None,
    ) -> None:
        self.article_repository = article_repository or ArticleRepository()
        self.source_enrichment_repository = source_enrichment_repository or SourceEnrichmentRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        if enricher is not None:
            self.enricher = enricher
        else:
            llm_client = llm_client or OllamaLLMClient()
            self.enricher = IncidentEnricher(llm_client=llm_client)

    def _select_article(self, session, source_incident) -> Tuple[Optional[ArticleContent], Optional[object], Optional[str]]:
        document = self.article_repository.get_selected_document(session, source_incident.id)
        if document is None:
            return None, None, None

        url = None
        if document.source_incident_url_id:
            for row in source_incident.urls or []:
                if row.id == document.source_incident_url_id:
                    url = row.resolved_url or row.url
                    break
        if url is None:
            metadata = document.document_metadata or {}
            url = metadata.get("fetched_url") or metadata.get("source_url")
        if not url:
            return None, document, None

        article = ArticleContent(
            url=url,
            title=document.title or "",
            content=document.content_text or "",
            author=document.author,
            publish_date=document.publish_date.isoformat() if document.publish_date else None,
            fetch_successful=True,
            error_message=None,
            content_length=len(document.content_text or ""),
        )
        return article, document, url

    def enrich_source_incident(
        self,
        session,
        source_incident,
        *,
        re_enrich_attempts: int | None = None,
        re_enrich_reason: str | None = None,
        force_canonicalize: bool = False,
    ) -> Dict[str, object]:
        article_content, document, article_url = self._select_article(session, source_incident)
        if article_content is None or document is None or not article_url:
            enrichment = self.source_enrichment_repository.get_by_source_incident(session, source_incident.id)
            if enrichment is None:
                enrichment = SourceEnrichment(
                    source_incident_id=source_incident.id,
                    article_document_id=None,
                )
            if re_enrich_attempts is not None:
                enrichment.re_enrich_attempts = int(re_enrich_attempts)
            if re_enrich_reason is not None:
                enrichment.re_enrich_reason = re_enrich_reason
            enrichment.failed_reason = "No selected article available for enrichment"
            enrichment.is_education_related = None
            enrichment.manual_review_required = False
            enrichment.manual_review_reason = None
            self.source_enrichment_repository.add(session, enrichment)
            return {
                "enriched": False,
                "reason": "missing_article",
                "canonicalize_tasks_enqueued": 0,
            }

        existing_enrichment = self.source_enrichment_repository.get_by_source_incident(session, source_incident.id)
        effective_attempts = (
            int(re_enrich_attempts)
            if re_enrich_attempts is not None
            else int(existing_enrichment.re_enrich_attempts or 0) if existing_enrichment is not None else None
        )
        effective_reason = (
            re_enrich_reason
            if re_enrich_reason is not None
            else existing_enrichment.re_enrich_reason if existing_enrichment is not None else None
        )

        base_incident = source_incident_to_base_incident(
            source_incident,
            article_url,
            re_enrich_attempts=effective_attempts,
            re_enrich_reason=effective_reason,
        )
        result, raw_json_data = self.enricher._enrich_article(
            base_incident,
            {article_url: article_content},
        )

        storage_debug = (raw_json_data or {}).get("_storage_debug", {}) if isinstance(raw_json_data, dict) else {}
        llm_metadata = storage_debug.get("llm_metadata", {})
        raw_llm_responses = storage_debug.get("raw_llm_responses", {})
        typed_enrichment = result.model_dump(mode="json", exclude_none=False) if result is not None else None
        is_education_related = None
        if isinstance(raw_json_data, dict):
            is_education_related = raw_json_data.get("is_edu_cyber_incident")
            if is_education_related is None and raw_json_data.get("_not_education_related"):
                is_education_related = False

        if result is not None and isinstance(raw_json_data, dict) and is_education_related is not False:
            raw_json_data, typed_enrichment, disposition = _repair_or_reject_primary_identity(
                source_incident,
                raw_json_data=raw_json_data,
                typed_enrichment=typed_enrichment,
            )
            if disposition == "reject":
                result = None
                is_education_related = False
            elif disposition == "review":
                result = None
                is_education_related = None

        enrichment = existing_enrichment
        if enrichment is None:
            enrichment = SourceEnrichment(
                source_incident_id=source_incident.id,
                article_document_id=document.id,
            )

        enrichment.article_document_id = document.id
        enrichment.llm_provider = llm_metadata.get("provider", "ollama")
        enrichment.llm_model = llm_metadata.get("model") or getattr(self.enricher.llm_client, "model", None)
        enrichment.prompt_version = llm_metadata.get("prompt_version")
        enrichment.schema_version = llm_metadata.get("schema_version")
        enrichment.mapper_version = llm_metadata.get("mapper_version")
        enrichment.post_processing_version = llm_metadata.get("post_processing_version")
        enrichment.raw_response = raw_llm_responses or None
        enrichment.raw_extraction = _strip_storage_debug(raw_json_data) if isinstance(raw_json_data, dict) else None
        enrichment.typed_enrichment = typed_enrichment
        enrichment.enrichment_confidence = (
            raw_json_data.get("confidence_score")
            if isinstance(raw_json_data, dict) and isinstance(raw_json_data.get("confidence_score"), (int, float))
            else None
        )
        enrichment.is_education_related = is_education_related
        enrichment.re_enrich_attempts = int(effective_attempts or 0)
        enrichment.re_enrich_reason = effective_reason
        enrichment.manual_review_required = bool(
            isinstance(raw_json_data, dict) and raw_json_data.get("_manual_review_required")
        )
        enrichment.manual_review_reason = (
            raw_json_data.get("_reason")
            if enrichment.manual_review_required and isinstance(raw_json_data, dict)
            else None
        )
        enrichment.failed_reason = None
        if result is None:
            if isinstance(raw_json_data, dict):
                enrichment.failed_reason = raw_json_data.get("_reason") or "Enrichment returned no typed result"
            else:
                enrichment.failed_reason = "Enrichment returned no typed result"

        self.source_enrichment_repository.add(session, enrichment)

        canonicalize_tasks_enqueued = 0
        if force_canonicalize or (result is not None and is_education_related is not False):
            existing_canonicalize_task = self.pipeline_task_repository.get_active_for_target(
                session,
                task_type="canonicalize",
                target_table="source_incidents",
                target_id=source_incident.id,
            )
            if existing_canonicalize_task is None:
                self.pipeline_task_repository.enqueue(
                    session,
                    PipelineTask(
                        run_id=None,
                        task_type="canonicalize",
                        target_table="source_incidents",
                        target_id=source_incident.id,
                        status="queued",
                        priority=120,
                        payload={
                            "source_incident_id": str(source_incident.id),
                            "source_name": source_incident.source_name,
                            "trigger": "reenrich" if force_canonicalize else "enrich_source",
                        },
                        result={},
                        available_at=datetime.now(timezone.utc),
                        attempt_count=0,
                        max_attempts=5,
                    ),
                )
                canonicalize_tasks_enqueued = 1

        return {
            "enriched": result is not None,
            "is_education_related": is_education_related,
            "has_typed_enrichment": typed_enrichment is not None,
            "article_document_id": str(document.id),
            "canonicalize_tasks_enqueued": canonicalize_tasks_enqueued,
        }
