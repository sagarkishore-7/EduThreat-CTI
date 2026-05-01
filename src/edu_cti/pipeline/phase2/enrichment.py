"""
Main LLM enrichment orchestrator for Phase 2.

Simplified pipeline focused on CTI extraction using JSON schema.
Only extracts cyber threat intelligence data from articles.
"""

import logging
import json
import re
import time as _time_module
from typing import Optional, Dict, List, Tuple, Any

from src.edu_cti.core import metrics as _metrics
from src.edu_cti.pipeline.phase2.extraction.instructor_corrector import (
    should_trigger_correction,
    apply_instructor_corrections,
)

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.pipeline.phase2.extraction.extraction_schema import (
    EXTRACTION_SCHEMA,
    EXTRACTION_SCHEMA_PART1,
    EXTRACTION_SCHEMA_PART2,
    PART2_PROMPT_TEMPLATE,
    SUMMARY_PROMPT,
)
from src.edu_cti.pipeline.phase2.extraction.extraction_prompt import PROMPT_TEMPLATE
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import json_to_cti_enrichment

logger = logging.getLogger(__name__)


_NON_EDU_ALLOWED_KEYS = {
    "is_edu_cyber_incident",
    "education_relevance_reasoning",
    "_salvaged_from_truncated",
    "_not_education_related",
    "_enrichment_failed",
    "_reason",
}

_STORAGE_DEBUG_KEY = "_storage_debug"
_PROMPT_VERSION = "phase2_prompt_v1"
_SCHEMA_VERSION = "phase2_schema_v1"
_MAPPER_VERSION = "phase2_mapper_v1"
_POST_PROCESSING_VERSION = "phase2_post_processing_v1"


def _coerce_bool_like(value: Any) -> Optional[bool]:
    """Convert common LLM boolean representations to bool while preserving None."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1"}:
            return True
        if normalized in {"false", "no", "0"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _strip_non_education_cti_fields(json_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only education-review fields when the LLM decides the article is not an
    education cyber incident.

    Some models still emit attack/timeline/impact structure even with
    is_edu_cyber_incident=false. Stripping those fields here prevents non-education
    rows from leaking into downstream analytics or incident detail views.
    """
    sanitized = {
        key: value for key, value in json_data.items() if key in _NON_EDU_ALLOWED_KEYS
    }
    sanitized["is_edu_cyber_incident"] = False
    sanitized["education_relevance_reasoning"] = (
        sanitized.get("education_relevance_reasoning")
        or "LLM determined the article is not an education cyber incident."
    )
    return sanitized


def _build_storage_debug_payload(
    llm_client: OllamaLLMClient,
    extraction_mode: str,
    raw_llm_responses: Dict[str, Optional[str]],
) -> Dict[str, Any]:
    """Capture exact model artifacts plus lightweight provenance metadata."""
    return {
        "raw_llm_responses": {
            key: value
            for key, value in raw_llm_responses.items()
            if value is not None
        },
        "llm_metadata": {
            "provider": "ollama",
            "host": getattr(llm_client, "host", None),
            "model": getattr(llm_client, "model", None),
            "extraction_mode": extraction_mode,
            "prompt_version": _PROMPT_VERSION,
            "schema_version": _SCHEMA_VERSION,
            "mapper_version": _MAPPER_VERSION,
            "post_processing_version": _POST_PROCESSING_VERSION,
        },
    }


def _attach_storage_debug(
    json_data: Dict[str, Any],
    llm_client: OllamaLLMClient,
    extraction_mode: str,
    raw_llm_responses: Dict[str, Optional[str]],
) -> Dict[str, Any]:
    """Attach debug artifacts to the parsed extraction payload without affecting mapping."""
    enriched = dict(json_data)
    enriched[_STORAGE_DEBUG_KEY] = _build_storage_debug_payload(
        llm_client=llm_client,
        extraction_mode=extraction_mode,
        raw_llm_responses=raw_llm_responses,
    )
    return enriched


def count_filled_fields(enrichment_result: CTIEnrichmentResult) -> int:
    """
    Count how many schema fields are filled (not None) in the enrichment result.
    
    This is used to score articles - the article that fills the most fields
    is considered the best source.
    
    Args:
        enrichment_result: CTIEnrichmentResult to count fields for
        
    Returns:
        Number of filled fields
    """
    count = 0
    
    # Count top-level optional fields
    if enrichment_result.primary_url:
        count += 1
    if enrichment_result.initial_access_description:
        count += 1
    if enrichment_result.timeline:
        count += len(enrichment_result.timeline)
    if enrichment_result.mitre_attack_techniques:
        count += len(enrichment_result.mitre_attack_techniques)
    if enrichment_result.attack_dynamics:
        count += 1
    
    # Count nested impact metrics
    for attr in ['data_impact', 'system_impact', 'user_impact', 'operational_impact_metrics',
                 'financial_impact', 'regulatory_impact', 'recovery_metrics', 
                 'transparency_metrics', 'research_impact']:
        val = getattr(enrichment_result, attr, None)
        if val:
            count += 1
            # Count non-None values in dict
            if isinstance(val, dict):
                count += sum(1 for v in val.values() if v is not None)
    
    # Count attack dynamics fields
    if enrichment_result.attack_dynamics:
        ad = enrichment_result.attack_dynamics
        for attr in ['attack_vector', 'attack_chain', 'ransomware_family', 'data_exfiltration',
                     'encryption_impact', 'ransom_demanded', 'ransom_amount', 'ransom_paid',
                     'recovery_timeframe_days', 'business_impact', 'operational_impact']:
            if getattr(ad, attr, None) is not None:
                count += 1
    
    return count


class IncidentEnricher:
    """
    Main orchestrator for enriching CTI incidents with LLM.

    For short articles (< SPLIT_THRESHOLD_CHARS combined text) a single extraction
    call is used. For long articles the 3-call split strategy is used instead:
      Call 1 — core identification & classification (Part 1 schema, ~45 fields)
      Call 2 — deep intelligence (Part 2 schema, ~35 fields: timeline, MITRE,
                regulatory, financial, recovery)
      Call 3 — enriched_summary (unchanged)
    This keeps each grammar-constrained schema small so the model fills every
    field rather than dropping late-schema fields under long-context pressure.
    """

    # Articles longer than this (combined chars) use the 3-call split.
    # Below the threshold a single call is faster and equally accurate.
    SPLIT_THRESHOLD_CHARS = 25_000

    def __init__(self, llm_client: Optional[OllamaLLMClient] = None):
        if not llm_client:
            raise ValueError("llm_client is required")
        self.llm_client = llm_client

    def _extract_json_with_artifacts(
        self,
        system_prompt: str,
        user_prompt: str,
        schema: Dict[str, Any],
        max_retries: int,
    ) -> Tuple[str, str]:
        """
        Return normalized JSON plus the exact raw model text when available.

        Falls back to the older extract_json() contract so existing mocks/tests
        and alternate clients keep working.
        """
        extractor = getattr(self.llm_client, "extract_json_with_metadata", None)
        if callable(extractor):
            payload = extractor(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                schema=schema,
                max_retries=max_retries,
            )
            if isinstance(payload, dict) and "normalized_json" in payload:
                raw_text = payload.get("raw_content") or payload["normalized_json"]
                return payload["normalized_json"], raw_text

        normalized_json = self.llm_client.extract_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema=schema,
            max_retries=max_retries,
        )
        return normalized_json, normalized_json

    def _should_use_split(self, article_contents: Dict[str, "ArticleContent"]) -> bool:
        """Return True when combined article text exceeds the split threshold."""
        total = sum(len(a.content or "") for a in article_contents.values())
        return total >= self.SPLIT_THRESHOLD_CHARS
    
    def process_incident(
        self,
        incident: BaseIncident,
        skip_if_not_education: bool = True,
        conn=None,
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """
        Process an incident through the LLM enrichment pipeline.
        
        For incidents with multiple articles, enriches each article individually,
        scores them based on field coverage, and selects the best one.
        
        Args:
            incident: BaseIncident to enrich
            skip_if_not_education: If True, skip incidents not related to education
            conn: Database connection (required to read articles from DB)
            
        Returns:
            Tuple of (CTIEnrichmentResult, raw_json_data) if incident is enriched, (None, None) otherwise
        """
        if conn is None:
            raise ValueError("Database connection required to read articles from DB")
        
        from src.edu_cti.pipeline.phase2.storage.article_storage import (
            get_all_articles_for_incident,
            promote_primary_article,
        )
        
        # Get articles from database
        all_articles_data = get_all_articles_for_incident(conn, incident.incident_id)
        
        if not all_articles_data:
            logger.warning(f"No articles found in DB for incident {incident.incident_id}")
            return None, None
        
        # Filter to only successful articles with content
        valid_articles_data = [
            art for art in all_articles_data
            if art["fetch_successful"] and art["content"] and len(art["content"].strip()) > 50
        ]
        
        if not valid_articles_data:
            logger.warning(f"No valid articles found in DB for incident {incident.incident_id}")
            return None, None
        
        # Convert article data to ArticleContent objects
        article_contents = {}
        for art_data in valid_articles_data:
            article_contents[art_data["url"]] = ArticleContent(
                url=art_data["url"],
                title=art_data["title"] or "",
                content=art_data["content"] or "",
                author=art_data.get("author"),
                publish_date=art_data.get("publish_date"),
                fetch_successful=art_data["fetch_successful"],
                error_message=art_data.get("error_message"),
                content_length=art_data.get("content_length", 0),
            )
        
        # Process articles — use the 3-call split for long articles
        if len(article_contents) > 1:
            enrichment_result, raw_json_data = self._process_multiple_articles(
                incident, article_contents, skip_if_not_education
            )
        else:
            use_split = self._should_use_split(article_contents)
            if use_split:
                logger.debug(
                    f"{incident.incident_id}: article ≥ {self.SPLIT_THRESHOLD_CHARS:,} chars "
                    f"— using 3-call split extraction"
                )
            _enrich = self._enrich_article_split if use_split else self._enrich_article
            enrichment_result, raw_json_data = _enrich(incident, article_contents)
            
            if not enrichment_result:
                # If _enrich_article already set a marker (e.g. _not_education_related),
                # preserve it so __main__ can delete rather than retry.
                if raw_json_data and isinstance(raw_json_data, dict) and (
                    raw_json_data.get("_not_education_related") or raw_json_data.get("_enrichment_failed")
                ):
                    return None, raw_json_data
                logger.warning(f"Failed to enrich incident {incident.incident_id}")
                return None, {"_enrichment_failed": True, "_reason": "LLM enrichment failed"}
            
            # Check education relevance - directly from LLM analysis
            if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
                logger.debug(
                    f"{incident.incident_id} not edu-related: "
                    f"{enrichment_result.education_relevance.reasoning}"
                )
                # Return marker indicating explicitly not education-related
                return None, {"_not_education_related": True, "_reason": enrichment_result.education_relevance.reasoning}
            
            # Set primary URL
            if not enrichment_result.primary_url and article_contents:
                enrichment_result.primary_url = list(article_contents.keys())[0]
        
        if not enrichment_result:
            # Preserve raw_json_data so __main__ can act on _not_education_related /
            # _enrichment_failed markers rather than treating this as "no article content".
            return None, raw_json_data

        # Mark primary article in database and cleanup
        if enrichment_result.primary_url:
            promote_primary_article(
                conn,
                incident.incident_id,
                enrichment_result.primary_url,
            )

        logger.debug(
            f"{incident.incident_id}: enrichment done "
            f"(edu={enrichment_result.education_relevance.is_education_related})"
        )
        
        return enrichment_result, raw_json_data
    
    def _process_multiple_articles(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
        skip_if_not_education: bool,
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """
        Process multiple articles with a single combined LLM call.

        Strategy:
        1. Send all articles concatenated in one LLM call — _enrich_article already
           combines texts, so passing the full dict costs one call regardless of N.
        2. If the combined call succeeds and passes the education relevance check, return
           immediately (1 LLM call total).
        3. If the combined call fails (LLM error, bad JSON, not-education), fall back to
           trying each article individually so a single bad article doesn't sink the whole
           incident.  Only articles that individually succeed and pass relevance are scored;
           the highest-scoring one wins.
        """
        n = len(article_contents)
        primary_url = next(iter(article_contents))

        _use_split = self._should_use_split(article_contents)
        logger.debug(
            f"{incident.incident_id}: {n} articles — "
            f"{'3-call split' if _use_split else 'single-call'} combined attempt"
        )
        _enrich_combined = self._enrich_article_split if _use_split else self._enrich_article

        # ── Step 1: single combined call ──────────────────────────────────────────
        try:
            enrichment_result, raw_json_data = _enrich_combined(incident, article_contents)

            if enrichment_result:
                is_edu = enrichment_result.education_relevance.is_education_related
                if skip_if_not_education and not is_edu:
                    logger.info(f"{incident.incident_id}: combined call — not education-related, skipping")
                    return None, {"_not_education_related": True, "_reason": "Combined article not education-related"}

                enrichment_result.primary_url = primary_url
                score = count_filled_fields(enrichment_result)
                logger.debug(
                    f"{incident.incident_id}: combined call succeeded ({score} fields, "
                    f"edu={is_edu}) — skipping per-article loop"
                )
                return enrichment_result, raw_json_data

            # Combined call returned None (explicit not-education or total failure)
            if raw_json_data and isinstance(raw_json_data, dict):
                if raw_json_data.get("_not_education_related"):
                    logger.info(f"{incident.incident_id}: combined call — not education-related")
                    return None, raw_json_data
                # Enrichment failed — fall through to per-article fallback
                logger.warning(
                    f"{incident.incident_id}: combined call failed "
                    f"({raw_json_data.get('_reason', 'unknown')}) — trying per-article fallback"
                )
            else:
                logger.warning(f"{incident.incident_id}: combined call returned None — trying per-article fallback")

        except Exception as e:
            logger.warning(f"{incident.incident_id}: combined call exception ({e}) — trying per-article fallback")

        # ── Step 2: per-article fallback ─────────────────────────────────────────
        logger.debug(f"{incident.incident_id}: per-article fallback ({n} articles)")
        article_scores: List[Tuple[str, CTIEnrichmentResult, int, Optional[Dict[str, Any]]]] = []
        all_not_education = True

        for idx, (url, article_content) in enumerate(article_contents.items(), 1):
            logger.debug(f"  [{idx}/{n}] enriching {url[:80]}")
            try:
                single_article = {url: article_content}
                _enrich_single = (
                    self._enrich_article_split
                    if self._should_use_split(single_article)
                    else self._enrich_article
                )
                enrichment_result, raw_json_data = _enrich_single(incident, single_article)

                if enrichment_result:
                    all_not_education = False
                    if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
                        logger.info(f"  ⊘ {url[:80]} — not education-related")
                        continue
                    score = count_filled_fields(enrichment_result)
                    article_scores.append((url, enrichment_result, score, raw_json_data))
                    logger.debug(f"  scored {score} fields: {url[:80]}")
                elif raw_json_data and isinstance(raw_json_data, dict):
                    if raw_json_data.get("_not_education_related"):
                        logger.info(f"  ⊘ {url[:80]} — not education-related")
                    elif raw_json_data.get("_enrichment_failed"):
                        all_not_education = False
                        logger.warning(f"  ✗ {url[:80]}: {raw_json_data.get('_reason', 'unknown')}")
                else:
                    all_not_education = False
                    logger.warning(f"  ✗ {url[:80]}: no result")
            except Exception as e:
                all_not_education = False
                logger.error(f"  ✗ {url[:80]}: {e}", exc_info=True)

        if not article_scores:
            if all_not_education:
                logger.debug(f"All articles for {incident.incident_id} are not education-related")
                return None, {"_not_education_related": True, "_reason": "All articles not education-related"}
            logger.debug(f"No articles scored for {incident.incident_id} (all failed or irrelevant)")
            return None, {"_enrichment_failed": True, "_reason": "All articles failed to enrich"}

        best_url, best_result, best_score, best_raw_json = max(article_scores, key=lambda x: x[2])
        logger.debug(f"Primary: {best_url[:80]} ({best_score} fields)")
        best_result.primary_url = best_url
        return best_result, best_raw_json
    
    def _enrich_article(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """
        Enrich incident using JSON schema extraction with centralized prompt.
        
        Uses PROMPT_TEMPLATE from extraction_prompt.py for consistent prompting.
        
        Args:
            incident: BaseIncident to enrich
            article_contents: Dictionary mapping URL to ArticleContent
            
        Returns:
            Tuple of (CTIEnrichmentResult, raw_json_data) or (None, None) on error
        """
        if not article_contents:
            return None, None
        
        # Get primary article
        primary_url = list(article_contents.keys())[0]
        primary_article = article_contents[primary_url]
        
        # Combine all article content
        all_text = []
        for url, article in article_contents.items():
            all_text.append(f"[URL: {url}]")
            if article.title:
                all_text.append(f"Title: {article.title}")
            if article.publish_date:
                all_text.append(f"Published: {article.publish_date}")
            if article.author:
                all_text.append(f"Author: {article.author}")
            all_text.append(f"\n{article.content}\n")
        
        combined_text = "\n".join(all_text)
        title = primary_article.title or ""

        # Truncate article text to fit within LLM context window.
        # DeepSeek V3 has 64K context (~256K chars):
        #   - prompt template + instructions : ~2K tokens  (~8K chars)
        #   - JSON schema                    : ~8K tokens  (~32K chars)
        #   - title + URL                    : ~100 tokens (~400 chars)
        #   - target_institution_line (max)  : ~50 tokens  (~200 chars)
        #   - LLM output budget              : ~4K tokens  (~16K chars)
        #   ─────────────────────────────────────────────────────────
        #   Remaining for article text       : ~50K tokens (~200K chars)
        # Use 180K chars as the cutoff for a comfortable safety margin.
        MAX_ARTICLE_CHARS = 180_000
        if len(combined_text) > MAX_ARTICLE_CHARS:
            logger.warning(
                f"Truncating article text from {len(combined_text):,} to {MAX_ARTICLE_CHARS:,} chars "
                f"for incident {incident.incident_id}"
            )
            combined_text = combined_text[:MAX_ARTICLE_CHARS] + "\n\n[TRUNCATED — article too long]"

        # System prompt — kept short and static so Ollama's KV-cache can reuse the prefix
        # across consecutive calls. Character-encoding warnings removed: the schema passed
        # as format= already enforces valid JSON tokens at generation level.
        system_prompt = (
            "You are a Cyber Threat Intelligence Analyst specialising in education sector incidents. "
            "Output ONLY valid JSON. Null for unknown fields. No prose, no markdown."
        )

        # Only inject a TARGET INSTITUTION hint when the incident is a secondary stub
        # extracted from a roundup article — i.e. its notes start with
        # "Extracted from roundup:". For normal single-article incidents the LLM
        # infers the primary institution from the article itself, and anchoring it
        # to a potentially-wrong DB name would reduce accuracy.
        notes_text = (incident.notes or "").strip()
        is_roundup_stub = notes_text.startswith("Extracted from roundup:")
        known_name = (incident.institution_name or "").strip()
        _UNKNOWN_NAMES = {"unknown", "n/a", "none", "unnamed", "undisclosed", ""}
        if is_roundup_stub and known_name and known_name.lower() not in _UNKNOWN_NAMES:
            target_institution_line = (
                f"\n- TARGET INSTITUTION: {known_name}"
                f"\n  (This article may cover multiple institutions. Extract THIS institution's"
                f" incident as the primary. List all others in other_edu_incidents.)"
            )
        else:
            target_institution_line = ""

        article_metadata_lines = []
        if incident.source_published_date:
            article_metadata_lines.append(f"- Source Published Date: {incident.source_published_date}")
        if primary_article.publish_date:
            article_metadata_lines.append(f"- Article Publish Date: {primary_article.publish_date}")
        if primary_article.author:
            article_metadata_lines.append(f"- Article Author: {primary_article.author}")
        article_metadata_block = ("\n" + "\n".join(article_metadata_lines)) if article_metadata_lines else "\n- Article Metadata: unknown"

        # ── GLiNER NER pre-pass ────────────────────────────────────────────────
        try:
            from src.edu_cti.pipeline.phase2.extraction.ner_preprocessor import build_ner_hint_block
            _ner_hint = build_ner_hint_block(combined_text, title)
        except Exception:
            _ner_hint = None

        # ── IntelEX RAG: retrieve top-5 MITRE technique candidates ────────────
        try:
            from src.edu_cti.pipeline.phase2.extraction.mitre_rag import build_mitre_rag_block
            _rag_block = build_mitre_rag_block(combined_text)
        except Exception:
            _rag_block = None

        _extra = ""
        if _ner_hint:
            _extra += f"\n\n{_ner_hint}"
        if _rag_block:
            _extra += f"\n\n{_rag_block}"

        user_prompt = PROMPT_TEMPLATE.format(
            url=primary_url,
            title=title,
            target_institution_line=target_institution_line,
            article_metadata_block=article_metadata_block,
            text=combined_text
        ) + _extra

        try:
            # Call LLM — pass EXTRACTION_SCHEMA as format so Ollama builds a GBNF grammar
            # from it. This enforces enum values at token level AND removes ~8K tokens of
            # schema JSON from the user prompt.
            _llm_t0 = _time_module.time()
            _metrics.increment("llm_attempt_total", labels={"attempt_num": "1"})
            try:
                raw_response, raw_response_text = self._extract_json_with_artifacts(
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    schema=EXTRACTION_SCHEMA,
                    max_retries=2,
                )
            except Exception as _llm_exc:
                _metrics.observe("llm_duration_seconds", _time_module.time() - _llm_t0)
                _exc_msg = str(_llm_exc).lower()
                if "timeout" in _exc_msg or "timed out" in _exc_msg:
                    _metrics.increment("llm_timeout_total")
                raise
            _metrics.observe("llm_duration_seconds", _time_module.time() - _llm_t0)

            # Parse JSON response
            json_data = self._parse_json_response(raw_response)
            if json_data is None:
                _metrics.increment("llm_invalid_json_total")
                return None, None

            # Handle salvaged truncated JSON (only has education relevance flag)
            if json_data.get("_salvaged_from_truncated"):
                if not json_data.get("is_edu_cyber_incident"):
                    # Not education-related — signal to caller for deletion
                    return None, {
                        "_not_education_related": True,
                        "_reason": json_data.get("education_relevance_reasoning", "Truncated JSON"),
                    }
                # Education-related but truncated — treat as failed for retry
                return None, None

            # Coerce any scalar fields that the LLM returned as lists.
            # Grammar-constrained generation occasionally wraps a single value in an array.
            # We normalise here so that BOTH json_to_cti_enrichment AND the raw_json_data
            # returned to save_enrichment_result see consistent scalar types.
            from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import _coerce_llm_scalars
            json_data = _coerce_llm_scalars(json_data)

            # Some models still emit structured CTI fields even after concluding the
            # article is not education-related. Strip those fields before mapping so
            # non-education incidents cannot pollute analytics when skip_if_not_education
            # is disabled.
            coerced_edu = _coerce_bool_like(json_data.get("is_edu_cyber_incident"))
            if coerced_edu is False:
                json_data = _strip_non_education_cti_fields(json_data)

            # ── Second LLM call: generate enriched_summary ───────────────────
            # Dedicated small call so the full token budget for the main call
            # goes to intelligence fields. Only run for education-related incidents.
            summary_raw_text: Optional[str] = None
            if coerced_edu is not False and combined_text:
                try:
                    _institution = (
                        json_data.get("institution_name")
                        or (incident.university_name if hasattr(incident, "university_name") else None)
                        or "Unknown institution"
                    )
                    _attack_cat = json_data.get("attack_category") or "unknown"
                    _summary_prompt = SUMMARY_PROMPT.format(
                        institution=_institution,
                        attack_category=_attack_cat,
                        text=combined_text[:8000],  # cap to keep latency low
                    )
                    # Use plain-text chat (format=None) — GBNF forces empty string,
                    # format="json" truncates mid-string on Ollama Cloud. Plain text
                    # returns a complete paragraph that we store directly.
                    _chat_resp = self.llm_client.chat(
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "You are a Cyber Threat Intelligence analyst. "
                                    "Write a concise 2-3 sentence summary of the cyber incident."
                                ),
                            },
                            {"role": "user", "content": _summary_prompt},
                        ],
                        format=None,
                        stream=False,
                        temperature=0.0,
                    )
                    # Extract plain-text content from Ollama response
                    _summary_text: Optional[str] = None
                    if hasattr(_chat_resp, "message") and hasattr(_chat_resp.message, "content"):
                        _summary_text = _chat_resp.message.content
                    elif isinstance(_chat_resp, dict):
                        _msg = _chat_resp.get("message", {})
                        _summary_text = (
                            _msg.get("content", "") if isinstance(_msg, dict)
                            else getattr(_msg, "content", None)
                        )
                    summary_raw_text = _summary_text

                    if _summary_text:
                        _summary_text = _summary_text.strip()
                        # Strip any JSON wrapper if the model added one anyway
                        if _summary_text.startswith("{"):
                            try:
                                _parsed = json.loads(_summary_text)
                                _summary_text = _parsed.get("enriched_summary", _summary_text)
                            except Exception:
                                pass
                        if len(_summary_text) > 20:
                            json_data["enriched_summary"] = _summary_text
                            logger.debug(f"Summary call succeeded ({len(_summary_text)} chars)")
                except Exception as _e:
                    logger.warning(f"Summary LLM call failed (non-fatal): {_e}")
            # ─────────────────────────────────────────────────────────────────

            # ── Instructor correction pass (targeted retry for null critical fields) ──
            # Fires only when ≥2 of {attack_category, institution_type, attack_vector,
            # country} are null/unknown. Makes ONE additional LLM call; never blocks
            # the main pipeline on failure.
            if coerced_edu is not False and should_trigger_correction(json_data):
                json_data, _corrected = apply_instructor_corrections(
                    json_data=json_data,
                    article_text=combined_text,
                    institution_name=json_data.get("institution_name") or "Unknown institution",
                    ollama_client=self.llm_client,
                )
                if _corrected:
                    _metrics.increment("instructor_correction_applied_total")

            json_data = _attach_storage_debug(
                json_data=json_data,
                llm_client=self.llm_client,
                extraction_mode="single_call",
                raw_llm_responses={
                    "extraction": raw_response_text,
                    "summary": summary_raw_text,
                },
            )

            # Map to CTIEnrichmentResult
            result = json_to_cti_enrichment(json_data, primary_url, incident)

            # Emit education relevance and confidence metrics
            _src_label = getattr(incident, "source_name", None) or "unknown"
            if coerced_edu is not False:
                _metrics.increment("llm_education_relevance_pass_total", labels={"source": _src_label})
            else:
                _metrics.increment("llm_education_relevance_fail_total", labels={"source": _src_label})
            _conf = json_data.get("confidence_score")
            if isinstance(_conf, (int, float)) and 0.0 <= _conf <= 1.0:
                _metrics.observe("llm_confidence_score", float(_conf))

            return result, json_data

        except Exception as e:
            logger.error(f"Error during enrichment: {e}", exc_info=True)
            return None, None
    
    def _parse_json_response(self, raw_response: str) -> Optional[Dict[str, Any]]:
        """Parse JSON from LLM response, handling various formats."""
        try:
            raw_response = raw_response.strip()
            
            # Try to extract JSON from markdown code blocks
            json_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', raw_response, re.DOTALL)
            if json_match:
                raw_response = json_match.group(1).strip()
            else:
                # Try to find JSON object directly
                json_match = re.search(r'\{.*\}', raw_response, re.DOTALL)
                if json_match:
                    raw_response = json_match.group(0).strip()
            
            # Handle escaped newlines
            if raw_response.startswith('{\\n') or raw_response.startswith('{\\\\n'):
                raw_response = raw_response.replace('\\n', '\n').replace('\\r', '\r').replace('\\t', '\t')
            
            # Parse JSON
            try:
                parsed = json.loads(raw_response)
                # LLM sometimes returns [] meaning "not applicable / not education-related"
                if isinstance(parsed, list):
                    return {
                        "is_edu_cyber_incident": False,
                        "education_relevance_reasoning": "LLM returned empty array (not education-related)",
                    }
                return parsed
            except json.JSONDecodeError as e:
                # Try fixing common LLM JSON issues
                fixed_response = raw_response

                # Fix 1: LLM sometimes outputs \' which is invalid JSON (should be ')
                fixed_response = fixed_response.replace("\\'", "'")

                # Fix 2: Handle double-escaped quotes
                fixed_response = fixed_response.replace('\\"', '"').replace('\\\\', '\\')

                # Fix 3: Remove trailing commas before } or ] — deepseek often emits these
                fixed_response = re.sub(r',\s*([}\]])', r'\1', fixed_response)

                # Fix 4: deepseek injects Chinese lottery-spam tokens into the JSON in
                # three patterns:
                #   a) Entire garbage lines between valid fields:
                #      null,\n极速赛车开奖直播历史记录\n  "field": null
                #   b) Leading non-ASCII before a JSON field on a line:
                #      \n极 "mttd_hours": null
                #   c) Non-ASCII embedded within a JSON key name:
                #      "field_name极": null
                #   d) Bare quoted non-ASCII string inside an object (no colon):
                #      "极速赛车开奖结果记录查询官网",
                # Strategy: drop any line that has NO colon (not a key-value pair) AND
                # contains at least one non-ASCII character.  Also drop lines with no
                # JSON structural chars at all (pure unquoted garbage).
                fixed_response = re.sub(
                    r'^\s*"[^"\n]*[^\x00-\x7F][^"\n]*"\s*,?\s*$',
                    '',
                    fixed_response,
                    flags=re.MULTILINE,
                )
                fixed_response = re.sub(
                    r'^\s*"[^"\n]*"\s*,?\s*$',
                    '',
                    fixed_response,
                    flags=re.MULTILINE,
                )

                def _is_spam_line(line: str) -> bool:
                    stripped = line.strip()
                    if not stripped:
                        return False  # keep blank lines
                    has_nonascii = bool(re.search(r'[^\x00-\x7F]', stripped))
                    if not has_nonascii:
                        return False  # no non-ASCII → not spam
                    # If the line has a colon it's likely a key-value pair — keep it
                    # (e.g. `"field极": null` — handled later by key-name strip).
                    if ':' in stripped:
                        return False
                    # No colon + has non-ASCII → spam line (bare string / garbage)
                    return True

                fixed_response = '\n'.join(
                    line for line in fixed_response.split('\n')
                    if not _is_spam_line(line) and (
                        re.search(r'[":{}\[\]]', line) or not line.strip()
                    )
                )
                # Strip leading non-ASCII prefix from remaining lines.
                fixed_response = re.sub(r'^[^\x00-\x7F]+', '', fixed_response, flags=re.MULTILINE)
                # Strip non-ASCII embedded within key names.
                fixed_response = re.sub(r'([A-Za-z_]\w*)[^\x00-\x7F]+(")', r'\1\2', fixed_response)
                # Re-apply trailing comma removal (spam-line removal may expose new `,}`)
                fixed_response = re.sub(r',\s*([}\]])', r'\1', fixed_response)

                # Fix 5: Try with fixed response
                try:
                    return json.loads(fixed_response)
                except json.JSONDecodeError as e5:
                    # Log what fixed_response looks like near the new failure position
                    # so we can diagnose any remaining garbage patterns
                    if hasattr(e5, 'pos') and e5.pos is not None:
                        snip = fixed_response[max(0, e5.pos - 60):e5.pos + 30]
                        logger.error(f"Fix5 still failing at pos={e5.pos}: {repr(snip)}")

                # Fix 5b: Nuclear — strip ALL non-ASCII chars from the entire response.
                # Last resort for responses with many scattered garbage injections.
                # May corrupt non-ASCII string values but JSON is unparseable anyway.
                try:
                    nuclear = re.sub(r'[^\x00-\x7F]', '', fixed_response)
                    # Re-apply trailing comma removal after stripping (new `,}` may appear)
                    nuclear = re.sub(r',\s*([}\]])', r'\1', nuclear)
                    # Remove orphaned empty strings left by nuclear strip of spam lines.
                    # e.g. `"极速赛车官网",` → `"",` which is invalid in object context.
                    nuclear = re.sub(r'^\s*""\s*,?\s*$', '', nuclear, flags=re.MULTILINE)
                    nuclear = re.sub(r',\s*([}\]])', r'\1', nuclear)
                    return json.loads(nuclear)
                except json.JSONDecodeError:
                    pass

                # Fix 6: Handle leading newline after brace
                if raw_response.startswith('{\n'):
                    fixed = '{' + raw_response[2:].lstrip()
                    try:
                        return json.loads(fixed)
                    except json.JSONDecodeError:
                        pass

                # Fix 7: Truncated JSON — attempt repair then salvage what we can.
                # The LLM often returns valid JSON that gets cut off at the token
                # limit. Strategy:
                #   a) Try closing the unclosed JSON object with "}" and re-parse.
                #      This recovers all fields the LLM managed to write before cutoff.
                #   b) If repair fails, at minimum extract is_edu_cyber_incident so
                #      we can delete non-education incidents without a full retry.
                edu_match = re.search(
                    r'"is_edu_cyber_incident"\s*:\s*(true|false)',
                    raw_response, re.IGNORECASE,
                )
                if edu_match:
                    is_edu = edu_match.group(1).lower() == "true"
                    reason_match = re.search(
                        r'"education_relevance_reasoning"\s*:\s*"([^"]*)"',
                        raw_response,
                    )
                    reason = reason_match.group(1) if reason_match else "Extracted from truncated JSON"

                    # Attempt JSON repair: strip trailing partial field and close object
                    repaired = None
                    try:
                        # Remove the last (likely incomplete) key-value pair by
                        # finding the last complete comma-separated field boundary.
                        # Strategy: strip everything after the last complete '"key": value' pair.
                        truncated = raw_response.rstrip()
                        # Remove trailing partial token (incomplete string/number/null)
                        # by stripping back to the last comma or opening brace.
                        last_comma = truncated.rfind(",")
                        last_open = truncated.rfind("{")
                        cut_pos = max(last_comma, last_open) if last_comma > last_open else last_comma
                        if cut_pos > 0:
                            candidate = truncated[:cut_pos].rstrip().rstrip(",") + "\n}"
                            try:
                                repaired = json.loads(candidate)
                            except json.JSONDecodeError:
                                pass
                        # Also try simply appending "}"
                        if repaired is None:
                            try:
                                repaired = json.loads(truncated + "}")
                            except json.JSONDecodeError:
                                pass
                    except Exception:
                        pass

                    if repaired is not None:
                        logger.info(
                            f"Repaired truncated JSON (is_edu={is_edu}) — "
                            f"{len(repaired)} fields recovered"
                        )
                        return repaired

                    # Repair failed — salvage what we can
                    if not is_edu:
                        logger.info(
                            f"Salvaged is_edu_cyber_incident=false from truncated JSON: {reason[:80]}"
                        )
                        return {
                            "is_edu_cyber_incident": False,
                            "education_relevance_reasoning": reason,
                            "_salvaged_from_truncated": True,
                        }
                    else:
                        logger.warning(
                            f"JSON truncated but is_edu_cyber_incident=true — cannot salvage full enrichment"
                        )

                logger.error(f"JSON parse error: {e}")
                logger.error(f"Response (first 500 chars): {repr(raw_response[:500])}")
                # Log chars around the failure position to diagnose the pattern
                if hasattr(e, 'pos') and e.pos is not None:
                    pos = e.pos
                    snippet = raw_response[max(0, pos - 80):pos + 40]
                    logger.error(f"Context around error (pos={pos}): {repr(snippet)}")
                return None
            
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            return None
    
    def _enrich_article_split(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """
        Experimental 3-call split extraction.

        Call 1: Core identification & classification (EXTRACTION_SCHEMA_PART1, ~45 fields).
                attack_chain is here, right after attack_vector, for focused attention.
        Call 2: Deep intelligence (EXTRACTION_SCHEMA_PART2, ~35 fields): timeline with
                event_description, MITRE ATT&CK with all 4 fields, regulatory, financial,
                recovery — the chronically null fields.
        Call 3: Summary (unchanged from _enrich_article).

        Returns the merged result as a (CTIEnrichmentResult, raw_json_data) tuple.
        Falls back to the original _enrich_article() if Part 1 fails.
        """
        if not article_contents:
            return None, None

        primary_url = list(article_contents.keys())[0]
        primary_article = article_contents[primary_url]

        all_text = []
        for url, article in article_contents.items():
            all_text.append(f"[URL: {url}]")
            if article.title:
                all_text.append(f"Title: {article.title}")
            if article.publish_date:
                all_text.append(f"Published: {article.publish_date}")
            if article.author:
                all_text.append(f"Author: {article.author}")
            all_text.append(f"\n{article.content}\n")
        combined_text = "\n".join(all_text)

        MAX_ARTICLE_CHARS = 180_000
        if len(combined_text) > MAX_ARTICLE_CHARS:
            combined_text = combined_text[:MAX_ARTICLE_CHARS] + "\n\n[TRUNCATED]"

        title = primary_article.title or ""

        system_prompt = (
            "You are a Cyber Threat Intelligence Analyst specialising in education sector incidents. "
            "Output ONLY valid JSON. Null for unknown fields. No prose, no markdown."
        )

        # ── Part 1: core extraction ───────────────────────────────────────────
        notes_text = (incident.notes or "").strip()
        is_roundup_stub = notes_text.startswith("Extracted from roundup:")
        known_name = (incident.institution_name or "").strip()
        _UNKNOWN_NAMES = {"unknown", "n/a", "none", "unnamed", "undisclosed", ""}
        if is_roundup_stub and known_name and known_name.lower() not in _UNKNOWN_NAMES:
            target_institution_line = (
                f"\n- TARGET INSTITUTION: {known_name}"
                f"\n  (This article may cover multiple institutions. Extract THIS institution's"
                f" incident as the primary. List all others in other_edu_incidents.)"
            )
        else:
            target_institution_line = ""

        article_metadata_lines = []
        if incident.source_published_date:
            article_metadata_lines.append(f"- Source Published Date: {incident.source_published_date}")
        if primary_article.publish_date:
            article_metadata_lines.append(f"- Article Publish Date: {primary_article.publish_date}")
        article_metadata_block = ("\n" + "\n".join(article_metadata_lines)) if article_metadata_lines else "\n- Article Metadata: unknown"

        # ── GLiNER NER pre-pass ────────────────────────────────────────────────
        try:
            from src.edu_cti.pipeline.phase2.extraction.ner_preprocessor import build_ner_hint_block
            _ner_hint = build_ner_hint_block(combined_text, title)
        except Exception:
            _ner_hint = None
        _ner_block = f"\n\n{_ner_hint}" if _ner_hint else ""

        part1_prompt = PROMPT_TEMPLATE.format(
            url=primary_url,
            title=title,
            target_institution_line=target_institution_line,
            article_metadata_block=article_metadata_block,
            text=combined_text,
        ) + _ner_block

        try:
            _t0 = _time_module.time()
            _metrics.increment("llm_attempt_total", labels={"attempt_num": "1"})
            raw_part1, raw_part1_text = self._extract_json_with_artifacts(
                system_prompt=system_prompt,
                user_prompt=part1_prompt,
                schema=EXTRACTION_SCHEMA_PART1,
                max_retries=2,
            )
            _metrics.observe("llm_duration_seconds", _time_module.time() - _t0)
        except Exception as exc:
            _metrics.observe("llm_duration_seconds", _time_module.time() - _t0)
            logger.warning(f"Split extraction Part 1 failed ({exc}), falling back to single-call")
            return self._enrich_article(incident, article_contents)

        json_part1 = self._parse_json_response(raw_part1)
        if json_part1 is None:
            logger.warning("Split extraction Part 1 returned invalid JSON, falling back")
            return self._enrich_article(incident, article_contents)

        from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import _coerce_llm_scalars
        json_part1 = _coerce_llm_scalars(json_part1)

        coerced_edu = _coerce_bool_like(json_part1.get("is_edu_cyber_incident"))
        if coerced_edu is False:
            json_part1 = _strip_non_education_cti_fields(json_part1)
            result = json_to_cti_enrichment(json_part1, primary_url, incident)
            return result, json_part1

        # ── Part 2: deep intelligence ─────────────────────────────────────────
        def _fmt(v):
            if v is None:
                return "unknown"
            if isinstance(v, list):
                return ", ".join(str(x) for x in v) if v else "unknown"
            return str(v)

        part2_text = combined_text[:60_000]  # smaller cap — model is focused

        # ── IntelEX RAG: retrieve top-5 MITRE techniques for grounding ────────
        try:
            from src.edu_cti.pipeline.phase2.extraction.mitre_rag import build_mitre_rag_block
            _rag_block = build_mitre_rag_block(combined_text)
        except Exception:
            _rag_block = None

        part2_prompt = PART2_PROMPT_TEMPLATE.format(
            institution_name=_fmt(json_part1.get("institution_name")),
            institution_type=_fmt(json_part1.get("institution_type")),
            country=_fmt(json_part1.get("country")),
            attack_category=_fmt(json_part1.get("attack_category")),
            attack_vector=_fmt(json_part1.get("attack_vector")),
            ransomware_family=_fmt(json_part1.get("ransomware_family")),
            data_categories=_fmt(json_part1.get("data_categories")),
            records_affected_exact=_fmt(json_part1.get("records_affected_exact")),
            publication_date=_fmt(json_part1.get("publication_date") or primary_article.publish_date),
            url=primary_url,
            title=title,
            text=part2_text,
        ) + (f"\n\n{_rag_block}" if _rag_block else "")

        json_part2: Dict[str, Any] = {}
        raw_part2_text: Optional[str] = None
        try:
            _t2 = _time_module.time()
            _metrics.increment("llm_attempt_total", labels={"attempt_num": "2"})
            raw_part2, raw_part2_text = self._extract_json_with_artifacts(
                system_prompt=system_prompt,
                user_prompt=part2_prompt,
                schema=EXTRACTION_SCHEMA_PART2,
                max_retries=1,
            )
            _metrics.observe("llm_duration_seconds", _time_module.time() - _t2)
            parsed2 = self._parse_json_response(raw_part2)
            if parsed2 is not None:
                json_part2 = _coerce_llm_scalars(parsed2)
                logger.debug(
                    f"Split Part 2 succeeded: "
                    f"timeline={len(json_part2.get('timeline') or [])}, "
                    f"mitre={len(json_part2.get('mitre_attack_techniques') or [])}, "
                    f"regulators={json_part2.get('regulators_notified')}"
                )
        except Exception as exc:
            logger.warning(f"Split extraction Part 2 failed ({exc}) — continuing with Part 1 only")

        # ── Merge Part 1 + Part 2 (Part 2 overwrites where populated) ────────
        merged = {**json_part1}
        for key, val in json_part2.items():
            if val is not None and val != [] and val != {}:
                merged[key] = val

        # ── Part 3: summary call (unchanged logic) ────────────────────────────
        summary_raw_text: Optional[str] = None
        if coerced_edu is not False and combined_text:
            try:
                _institution = (
                    merged.get("institution_name")
                    or (incident.institution_name if hasattr(incident, "institution_name") else None)
                    or "Unknown institution"
                )
                _attack_cat = merged.get("attack_category") or "unknown"
                _summary_prompt = SUMMARY_PROMPT.format(
                    institution=_institution,
                    attack_category=_attack_cat,
                    text=combined_text[:8000],
                )
                _chat_resp = self.llm_client.chat(
                    messages=[
                        {"role": "system", "content": (
                            "You are a Cyber Threat Intelligence analyst. "
                            "Write a concise 2-3 sentence summary of the cyber incident."
                        )},
                        {"role": "user", "content": _summary_prompt},
                    ],
                    format=None,
                    stream=False,
                    temperature=0.0,
                )
                _summary_text: Optional[str] = None
                if hasattr(_chat_resp, "message") and hasattr(_chat_resp.message, "content"):
                    _summary_text = _chat_resp.message.content
                elif isinstance(_chat_resp, dict):
                    _msg = _chat_resp.get("message", {})
                    _summary_text = (
                        _msg.get("content", "") if isinstance(_msg, dict)
                        else getattr(_msg, "content", None)
                    )
                summary_raw_text = _summary_text
                if _summary_text:
                    _summary_text = _summary_text.strip()
                    if _summary_text.startswith("{"):
                        try:
                            _parsed = json.loads(_summary_text)
                            _summary_text = _parsed.get("enriched_summary", _summary_text)
                        except Exception:
                            pass
                    if len(_summary_text) > 20:
                        merged["enriched_summary"] = _summary_text
            except Exception as _e:
                logger.warning(f"Split extraction summary call failed (non-fatal): {_e}")

        # ── Instructor correction pass (split path) ────────────────────────────
        if should_trigger_correction(merged):
            merged, _corrected = apply_instructor_corrections(
                json_data=merged,
                article_text=combined_text,
                institution_name=merged.get("institution_name") or "Unknown institution",
                ollama_client=self.llm_client,
            )
            if _corrected:
                _metrics.increment("instructor_correction_applied_total")

        merged = _attach_storage_debug(
            json_data=merged,
            llm_client=self.llm_client,
            extraction_mode="split_call",
            raw_llm_responses={
                "part1_extraction": raw_part1_text,
                "part2_extraction": raw_part2_text,
                "summary": summary_raw_text,
            },
        )

        # ── Map to CTIEnrichmentResult ────────────────────────────────────────
        result = json_to_cti_enrichment(merged, primary_url, incident)
        _src_label = getattr(incident, "source_name", None) or "unknown"
        _metrics.increment("llm_education_relevance_pass_total", labels={"source": _src_label})
        _conf = merged.get("confidence_score")
        if isinstance(_conf, (int, float)) and 0.0 <= _conf <= 1.0:
            _metrics.observe("llm_confidence_score", float(_conf))

        return result, merged

    # Backwards compatibility alias
    def enrich_incident_json_schema(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """Alias for _enrich_article for backwards compatibility."""
        return self._enrich_article(incident, article_contents)
