"""
Main LLM enrichment orchestrator for Phase 2.

Simplified pipeline focused on CTI extraction using JSON schema.
Only extracts cyber threat intelligence data from articles.
"""

import logging
import json
import re
from typing import Optional, Dict, List, Tuple, Any

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult
from src.edu_cti.pipeline.phase2.extraction.extraction_schema import EXTRACTION_SCHEMA
from src.edu_cti.pipeline.phase2.extraction.extraction_prompt import PROMPT_TEMPLATE
from src.edu_cti.pipeline.phase2.extraction.json_to_schema_mapper import json_to_cti_enrichment

logger = logging.getLogger(__name__)


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
    
    Handles:
    - Article fetching from URLs
    - Education relevance checking (Yes/No with reasoning)
    - Comprehensive CTI extraction (timeline, MITRE ATT&CK, attack dynamics, impact metrics)
    - Article scoring based on field coverage
    """
    
    def __init__(self, llm_client: Optional[OllamaLLMClient] = None):
        """
        Initialize the enricher.
        
        Args:
            llm_client: Ollama LLM client (required)
        """
        if not llm_client:
            raise ValueError("llm_client is required")
        self.llm_client = llm_client
    
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
            cleanup_non_primary_articles,
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
        
        # Process articles
        if len(article_contents) > 1:
            # Multiple articles - enrich each and select best
            enrichment_result, raw_json_data = self._process_multiple_articles(
                incident, article_contents, skip_if_not_education
            )
        else:
            # Single article
            enrichment_result, raw_json_data = self._enrich_article(
                incident, article_contents
            )
            
            if not enrichment_result:
                logger.warning(f"Failed to enrich incident {incident.incident_id}")
                # Return special marker to indicate enrichment failed (not "not education-related")
                return None, {"_enrichment_failed": True, "_reason": "LLM enrichment failed"}
            
            # Check education relevance - directly from LLM analysis
            if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
                logger.info(
                    f"Skipping incident {incident.incident_id} - not education-related. "
                    f"Reasoning: {enrichment_result.education_relevance.reasoning}"
                )
                # Return marker indicating explicitly not education-related
                return None, {"_not_education_related": True, "_reason": enrichment_result.education_relevance.reasoning}
            
            # Set primary URL
            if not enrichment_result.primary_url and article_contents:
                enrichment_result.primary_url = list(article_contents.keys())[0]
        
        if not enrichment_result:
            return None, None
        
        # Mark primary article in database and cleanup
        if enrichment_result.primary_url:
            conn.execute(
                "UPDATE articles SET is_primary = 0 WHERE incident_id = ?",
                (incident.incident_id,)
            )
            conn.execute(
                "UPDATE articles SET is_primary = 1 WHERE incident_id = ? AND url = ?",
                (incident.incident_id, enrichment_result.primary_url)
            )
            conn.commit()
            
            deleted_count = cleanup_non_primary_articles(conn, incident.incident_id)
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} non-primary articles for {incident.incident_id}")
        
        logger.info(
            f"Enrichment complete for {incident.incident_id} "
            f"(education: {enrichment_result.education_relevance.is_education_related})"
        )
        
        return enrichment_result, raw_json_data
    
    def _process_multiple_articles(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
        skip_if_not_education: bool,
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """Process multiple articles and select the best one based on field coverage."""
        logger.info(
            f"Multiple articles ({len(article_contents)}) for incident {incident.incident_id}. "
            f"Enriching all and selecting best."
        )
        
        article_scores: List[Tuple[str, CTIEnrichmentResult, int, Optional[Dict[str, Any]]]] = []
        
        all_not_education = True  # Track if all articles are not education-related
        
        for idx, (url, article_content) in enumerate(article_contents.items(), 1):
            logger.info(f"[{idx}/{len(article_contents)}] Enriching: {url}")
            try:
                single_article = {url: article_content}
                enrichment_result, raw_json_data = self._enrich_article(incident, single_article)
                
                if enrichment_result:
                    all_not_education = False  # At least one article was enriched
                    
                    # Check education relevance - directly from LLM analysis
                    if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
                        logger.info(f"Skipping {url} - not education-related")
                        continue
                    
                    score = count_filled_fields(enrichment_result)
                    article_scores.append((url, enrichment_result, score, raw_json_data))
                    logger.info(f"✓ {url} - {score} fields filled")
                elif raw_json_data and isinstance(raw_json_data, dict):
                    # Check what kind of failure
                    if raw_json_data.get("_not_education_related"):
                        logger.info(f"⊘ {url} - not education-related")
                    elif raw_json_data.get("_enrichment_failed"):
                        all_not_education = False  # Enrichment failed, not "not education-related"
                        logger.warning(f"✗ Failed to enrich {url}: {raw_json_data.get('_reason', 'unknown')}")
                else:
                    all_not_education = False  # Unknown failure, not "not education-related"
                    logger.warning(f"✗ Failed to enrich {url}")
            except Exception as e:
                all_not_education = False  # Exception, not "not education-related"
                logger.error(f"✗ Error enriching {url}: {e}", exc_info=True)
        
        if not article_scores:
            if all_not_education:
                # All articles were explicitly not education-related
                logger.info(f"All articles for {incident.incident_id} are not education-related")
                return None, {"_not_education_related": True, "_reason": "All articles not education-related"}
            else:
                # Some or all articles failed to enrich (not due to education-relevance)
                logger.warning(f"No articles enriched for {incident.incident_id}")
                return None, {"_enrichment_failed": True, "_reason": "All articles failed to enrich"}
        
        # Select best article
        best_url, best_result, best_score, best_raw_json = max(article_scores, key=lambda x: x[2])
        
        logger.info(f"Selected PRIMARY: {best_url} ({best_score} fields)")
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
            all_text.append(f"\n{article.content}\n")
        
        combined_text = "\n".join(all_text)
        title = primary_article.title or ""
        
        # Use centralized prompt template
        system_prompt = (
            "You are a Cyber Threat Intelligence Analyst. "
            "Output ONLY valid JSON matching the provided schema. "
            "No prose, no explanations, no markdown - pure JSON only."
        )
        
        user_prompt = PROMPT_TEMPLATE.format(
            schema_json=json.dumps(EXTRACTION_SCHEMA, ensure_ascii=False, indent=2),
            url=primary_url,
            title=title,
            text=combined_text
        )
        
        try:
            # Call LLM
            raw_response = self.llm_client.extract_json(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_retries=2
            )
            
            # Parse JSON response
            json_data = self._parse_json_response(raw_response)
            if json_data is None:
                return None, None
            
            # Map to CTIEnrichmentResult
            result = json_to_cti_enrichment(json_data, primary_url, incident)
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
                return json.loads(raw_response)
            except json.JSONDecodeError as e:
                # Try fixing common LLM JSON issues
                fixed_response = raw_response
                
                # Fix 1: LLM sometimes outputs \' which is invalid JSON (should be ')
                fixed_response = fixed_response.replace("\\'", "'")
                
                # Fix 2: Handle double-escaped quotes
                fixed_response = fixed_response.replace('\\"', '"').replace('\\\\', '\\')
                
                # Fix 3: Try with fixed response
                try:
                    return json.loads(fixed_response)
                except json.JSONDecodeError:
                    pass
                
                # Fix 4: Handle leading newline after brace
                if raw_response.startswith('{\n'):
                    fixed = '{' + raw_response[2:].lstrip()
                    try:
                        return json.loads(fixed)
                    except json.JSONDecodeError:
                        pass
                
                logger.error(f"JSON parse error: {e}")
                logger.error(f"Response (first 500 chars): {repr(raw_response[:500])}")
                return None
            
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            return None
    
    # Backwards compatibility alias
    def enrich_incident_json_schema(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """Alias for _enrich_article for backwards compatibility."""
        return self._enrich_article(incident, article_contents)
