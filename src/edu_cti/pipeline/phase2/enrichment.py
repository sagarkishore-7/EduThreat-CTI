"""
Main LLM enrichment orchestrator for Phase 2.

Simplified pipeline focused on CTI extraction - no scoring mechanisms.
Only extracts cyber threat intelligence data from articles.
"""

import logging
from typing import Optional, Dict, List, Tuple, Any

import json
from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
from src.edu_cti.pipeline.phase2.schemas import (
    EducationRelevanceCheck,
    CTIEnrichmentResult,
)
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
        Number of filled fields (excluding required fields like enriched_summary and education_relevance)
    """
    count = 0
    
    # Count top-level optional fields
    if enrichment_result.primary_url:
        count += 1
    if enrichment_result.initial_access_description:
        count += 1
    if enrichment_result.timeline:
        count += 1
    if enrichment_result.mitre_attack_techniques:
        count += 1
    if enrichment_result.attack_dynamics:
        count += 1
    
    # Count nested impact metrics
    if enrichment_result.data_impact:
        count += 1
    if enrichment_result.system_impact:
        count += 1
    if enrichment_result.user_impact:
        count += 1
    if enrichment_result.operational_impact_metrics:
        count += 1
    if enrichment_result.financial_impact:
        count += 1
    if enrichment_result.regulatory_impact:
        count += 1
    if enrichment_result.recovery_metrics:
        count += 1
    if enrichment_result.transparency_metrics:
        count += 1
    if enrichment_result.research_impact:
        count += 1
    
    # Count fields within nested objects
    if enrichment_result.attack_dynamics:
        ad = enrichment_result.attack_dynamics
        if ad.attack_vector:
            count += 1
        if ad.attack_chain:
            count += 1
        if ad.ransomware_family:
            count += 1
        if ad.data_exfiltration is not None:
            count += 1
        if ad.encryption_impact:
            count += 1
        if ad.impact_scope:
            count += 1
        if ad.ransom_demanded is not None:
            count += 1
        if ad.ransom_amount:
            count += 1
        if ad.ransom_paid is not None:
            count += 1
        if ad.recovery_timeframe_days is not None:
            count += 1
        if ad.business_impact:
            count += 1
        if ad.operational_impact:
            count += 1
    
    # Count timeline events
    if enrichment_result.timeline:
        for event in enrichment_result.timeline:
            if event.date:
                count += 1
            if event.event_description:
                count += 1
            if event.event_type:
                count += 1
            if event.actor_attribution:
                count += 1
            if event.indicators:
                count += 1
    
    # Count MITRE techniques
    if enrichment_result.mitre_attack_techniques:
        for tech in enrichment_result.mitre_attack_techniques:
            if tech.technique_id:
                count += 1
            if tech.technique_name:
                count += 1
            if tech.tactic:
                count += 1
            if tech.description:
                count += 1
    
    # Count nested metrics fields (sample key fields from each)
    if enrichment_result.data_impact:
        di = enrichment_result.data_impact
        if di.get("records_affected_exact") is not None or di.get("records_affected_min") is not None:
            count += 1
        if di.get("data_types_affected"):
            count += 1
    
    if enrichment_result.recovery_metrics:
        rm = enrichment_result.recovery_metrics
        if rm.get("recovery_timeframe_days") is not None:
            count += 1
        if rm.get("recovery_started_date"):
            count += 1
        if rm.get("recovery_completed_date"):
            count += 1
    
    if enrichment_result.operational_impact_metrics:
        oim = enrichment_result.operational_impact_metrics
        if oim.get("downtime_days") is not None:
            count += 1
        if oim.get("partial_service_days") is not None:
            count += 1
    
    if enrichment_result.transparency_metrics:
        tm = enrichment_result.transparency_metrics
        if tm.get("disclosure_delay_days") is not None:
            count += 1
        if tm.get("public_disclosure_date"):
            count += 1
    
    return count


class IncidentEnricher:
    """
    Main orchestrator for enriching CTI incidents with LLM.
    
    Handles:
    - Article fetching from URLs
    - Education relevance checking (Yes/No with reasoning)
    - Comprehensive CTI extraction (timeline, MITRE ATT&CK, attack dynamics, impact metrics)
    - Initial access description (how attacker gained access)
    - Article scoring based on field coverage
    """
    
    def __init__(
        self,
        llm_client: Optional[OllamaLLMClient] = None,
    ):
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
    ) -> tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
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
        
        # Import here to avoid circular dependencies
        from src.edu_cti.pipeline.phase2.storage.article_storage import (
            get_all_articles_for_incident,
        )
        
        # Get articles from database
        all_articles_data = get_all_articles_for_incident(conn, incident.incident_id)
        
        if not all_articles_data:
            logger.warning(
                f"No articles found in DB for incident {incident.incident_id}. "
                f"Articles must be fetched and stored first."
            )
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
        
        # If multiple articles, enrich each individually and score them
        if len(article_contents) > 1:
            logger.info(
                f"Multiple articles ({len(article_contents)}) found for incident {incident.incident_id}. "
                f"Enriching all articles and selecting the one with most filled fields as primary."
            )
            
            article_scores: List[Tuple[str, CTIEnrichmentResult, int, Optional[Dict[str, Any]]]] = []
            
            for idx, (url, article_content) in enumerate(article_contents.items(), 1):
                logger.info(f"[{idx}/{len(article_contents)}] Enriching article: {url}")
                try:
                    # Enrich this single article
                    single_article_dict = {url: article_content}
                    enrichment_result, raw_json_data = self.enrich_incident_json_schema(
                        incident=incident,
                        article_contents=single_article_dict,
                    )
                    # Fallback to comprehensive if JSON schema fails
                    if enrichment_result is None:
                        logger.warning(f"JSON schema enrichment failed for {url}, trying comprehensive method...")
                        enrichment_result = self.enrich_incident_comprehensive(
                            incident=incident,
                            article_contents=single_article_dict,
                        )
                        raw_json_data = None  # Comprehensive method doesn't return raw JSON
                    
                    if enrichment_result:
                        # Check education relevance first
                        if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
                            logger.info(f"Skipping article {url} - not education-related")
                            continue
                        
                        # Score this article based on field coverage
                        score = count_filled_fields(enrichment_result)
                        article_scores.append((url, enrichment_result, score, raw_json_data))
                        logger.info(
                            f"✓ Article {url} enriched successfully - filled {score} fields. "
                            f"Education-related: {enrichment_result.education_relevance.is_education_related}"
                        )
                    else:
                        logger.warning(f"✗ Failed to enrich article {url} (both methods failed)")
                except Exception as e:
                    logger.error(f"✗ Error enriching article {url}: {e}", exc_info=True)
                    continue
            
            if not article_scores:
                logger.warning(
                    f"No articles successfully enriched for incident {incident.incident_id}. "
                    f"Tried {len(article_contents)} articles."
                )
                return None, None
            
            # Select article with highest field coverage score
            best_url, best_result, best_score, best_raw_json = max(article_scores, key=lambda x: x[2])
            
            # Log detailed comparison
            logger.info("=" * 80)
            logger.info(f"ARTICLE SELECTION SUMMARY for incident {incident.incident_id}")
            logger.info("=" * 80)
            logger.info(f"Selected PRIMARY article: {best_url}")
            logger.info(f"  Field coverage score: {best_score} fields")
            logger.info(f"  Education-related: {best_result.education_relevance.is_education_related}")
            logger.info("")
            logger.info("All articles compared:")
            for url, result, score, _ in sorted(article_scores, key=lambda x: x[2], reverse=True):
                marker = "✓ PRIMARY" if url == best_url else "  "
            logger.info(
                    f"{marker} {url}: {score} fields filled "
                    f"(edu: {result.education_relevance.is_education_related})"
            )
            logger.info("=" * 80)
            
            # Set primary URL
            best_result.primary_url = best_url
            enrichment_result = best_result
            raw_json_data = best_raw_json
            
        else:
            # Single article - enrich it directly
            logger.info(
                f"Single article found for incident {incident.incident_id}. Enriching..."
            )
            enrichment_result, raw_json_data = self.enrich_incident_json_schema(
                incident=incident,
                article_contents=article_contents,
            )
            # Fallback to comprehensive if JSON schema fails
            if enrichment_result is None:
                enrichment_result = self.enrich_incident_comprehensive(
                    incident=incident,
                    article_contents=article_contents,
                )
                raw_json_data = None  # Comprehensive method doesn't return raw JSON
            
            if not enrichment_result:
                logger.warning(f"Failed to enrich incident {incident.incident_id}")
                return None, None
            
            # Check education relevance
            if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
                logger.info(
                    f"Skipping incident {incident.incident_id} - not education-related. "
                    f"Reasoning: {enrichment_result.education_relevance.reasoning}"
                )
                return None, None
            
            # Set primary URL
            if not enrichment_result.primary_url and article_contents:
                enrichment_result.primary_url = list(article_contents.keys())[0]
        
        # Mark primary article in database
        if enrichment_result.primary_url:
            from src.edu_cti.pipeline.phase2.storage.article_storage import cleanup_non_primary_articles
            # Mark the primary URL
            conn.execute(
                "UPDATE articles SET is_primary = 0 WHERE incident_id = ?",
                (incident.incident_id,)
            )
            conn.execute(
                "UPDATE articles SET is_primary = 1 WHERE incident_id = ? AND url = ?",
                (incident.incident_id, enrichment_result.primary_url)
            )
            conn.commit()
            
            # Clean up non-primary articles
            deleted_count = cleanup_non_primary_articles(conn, incident.incident_id)
            if deleted_count > 0:
                logger.info(
                    f"Cleaned up {deleted_count} non-primary articles for incident {incident.incident_id}. "
                    f"Only primary article ({enrichment_result.primary_url}) remains."
                )
        
        logger.info(
            f"Enrichment complete for incident {incident.incident_id} "
            f"(education: {enrichment_result.education_relevance.is_education_related})"
        )
        
        return enrichment_result, raw_json_data
    
    def enrich_incident_json_schema(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> tuple[Optional[CTIEnrichmentResult], Optional[Dict[str, Any]]]:
        """
        Perform CTI enrichment using JSON schema-based extraction.
        
        This method uses a JSON schema approach similar to the example provided,
        then maps the response to CTIEnrichmentResult.
        
        Args:
            incident: BaseIncident to enrich
            article_contents: Dictionary mapping URL to ArticleContent
            
        Returns:
            Tuple of (CTIEnrichmentResult, raw_json_data) with all extracted information,
            or (None, None) on error. raw_json_data contains the original JSON response
            from LLM for extracting fields like country/region/city that aren't in CTIEnrichmentResult.
        """
        if not article_contents:
            return None
        
        # Use the first article as primary
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
        
        # Get title from article
        title = primary_article.title or ""
        
        # Prepare prompt
        system_prompt = "You are a Cyber Threat Intelligence Analyst. Output ONLY valid JSON matching the provided schema. No prose, no explanations, no markdown - pure JSON only."
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
            
            # Parse JSON response - try to extract JSON from markdown code blocks if present
            try:
                # Clean up the response - remove leading/trailing whitespace and newlines
                raw_response = raw_response.strip()
                
                # Try to extract JSON from markdown code blocks
                import re
                json_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', raw_response, re.DOTALL)
                if json_match:
                    raw_response = json_match.group(1).strip()
                else:
                    # Try to find JSON object directly (from first { to last })
                    json_match = re.search(r'\{.*\}', raw_response, re.DOTALL)
                    if json_match:
                        raw_response = json_match.group(0).strip()
                
                # Remove any leading/trailing whitespace or newlines
                raw_response = raw_response.strip()
                
                # Check if newlines are escaped as literal \n (backslash + n) instead of actual newlines
                # This happens when the LLM response is double-encoded
                if raw_response.startswith('{\\n') or raw_response.startswith('{\\\\n'):
                    logger.debug("Detected escaped newlines in JSON - unescaping...")
                    # Replace literal \n with actual newlines
                    raw_response = raw_response.replace('\\n', '\n').replace('\\r', '\r')
                    # Also handle other common escape sequences
                    raw_response = raw_response.replace('\\t', '\t')
                
                # Debug: log the first few characters to understand the format
                logger.debug(f"JSON to parse (first 50 chars repr): {repr(raw_response[:50])}")
                
                # Try parsing
                try:
                    json_data = json.loads(raw_response)
                except json.JSONDecodeError as parse_error:
                    logger.debug(f"JSON parse error at position {parse_error.pos}: {parse_error}")
                    # If it fails, try removing newline after opening brace (for actual newlines)
                    if raw_response.startswith('{\n'):
                        logger.debug("Trying to fix JSON by removing newline after opening brace")
                        raw_response = '{' + raw_response[2:].lstrip()
                        try:
                            json_data = json.loads(raw_response)
                            logger.info("Successfully fixed JSON by removing newline after opening brace")
                        except json.JSONDecodeError as e2:
                            logger.error(f"Still failed after fix: {e2}")
                            raise parse_error  # Raise original error
                    else:
                        raise parse_error
            except (json.JSONDecodeError, AttributeError) as e:
                logger.error(f"Failed to parse JSON response: {e}")
                logger.error(f"Raw response (first 500 chars): {repr(raw_response[:500])}")
                logger.error(f"Raw response length: {len(raw_response)}")
                # Try to see what's at the problematic position
                if len(raw_response) > 1:
                    logger.error(f"First 10 chars (repr): {repr(raw_response[:10])}")
                # Try fallback to comprehensive method
                logger.info("Falling back to comprehensive enrichment method...")
                return None, None  # Return tuple (None, None) to match expected return type
            
            # Map to CTIEnrichmentResult
            result = json_to_cti_enrichment(json_data, primary_url, incident)
            
            return result, json_data
            
        except Exception as e:
            logger.error(f"Error during JSON schema extraction: {e}", exc_info=True)
            return None, None
    
    def enrich_incident_comprehensive(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Optional[CTIEnrichmentResult]:
        """
        Perform comprehensive CTI enrichment for a single article or multiple articles.
        
        Extracts:
        1. Education relevance check (Yes/No with reasoning)
        2. Initial access description (how attacker gained access - 1-3 sentences)
        3. Complete CTI extraction (timeline, MITRE ATT&CK, attack dynamics, impact metrics)
        
        Args:
            incident: BaseIncident to enrich
            article_contents: Dictionary mapping URL to ArticleContent (can be single or multiple)
            
        Returns:
            CTIEnrichmentResult with all extracted information, or None on error
        """
        if not article_contents:
            return None
        
        # Build comprehensive context with all articles
        context_parts = [
            "=" * 80,
            "INCIDENT INFORMATION",
            "=" * 80,
            f"Incident ID: {incident.incident_id}",
            f"Title: {incident.title or 'N/A'}",
            f"Subtitle: {incident.subtitle or 'N/A'}",
            f"University/Institution Name: {incident.university_name}",
            f"Institution Type: {incident.institution_type or 'Unknown'}",
            f"Country: {incident.country or 'Unknown'}",
            f"Incident Date: {incident.incident_date or 'Unknown'}",
            f"Attack Type Hint: {incident.attack_type_hint or 'Unknown'}",
            "",
            "=" * 80,
            "ARTICLE CONTENT",
            "=" * 80,
        ]
        
        # Add all articles with their URLs and metadata
        publication_dates = []
        for idx, (url, article) in enumerate(article_contents.items(), 1):
            context_parts.append(f"\n--- Article {idx} ---")
            context_parts.append(f"URL: {url}")
            context_parts.append(f"Title: {article.title or 'N/A'}")
            if article.author:
                context_parts.append(f"Author: {article.author}")
            if article.publish_date:
                context_parts.append(f"PUBLICATION DATE: {article.publish_date}")
                publication_dates.append(f"Article {idx}: {article.publish_date}")
            else:
                context_parts.append("PUBLICATION DATE: Not available")
            # Include full article content
            content_preview = article.content[:12000] if len(article.content) > 12000 else article.content
            if len(article.content) > 12000:
                context_parts.append(f"\nContent (first 12000 chars of {len(article.content)} total):\n{content_preview}")
                context_parts.append(f"\n[... {len(article.content) - 12000} more characters truncated ...]")
            else:
                context_parts.append(f"\nContent:\n{content_preview}")
            context_parts.append("")
        
        # Build date resolution instructions
        date_resolution_instructions = ""
        if publication_dates:
            date_resolution_instructions = f"""
CRITICAL DATE RESOLUTION INSTRUCTIONS:
The articles above contain publication dates. When extracting timeline dates, you MUST:
1. Use the article publication date(s) as a reference point to resolve relative dates
2. Convert relative dates like "last Friday", "yesterday", "last week", "two weeks ago" to absolute dates (YYYY-MM-DD)
3. Example: If article says "last Friday" and the article was published on 2025-11-19, then "last Friday" = 2025-11-14
4. Publication dates available:
{chr(10).join('   - ' + pd for pd in publication_dates)}
5. If multiple publication dates exist, use the earliest one as the reference point
6. All timeline dates MUST be in YYYY-MM-DD format (ISO 8601)
7. If you cannot determine an exact date, use approximate dates but set date_precision to 'approximate', 'month', or 'year' accordingly
8. NEVER use relative dates in the timeline - always convert to absolute dates using publication dates as reference

"""
        else:
            date_resolution_instructions = """
DATE RESOLUTION INSTRUCTIONS:
- Article publication dates are not available in the metadata
- Still try to extract dates from the article content itself
- Convert any relative dates to the best approximation possible
- All timeline dates MUST be in YYYY-MM-DD format (ISO 8601)
- If only approximate dates are available, set date_precision accordingly

"""
        
        context = "\n".join(context_parts)
        
        # System prompt - focused on CTI extraction only
        system_prompt = """You are an expert cyber threat intelligence analyst specializing in education sector incidents.

CRITICAL: You MUST respond with valid JSON that matches the provided schema exactly. Fill only fields that are mentioned in the article, set all others to null/None.

Your task is to extract comprehensive CTI data from the article(s). You must:
1. EDUCATION RELEVANCE CHECK (MOST IMPORTANT):
   - Determine if this is an ACTUAL SECURITY INCIDENT/ATTACK affecting education sector institutions
   - Set is_education_related = true ONLY if:
     * It is a REAL security incident: data breach, ransomware attack, phishing, malware, DDoS, credential theft, etc.
     * AND it affects an educational institution (universities, colleges, K-12 schools, research institutions)
   - Set is_education_related = false if:
     * It's about banks, financial institutions, or other non-education entities (even if research/university is mentioned)
     * It's a competition, achievement, award, training, or educational program
     * It mentions universities/research but describes NO actual security incident
     * You are unsure - default to false
   - Provide clear reasoning (1-2 sentences) explaining your decision
   - Identify institution name if mentioned

2. INITIAL ACCESS DESCRIPTION:
   - Extract 1-3 sentences describing how the attacker gained initial access (if mentioned in article)
   - Examples: "Attacker gained access through phishing email", "Vulnerability in web application", "Compromised credentials", etc.
   - If not mentioned, set to null

3. CTI EXTRACTION:
   - Extract timeline of events (only if dates/events are mentioned)
   - Extract MITRE ATT&CK techniques (only if techniques are identified)
   - Extract attack dynamics (attack vector, kill chain, ransomware family, etc.)
   - Extract impact metrics (data impact, system impact, user impact, etc.) - only if mentioned
   - Extract recovery metrics, regulatory impact, transparency metrics - only if mentioned
   - Fill fields ONLY if explicitly mentioned in article - set all others to null
   - DO NOT create empty arrays - use null instead
   - DO NOT use "unknown", "N/A", or placeholders - use null

SYSTEM IMPACT EXTRACTION (CRITICAL):
   - If the article mentions ANY systems, services, or infrastructure that were affected, disabled, compromised, or disrupted, you MUST extract this into system_impact.systems_affected
   - Examples of system mentions to extract:
     * "email system was down" → systems_affected: ["email_system"]
     * "disabled internet, VoIP phones, email, and financial software" → systems_affected: ["network_infrastructure", "email_system", "financial_systems"]
     * "student portal unavailable" → systems_affected: ["student_portal"]
     * "network was compromised" → systems_affected: ["network_infrastructure"], network_compromised: true
     * "backup systems deleted" → systems_affected: ["backup_systems"]
   - Map common system names to standardized tags:
     * "email", "email system", "mail server" → "email_system"
     * "internet", "network", "network infrastructure" → "network_infrastructure"
     * "VoIP", "phone system", "telephony" → "other" (or specific tag if available)
     * "financial software", "financial systems", "accounting system" → "financial_systems"
     * "student portal", "student information system", "SIS" → "student_portal"
     * "backup", "backup systems", "backups" → "backup_systems"
   - If systems are mentioned but don't match exact tags, use the closest match or "other"
   - Set system_impact.critical_systems_affected = true if critical systems were mentioned as affected
   - Set system_impact.network_compromised = true if network/infrastructure was mentioned as compromised
   - Set individual boolean fields (email_system_affected, network_compromised, etc.) based on what was mentioned

RANSOM DATA EXTRACTION (CRITICAL):
   - If the article mentions ANY ransom-related information, you MUST extract this into attack_dynamics and/or financial_impact
   - Examples of ransom mentions to extract:
     * "paid $400,000 ransom" → ransom_demanded: true, ransom_amount: "$400,000" or 400000, ransom_paid: true
     * "ransom of $500,000 was demanded" → ransom_demanded: true, ransom_amount: "$500,000" or 500000, ransom_paid: false or null
     * "refused to pay the ransom" → ransom_demanded: true, ransom_paid: false
     * "negotiated ransom down to $300,000" → ransom_demanded: true, ransom_amount: "$300,000" or 300000
   - Extract into attack_dynamics if mentioned in general attack context:
     * ransom_demanded: true/false (if ransom was demanded)
     * ransom_amount: string or number (amount mentioned, e.g., "$400,000" or 400000)
     * ransom_paid: true/false (if ransom was paid)
   - Extract into financial_impact for detailed financial information:
     * financial_impact.ransom_demanded: true/false
     * financial_impact.ransom_amount_exact: number (convert to USD if currency mentioned, e.g., "$400,000" → 400000)
     * financial_impact.ransom_paid: true/false
     * financial_impact.ransom_paid_amount: number (actual amount paid, convert to USD)
     * financial_impact.ransom_currency: string (if currency mentioned, e.g., "USD", "BTC")
   - If article says "paid $400,000 ransom", extract:
     * attack_dynamics.ransom_demanded: true
     * attack_dynamics.ransom_amount: "$400,000" or 400000
     * attack_dynamics.ransom_paid: true
     * financial_impact.ransom_paid: true
     * financial_impact.ransom_paid_amount: 400000
   - If only general mention (e.g., "ransomware attack"), you may not have specific ransom data - set to null
   - If specific amounts or payment status is mentioned, you MUST extract it

CRITICAL - USE STANDARDIZED TAGS (DO NOT PARAPHRASE):
   - attack_vector: Use exact tags like "phishing", "vulnerability_exploit", "credential_stuffing", "ransomware", etc. (NOT "phishing email" or "via phishing")
   - encryption_impact: Use "full", "partial", or "none" (NOT "fully encrypted" or "partially encrypted")
   - business_impact: Use "critical", "severe", "moderate", "limited", or "minimal" (NOT "very severe" or "moderately severe")
   - event_type: Use exact tags like "initial_access", "discovery", "impact", "containment", "recovery", etc.
   - attack_chain: Use exact tags like "reconnaissance", "weaponization", "delivery", "exploitation", etc.
   - operational_impact: Use exact tags like "teaching_disrupted", "research_disrupted", "email_system_down", etc.
   - systems_affected: Use exact tags like "email_system", "student_portal", "network_infrastructure", etc.
   - recovery_phases: Use exact tags like "containment", "eradication", "recovery", "lessons_learned"
   - date_precision: Use "day", "month", "year", or "approximate"

4. RESPONSE MEASURES (CRITICAL - USE STANDARDIZED TAGS):
   - Extract response measures taken during/after the incident
   - Use EXACT standardized tags from the list - DO NOT paraphrase or create variations
   - Standardized tags: password_reset, account_lockout, credential_rotation, backup_restoration, system_rebuild, network_isolation, endpoint_containment, malware_removal, patch_application, vulnerability_remediation, access_revocation, incident_response_team, forensics_investigation, law_enforcement_notification, regulatory_notification, user_notification, public_disclosure, security_audit, penetration_testing, security_training, mfa_implementation, network_segmentation, firewall_update, ids_ips_deployment, monitoring_enhancement, other
   - Examples:
     * "passwords were reset" → use tag: "password_reset" (NOT "passwords were reset" or "reset password")
     * "restored from backup" → use tag: "backup_restoration" (NOT "restored from backup" or "backup restore")
     * "implemented MFA" → use tag: "mfa_implementation" (NOT "implemented MFA" or "MFA enabled")
   - If a measure doesn't match any tag, use "other"
   - Return as a list of tags (e.g., ["password_reset", "user_notification"])

5. TIME CONVERSION (CRITICAL):
   - ALL time-related fields MUST be converted to DAYS (float)
   - Examples:
     * "2 weeks" → 14.0 days
     * "1 month" → 30.0 days (or 31.0 if specific month mentioned)
     * "3 days" → 3.0 days
     * "48 hours" → 2.0 days
     * "1 year" → 365.0 days
   - Fields that need conversion: recovery_timeframe_days, downtime_days, partial_service_days, recovery_timeframe_days (in RecoveryMetrics), disclosure_delay_days, backup_age_days
   - If time is mentioned but unit is unclear, estimate to days (e.g., "several days" → estimate like 5.0 days)
   - If time is not mentioned, set to null

IMPORTANT:
- Respond with valid JSON matching the schema - no markdown, no code blocks, just pure JSON
- Synthesize information from ALL articles if multiple are provided
- All dates in timeline must be in ISO 8601 format (YYYY-MM-DD)
- Use article publish_date metadata to resolve relative dates
- ONLY fill fields explicitly mentioned in the article - set all others to null/None
- Convert ALL time values to DAYS (float) for consistency"""

        user_prompt = f"""Analyze the following incident and articles. Extract cyber threat intelligence data.

CRITICAL: Respond with valid JSON only - no markdown, no code blocks, no explanations. Just the JSON object.

1. EDUCATION RELEVANCE CHECK (MOST IMPORTANT):
   - Ask yourself: "Is this article about a REAL security incident/attack affecting an educational institution?"
   - Set is_education_related = true ONLY if it's an actual security incident affecting education sector
   - Set is_education_related = false otherwise (competitions, achievements, non-education entities, etc.)
   - Provide clear reasoning (1-2 sentences)
   - Example: Article about "Azerbaijani banks processing fake antivirus payments" → is_education_related = false

2. INITIAL ACCESS DESCRIPTION:
   - Extract 1-3 sentences on how attacker gained access (if mentioned)
   - If not mentioned, set to null

3. CTI EXTRACTION:
   - Extract timeline, MITRE ATT&CK techniques, attack dynamics, impact metrics
   - Fill fields ONLY if explicitly mentioned - set all others to null
   - DO NOT create empty arrays - use null instead
   - USE STANDARDIZED TAGS for: attack_vector, encryption_impact, business_impact, event_type, attack_chain, operational_impact, systems_affected, recovery_phases, date_precision
   - DO NOT paraphrase - use exact tags from schema (e.g., "phishing" not "phishing email", "critical" not "very critical")

SYSTEM IMPACT EXTRACTION (CRITICAL):
   - If the article mentions ANY systems, services, or infrastructure affected/disabled/compromised, extract into system_impact.systems_affected
   - Examples: "email was down" → ["email_system"], "disabled internet and email" → ["network_infrastructure", "email_system"]
   - Map to standardized tags: email→email_system, internet/network→network_infrastructure, financial software→financial_systems, student portal→student_portal, backup→backup_systems
   - Set boolean fields (email_system_affected, network_compromised, etc.) based on what was mentioned

RANSOM DATA EXTRACTION (CRITICAL):
   - If article mentions ransom amounts, payment status, or negotiations, extract into attack_dynamics and financial_impact
   - Examples: "paid $400,000" → ransom_demanded: true, ransom_amount: "$400,000", ransom_paid: true
   - Extract financial_impact.ransom_paid_amount as number (convert "$400,000" → 400000)
   - If specific ransom details mentioned, you MUST extract them

4. RESPONSE MEASURES:
   - Extract response measures using EXACT standardized tags
   - Use tags like: password_reset, backup_restoration, mfa_implementation, user_notification, etc.
   - DO NOT paraphrase - use exact tags from the schema
   - Example: "passwords were reset" → tag: "password_reset" (not "passwords were reset")

5. TIME CONVERSION:
   - Convert ALL time values to DAYS (float)
   - "2 weeks" → 14.0, "1 month" → 30.0, "48 hours" → 2.0, etc.
   - If not mentioned, set to null

{date_resolution_instructions}
{context}

RESPONSE FORMAT:
- Return ONLY valid JSON matching the schema
- Fill only fields mentioned in the article
- Set all other fields to null (not empty arrays, not "unknown", just null)
- Convert ALL time values to DAYS (float)
- Required fields: enriched_summary (string), education_relevance (object with is_education_related: bool, reasoning: string, institution_identified: string or null)
- All other fields can be null if not mentioned"""
        
        try:
            result = self.llm_client.extract_structured(
                prompt=user_prompt,
                schema_model=CTIEnrichmentResult,
                system_prompt=system_prompt,
            )
            
            # Ensure primary_url is set (use first article if not specified)
            if not result.primary_url and article_contents:
                result.primary_url = list(article_contents.keys())[0]
            
            return result
            
        except Exception as e:
            logger.error(f"Error during comprehensive CTI extraction: {e}")
            # Return None on error - don't create fake enrichment data
            return None
