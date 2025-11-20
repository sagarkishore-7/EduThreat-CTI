"""
Main LLM enrichment orchestrator for Phase 2.

Coordinates article fetching, education relevance checking, URL scoring,
and comprehensive CTI extraction.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.article_fetcher import ArticleFetcher, ArticleContent
from src.edu_cti.pipeline.phase2.schemas import (
    EducationRelevanceCheck,
    URLConfidenceScore,
    CTIEnrichmentResult,
)
from src.edu_cti.pipeline.phase2.metadata_extractor import MetadataExtractor, MetadataCoverage

logger = logging.getLogger(__name__)


class IncidentEnricher:
    """
    Main orchestrator for enriching CTI incidents with LLM.
    
    Handles:
    - Article fetching from URLs
    - Education relevance checking
    - URL confidence scoring and selection
    - Comprehensive CTI extraction (timeline, MITRE ATT&CK, attack dynamics)
    - Incremental processing (only process non-enriched incidents)
    """
    
    def __init__(
        self,
        llm_client: Optional[OllamaLLMClient] = None,
        article_fetcher: Optional[ArticleFetcher] = None,
        metadata_extractor: Optional[MetadataExtractor] = None,
        hybrid_scoring_weight: float = 0.6,
    ):
        """
        Initialize the enricher.
        
        Args:
            llm_client: Ollama LLM client (creates default if not provided)
            article_fetcher: Article fetcher (creates default if not provided)
            metadata_extractor: Metadata extractor for schema coverage analysis
            hybrid_scoring_weight: Weight for LLM confidence vs metadata coverage (0.0-1.0)
        """
        self.llm_client = llm_client
        self.article_fetcher = article_fetcher or ArticleFetcher()
        self.metadata_extractor = metadata_extractor or MetadataExtractor()
        self.hybrid_scoring_weight = hybrid_scoring_weight
        
        if not self.llm_client:
            raise ValueError("llm_client is required")
    
    def process_incident(
        self,
        incident: BaseIncident,
        skip_if_not_education: bool = True,
        conn=None,
    ) -> Optional[CTIEnrichmentResult]:
        """
        Process an incident through the LLM enrichment pipeline.
        
        Reads articles from database (must be fetched and stored beforehand).
        
        Args:
            incident: BaseIncident to enrich
            skip_if_not_education: If True, skip incidents not related to education
            conn: Database connection (required to read articles from DB)
            
        Returns:
            CTIEnrichmentResult if incident is enriched, None otherwise
        """
        if conn is None:
            raise ValueError("Database connection required to read articles from DB")
        
        # Import here to avoid circular dependencies
        from src.edu_cti.pipeline.phase2.article_storage import (
            get_primary_article,
            get_all_articles_for_incident,
        )
        
        # Step 1: Get articles from database
        all_articles_data = get_all_articles_for_incident(conn, incident.incident_id)
        
        if not all_articles_data:
            logger.warning(
                f"No articles found in DB for incident {incident.incident_id}. "
                f"Articles must be fetched and stored first."
            )
            return None
        
        # Filter to only successful articles with content
        valid_articles_data = [
            art for art in all_articles_data
            if art["fetch_successful"] and art["content"] and len(art["content"].strip()) > 50
        ]
        
        if not valid_articles_data:
            logger.warning(f"No valid articles found in DB for incident {incident.incident_id}")
            return None
        
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
        
        # SINGLE comprehensive LLM call for all enrichment tasks
        # The LLM will receive ALL articles and evaluate them all
        logger.info(
            f"Performing comprehensive LLM enrichment for incident {incident.incident_id} "
            f"using ALL {len(article_contents)} articles from DB. "
            f"The LLM will evaluate and score all URLs, then select the best one."
        )
        enrichment_result = self.enrich_incident_comprehensive(
            incident=incident,
            article_contents=article_contents,
        )
        
        if not enrichment_result:
            logger.warning(f"Failed to enrich incident {incident.incident_id}")
            return None
        
        # Check education relevance if we should skip non-education incidents
        if skip_if_not_education and not enrichment_result.education_relevance.is_education_related:
            logger.info(
                f"Skipping incident {incident.incident_id} - not education-related "
                f"(confidence: {enrichment_result.education_relevance.confidence:.2f})"
            )
            return None
        
        # Update article scores and primary selection in DB based on LLM evaluation
        if enrichment_result.url_scores:
            from src.edu_cti.pipeline.phase2.article_storage import (
                update_article_scores_from_llm,
                cleanup_non_primary_articles,
            )
            update_article_scores_from_llm(
                conn=conn,
                incident_id=incident.incident_id,
                url_scores=enrichment_result.url_scores,
                primary_url=enrichment_result.primary_url,
            )
            
            # Log the LLM's URL evaluation
            logger.info(f"LLM evaluated {len(enrichment_result.url_scores)} URLs:")
            for score in sorted(enrichment_result.url_scores, key=lambda s: s.confidence_score, reverse=True):
                is_selected = score.url == enrichment_result.primary_url
                marker = "★" if is_selected else " "
                logger.info(
                    f"  {marker} {score.url}: {score.confidence_score:.2f} "
                    f"({score.article_quality}, {score.content_completeness}, {score.source_reliability})"
                )
            
            # Clean up: Keep only primary article, delete all others
            deleted_count = cleanup_non_primary_articles(conn, incident.incident_id)
            if deleted_count > 0:
                logger.info(
                    f"Cleaned up {deleted_count} non-primary articles for incident {incident.incident_id}. "
                    f"Only primary article ({enrichment_result.primary_url}) remains in database."
                )
        else:
            # Fallback if LLM didn't provide scores
            if enrichment_result.primary_url:
                primary_url = enrichment_result.primary_url
            else:
                primary_url = list(article_contents.keys())[0]
            
            enrichment_result.primary_url = primary_url
            
            # Still clean up non-primary articles even if no scores
            from src.edu_cti.pipeline.phase2.article_storage import cleanup_non_primary_articles
            # Mark the fallback URL as primary first
            conn.execute(
                "UPDATE articles SET is_primary = 0 WHERE incident_id = ?",
                (incident.incident_id,)
            )
            conn.execute(
                "UPDATE articles SET is_primary = 1 WHERE incident_id = ? AND url = ?",
                (incident.incident_id, primary_url)
            )
            conn.commit()
            
            # Clean up non-primary articles
            deleted_count = cleanup_non_primary_articles(conn, incident.incident_id)
            if deleted_count > 0:
                logger.info(
                    f"Cleaned up {deleted_count} non-primary articles for incident {incident.incident_id}. "
                    f"Only primary article ({primary_url}) remains in database."
                )
            
            logger.warning(
                f"LLM did not provide URL scores - using {primary_url} as primary for incident {incident.incident_id}"
            )
        
        logger.info(
            f"Enrichment complete for incident {incident.incident_id} "
            f"(confidence: {enrichment_result.extraction_confidence:.2f}, "
            f"education: {enrichment_result.education_relevance.is_education_related})"
        )
        
        return enrichment_result
    
    def check_education_relevance(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> EducationRelevanceCheck:
        """
        Check if an incident is education-related using LLM.
        
        Args:
            incident: BaseIncident to check
            article_contents: Dictionary mapping URL to ArticleContent
            
        Returns:
            EducationRelevanceCheck result
        """
        # Build context from incident and articles
        context_parts = [
            f"Incident Title: {incident.title or 'N/A'}",
            f"Subtitle: {incident.subtitle or 'N/A'}",
            f"University Name: {incident.university_name}",
            f"Institution Type: {incident.institution_type or 'Unknown'}",
            f"Country: {incident.country or 'Unknown'}",
        ]
        
        # Add article summaries
        if article_contents:
            context_parts.append("\nArticle Content:")
            for url, article in article_contents.items():
                if article.fetch_successful and article.content:
                    # Use first 2000 chars per article to stay within token limits
                    content_snippet = article.content[:2000] + "..." if len(article.content) > 2000 else article.content
                    context_parts.append(f"\nURL: {url}\nTitle: {article.title}\nContent: {content_snippet}")
        
        context = "\n".join(context_parts)
        
        prompt = f"""Analyze the following cyber incident information and determine if it is related to the education sector.

Context:
{context}

Determine if this incident affects:
- Universities or colleges
- Schools (K-12)
- Research institutions
- Educational organizations

Return your assessment with confidence."""
        
        try:
            result = self.llm_client.extract_structured(
                prompt=prompt,
                schema_model=EducationRelevanceCheck,
            )
            return result
        except Exception as e:
            logger.error(f"Error checking education relevance: {e}")
            # Default to education-related if we can't determine
            return EducationRelevanceCheck(
                is_education_related=True,
                confidence=0.5,
                reasoning=f"Error during relevance check: {str(e)}",
                institution_name=incident.university_name,
            )
    
    def score_urls(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> List[URLConfidenceScore]:
        """
        Score URLs based on LLM assessment and metadata coverage.
        
        Uses hybrid scoring:
        - LLM confidence (subjective assessment)
        - Metadata coverage (objective analysis of schema field coverage)
        
        Args:
            incident: BaseIncident being enriched
            article_contents: Dictionary mapping URL to ArticleContent
            
        Returns:
            List of URLConfidenceScore objects, sorted by confidence (highest first)
        """
        scores = []
        
        for url, article in article_contents.items():
            if not article.fetch_successful or not article.content:
                continue
            
            try:
                # LLM-based confidence scoring
                llm_prompt = f"""Evaluate the quality and completeness of this article for extracting cyber threat intelligence about an education sector incident.

Article Title: {article.title or 'N/A'}
Article Content (first 3000 chars): {article.content[:3000]}

Assess:
1. Article quality (high/medium/low)
2. Content completeness (complete/partial/minimal)
3. Source reliability (reliable/moderate/unreliable)
4. Overall confidence for CTI extraction (0.0-1.0)

Provide a confidence score and reasoning."""
                
                llm_score = self.llm_client.extract_structured(
                    prompt=llm_prompt,
                    schema_model=URLConfidenceScore,
                )
                
                # Metadata coverage analysis (objective)
                coverage = self.metadata_extractor.analyze_coverage(
                    incident=incident,
                    article_content=article,
                )
                
                # Hybrid scoring: combine LLM confidence with metadata coverage
                llm_confidence = llm_score.confidence_score
                metadata_coverage = coverage.coverage_score
                
                hybrid_confidence = (
                    self.hybrid_scoring_weight * llm_confidence +
                    (1 - self.hybrid_scoring_weight) * metadata_coverage
                )
                
                # Create final score
                final_score = URLConfidenceScore(
                    url=url,
                    confidence_score=hybrid_confidence,
                    reasoning=f"LLM: {llm_confidence:.2f}, Coverage: {metadata_coverage:.2f}. {llm_score.reasoning}",
                    article_quality=llm_score.article_quality,
                    content_completeness=llm_score.content_completeness,
                    source_reliability=llm_score.source_reliability,
                )
                
                scores.append(final_score)
                
            except Exception as e:
                logger.error(f"Error scoring URL {url}: {e}")
                # Add low-confidence score for error case
                scores.append(URLConfidenceScore(
                    url=url,
                    confidence_score=0.0,
                    reasoning=f"Error during LLM scoring: {str(e)}",
                    article_quality="poor",
                    content_completeness="minimal",
                    source_reliability="unknown"
                ))
        
        return scores
    
    def enrich_incident_comprehensive(
        self,
        incident: BaseIncident,
        article_contents: Dict[str, ArticleContent],
    ) -> Optional[CTIEnrichmentResult]:
        """
        Perform comprehensive CTI enrichment in a SINGLE LLM call.
        
        This single call handles:
        1. Education relevance check
        2. URL scoring and selection
        3. Complete CTI extraction (timeline, MITRE, attack dynamics, summary)
        
        Args:
            incident: BaseIncident to enrich
            article_contents: Dictionary mapping URL to ArticleContent
            
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
        # Also collect publication dates for date resolution instructions
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
            # Include full article content (LLM needs to evaluate all articles)
            # If article is too long, include more content but limit to reasonable size
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
        
        # Separate system prompt for better instruction clarity
        system_prompt = """You are an expert cyber threat intelligence analyst specializing in education sector incidents.

Your task is to analyze cyber incident information and extract comprehensive CTI data. You must:
1. Determine if the incident is education-related (universities, colleges, K-12 schools, research institutions, educational organizations)
2. Evaluate ALL provided article URLs and score them based on quality, completeness, and reliability
3. Select the best primary URL from your evaluation
4. Extract detailed incident information including timeline, MITRE ATT&CK techniques, and attack dynamics
5. Use article publication dates to resolve relative dates (e.g., "last Friday" -> actual date)

IMPORTANT:
- Score ALL URLs provided - do not skip any
- Synthesize information from ALL articles - each may contain unique details
- Select primary_url based on comprehensive evaluation (not just highest score)
- All dates in timeline must be in ISO 8601 format (YYYY-MM-DD)
- Use article publish_date metadata to resolve relative dates
- Be thorough and accurate in your extraction"""

        user_prompt = f"""Analyze the following incident and articles. Perform THREE tasks in ONE response:

1. EDUCATION RELEVANCE CHECK:
   - Determine if this incident is related to the education sector (universities, colleges, K-12 schools, research institutions, educational organizations)
   - Provide confidence score (0.0-1.0) and reasoning
   - Identify the specific institution name if mentioned

2. URL SCORING (CRITICAL - You must evaluate ALL URLs):
   - You have been provided with {len(article_contents)} article(s) for this incident
   - Evaluate EACH article URL for quality and completeness for CTI extraction
   - Score EACH URL (0.0-1.0) based on:
     * Article quality: How well-written and informative is the article? (excellent/good/fair/poor)
     * Content completeness: How much CTI-relevant information does it contain? (complete/partial/minimal)
     * Source reliability: How trustworthy is the source? (highly_reliable/reliable/moderate/unknown)
     * Information richness: Does it provide detailed incident information, timelines, technical details?
     * Coverage: Does it cover multiple aspects (timeline, techniques, impact, attribution)?
   - Provide detailed reasoning for each score explaining why each URL scores that way
   - IMPORTANT: You MUST score ALL URLs provided - do not skip any

3. COMPREHENSIVE CTI EXTRACTION:
   - Extract detailed chronological timeline of events
   - Identify MITRE ATT&CK techniques and tactics used
   - Analyze attack dynamics (attack vector, kill chain, ransomware family if applicable, data exfiltration, impact scope, ransom details if applicable)
   - Provide comprehensive summary with all key details

{date_resolution_instructions}
{context}

CRITICAL INSTRUCTIONS: 
- If multiple articles exist, you MUST:
  * Score ALL URLs in the url_scores list (do not skip any)
  * Synthesize information from ALL articles (not just one)
  * Use the best information from each article to build a comprehensive CTI picture
  * The primary_url field should be the URL of the best/most complete article (based on your evaluation)
- For URL scoring:
  * Evaluate each article independently and fairly
  * Consider which article provides the most comprehensive CTI information
  * An article with more detailed technical information should score higher
  * An article with better timeline coverage should score higher
  * An article from a more reliable source should score higher
- Fill all fields in the schema completely and accurately
- If information is not available, use null/empty lists as appropriate, but try to extract as much as possible
- **CRITICAL**: For timeline events, you MUST convert all relative dates (like "last Friday", "yesterday", "last week") to absolute dates (YYYY-MM-DD) using the article publication dates provided above as reference points
- Timeline dates must be in YYYY-MM-DD format with appropriate date_precision values

Return the complete CTIEnrichmentResult with all fields populated."""
        
        try:
            result = self.llm_client.extract_structured(
                prompt=user_prompt,
                schema_model=CTIEnrichmentResult,
                system_prompt=system_prompt,
            )
            
            # Ensure primary_url is set if we have URL scores
            if not result.primary_url and result.url_scores:
                best_score = max(result.url_scores, key=lambda s: s.confidence_score)
                result.primary_url = best_score.url
            
            # Ensure primary_url is set even if no scores
            if not result.primary_url and article_contents:
                result.primary_url = list(article_contents.keys())[0]
            
            return result
            
        except Exception as e:
            logger.error(f"Error during comprehensive CTI extraction: {e}")
            # Return minimal result with required fields
            first_url = list(article_contents.keys())[0] if article_contents else None
            return CTIEnrichmentResult(
                primary_url=first_url,
                extraction_confidence=0.5,
                enriched_summary=f"Error during extraction: {str(e)}",
                timeline=[],
                mitre_attack_techniques=[],
                attack_dynamics=None,
                url_scores=[],
                education_relevance=EducationRelevanceCheck(
                    is_education_related=True,
                    confidence=0.5,
                    reasoning=f"Default due to extraction error: {str(e)}",
                    institution_identified=None,
                ),
            )
    
    def enrich_incident(
        self,
        incident: BaseIncident,
        primary_article: ArticleContent,
        all_article_contents: Dict[str, ArticleContent],
    ) -> CTIEnrichmentResult:
        """
        Legacy method - kept for backwards compatibility.
        Use enrich_incident_comprehensive instead.
        """
        return self.enrich_incident_comprehensive(incident, all_article_contents) or CTIEnrichmentResult(
            primary_url=primary_article.url,
            extraction_confidence=0.5,
            enriched_summary="Legacy method called - should use enrich_incident_comprehensive",
            timeline=[],
            mitre_attack_techniques=[],
            attack_dynamics=None,
            url_scores=[],
            education_relevance=EducationRelevanceCheck(
                is_education_related=True,
                confidence=0.5,
                reasoning="Legacy method",
                institution_identified=None,
            ),
        )

