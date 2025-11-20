"""
Metadata extractor for analyzing schema field coverage from articles.

This module extracts metadata from articles to assess how well they populate
the CTI enrichment schema, enabling hybrid URL scoring.
"""

import re
import logging
from typing import Dict, List, Set, Optional
from dataclasses import dataclass

from src.edu_cti.pipeline.phase2.article_fetcher import ArticleContent
from src.edu_cti.pipeline.phase2.schemas_extended import (
    ExtendedCTIEnrichmentResult,
    DataImpactMetrics,
    SystemImpactMetrics,
    UserImpactMetrics,
    OperationalImpactMetrics,
    FinancialImpactMetrics,
    RegulatoryImpactMetrics,
    RecoveryMetrics,
    TransparencyMetrics,
    ResearchImpactMetrics,
)

logger = logging.getLogger(__name__)


@dataclass
class MetadataCoverage:
    """Metadata coverage analysis for an article."""
    
    url: str
    total_fields: int = 0
    populated_fields: int = 0
    coverage_score: float = 0.0  # 0.0 to 1.0
    field_details: Dict[str, bool] = None  # Field -> is_populated
    
    def __post_init__(self):
        if self.field_details is None:
            self.field_details = {}


class MetadataExtractor:
    """
    Extracts metadata from articles to assess schema coverage.
    
    Uses pattern matching and keyword detection to identify
    what fields can be populated from the article content.
    """
    
    def __init__(self):
        """Initialize metadata extractor with pattern matchers."""
        self.patterns = self._build_patterns()
    
    def _build_patterns(self) -> Dict[str, List[re.Pattern]]:
        """Build regex patterns for field detection."""
        return {
            # Data types
            "student_data": [
                re.compile(r'\bstudent[s]?\b.*\b(data|information|record)', re.I),
                re.compile(r'\bstudent.*(email|ssn|personal information)', re.I),
            ],
            "faculty_data": [
                re.compile(r'\b(faculty|staff|employee[s]?)\b.*\b(data|information|record)', re.I),
            ],
            "alumni_data": [
                re.compile(r'\balumni.*\b(data|information|record)', re.I),
            ],
            "financial_data": [
                re.compile(r'\b(financial|payment|donation|tuition|payroll)\b.*\b(data|information)', re.I),
            ],
            "research_data": [
                re.compile(r'\bresearch.*\b(data|information)', re.I),
            ],
            "medical_records": [
                re.compile(r'\b(medical|health|patient|hipaa)\b.*\b(record|data|information)', re.I),
            ],
            
            # User counts
            "records_affected": [
                re.compile(r'\b(\d+[,.]?\d*)\s*(?:individuals?|people|records?|accounts?|users?)\b', re.I),
                re.compile(r'\b(?:affecting|impacting|compromising)\s+(\d+[,.]?\d*)', re.I),
            ],
            
            # Financial
            "ransom_amount": [
                re.compile(r'\b(?:ransom|demand)\s+(?:of\s+)?[\$€£]?(\d+[,.]?\d*)\s*(?:million|thousand|billion|k|m|b)?', re.I),
            ],
            "ransom_paid": [
                re.compile(r'\b(?:paid|paying)\s+(?:the\s+)?(?:ransom|demand)', re.I),
            ],
            
            # Systems
            "email_system_affected": [
                re.compile(r'\bemail\s+(?:system|server|service)\b', re.I),
            ],
            "student_portal_affected": [
                re.compile(r'\b(student\s+portal|learning\s+management|student\s+information\s+system|sis|lms)\b', re.I),
            ],
            "research_systems_affected": [
                re.compile(r'\bresearch\s+(?:computing|server|system)', re.I),
            ],
            
            # Operational
            "teaching_disrupted": [
                re.compile(r'\b(?:teaching|classes?|courses?|instruction)\s+(?:disrupted|affected|cancelled)', re.I),
            ],
            "classes_cancelled": [
                re.compile(r'\bclasses?\s+(?:cancelled|canceled|postponed)', re.I),
            ],
            "exams_postponed": [
                re.compile(r'\bexams?\s+(?:postponed|delayed|cancelled)', re.I),
            ],
            
            # Recovery
            "recovery_timeframe": [
                re.compile(r'\b(?:recover|restore|restoration)\s+(?:took|takes|took\s+about)\s+(\d+)\s+(?:days?|weeks?|months?)', re.I),
            ],
            "from_backup": [
                re.compile(r'\brecover(?:ed|ing|y)?\s+(?:from|using)\s+backup', re.I),
            ],
            
            # Regulatory
            "breach_notification": [
                re.compile(r'\bnotified?\s+(?:individuals?|affected\s+parties?|regulators?)', re.I),
            ],
            "gdpr_breach": [
                re.compile(r'\b(gdpr|eu\s+data|general\s+data\s+protection)', re.I),
            ],
            "hipaa_breach": [
                re.compile(r'\bhipaa\b', re.I),
            ],
            "ferc_breach": [
                re.compile(r'\b(ferpa|family\s+educational\s+rights)', re.I),
            ],
            
            # Dates
            "incident_date": [
                re.compile(r'\b(?:incident|breach|attack|discovered|occurred)\s+(?:on\s+)?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', re.I),
            ],
            "disclosure_date": [
                re.compile(r'\b(?:disclosed?|announced?|revealed?)\s+(?:on\s+)?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', re.I),
            ],
            
            # Ransomware
            "ransomware_family": [
                re.compile(r'\b(lockbit|blackcat|blackmatter|conti|revil|ryuk|phobos|darkside|clop|maze)\b', re.I),
            ],
        }
    
    def extract_metadata_coverage(
        self,
        article: ArticleContent,
        schema_model: type = ExtendedCTIEnrichmentResult,
    ) -> MetadataCoverage:
        """
        Extract metadata coverage for an article.
        
        Args:
            article: ArticleContent to analyze
            schema_model: Schema model class to check coverage against
            
        Returns:
            MetadataCoverage with coverage analysis
        """
        if not article.fetch_successful or not article.content:
            return MetadataCoverage(
                url=article.url,
                total_fields=0,
                populated_fields=0,
                coverage_score=0.0,
            )
        
        text = (article.title or "") + " " + article.content
        text_lower = text.lower()
        
        # Get all fields from the schema
        schema_fields = self._get_schema_fields(schema_model)
        
        # Check which fields can be populated
        field_details = {}
        populated_count = 0
        
        for field_name in schema_fields:
            is_populated = self._check_field_populatable(field_name, text, text_lower)
            field_details[field_name] = is_populated
            if is_populated:
                populated_count += 1
        
        total_fields = len(schema_fields)
        coverage_score = populated_count / total_fields if total_fields > 0 else 0.0
        
        return MetadataCoverage(
            url=article.url,
            total_fields=total_fields,
            populated_fields=populated_count,
            coverage_score=coverage_score,
            field_details=field_details,
        )
    
    def _get_schema_fields(self, schema_model: type) -> List[str]:
        """Get all field names from a schema model."""
        if not hasattr(schema_model, 'model_fields'):
            return []
        
        fields = []
        
        # Get top-level fields
        for field_name in schema_model.model_fields.keys():
            fields.append(field_name)
            
            # Get nested model fields
            field_info = schema_model.model_fields[field_name]
            if hasattr(field_info, 'annotation'):
                ann = field_info.annotation
                # Check if it's an optional nested model
                if hasattr(ann, '__origin__') and hasattr(ann, '__args__'):
                    # Handle Optional[...] types
                    args = ann.__args__
                    for arg in args:
                        if hasattr(arg, 'model_fields'):
                            # Recursively get nested fields
                            for nested_field in arg.model_fields.keys():
                                fields.append(f"{field_name}.{nested_field}")
        
        return fields
    
    def _check_field_populatable(self, field_name: str, text: str, text_lower: str) -> bool:
        """
        Check if a field can be populated from article text.
        
        Uses pattern matching and keyword detection.
        """
        # Direct pattern match
        field_key = field_name.split('.')[-1]  # Handle nested fields like "data_impact.student_data"
        
        if field_key in self.patterns:
            for pattern in self.patterns[field_key]:
                if pattern.search(text):
                    return True
        
        # Keyword-based detection
        keywords_map = {
            "student_data": ["student", "pupil"],
            "faculty_data": ["faculty", "staff", "employee"],
            "alumni_data": ["alumni", "alumna"],
            "financial_data": ["financial", "payment", "donation"],
            "research_data": ["research", "study", "experiment"],
            "medical_records": ["medical", "health", "patient", "hipaa"],
            "ransomware_family": ["ransomware", "malware"],
            "teaching_disrupted": ["teaching", "class", "course"],
            "classes_cancelled": ["cancelled", "canceled", "postponed"],
            "recovery_timeframe": ["recover", "restore", "restoration"],
        }
        
        if field_key in keywords_map:
            keywords = keywords_map[field_key]
            if any(keyword in text_lower for keyword in keywords):
                return True
        
        # Check for numeric patterns (counts, amounts, dates)
        if "count" in field_key or "amount" in field_key or "affected" in field_key:
            # Look for numbers
            if re.search(r'\d+[,.]?\d*', text):
                return True
        
        # Check for date fields
        if "date" in field_key or "timeframe" in field_key:
            # Look for date patterns
            if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', text):
                return True
        
        # Check for boolean fields (presence implies True)
        if field_key.endswith("_affected") or field_key.endswith("_disrupted") or field_key.endswith("_compromised"):
            # If field name is mentioned, likely True
            base_term = field_key.replace("_affected", "").replace("_disrupted", "").replace("_compromised", "")
            if base_term in text_lower:
                return True
        
        return False
    
    def compare_url_metadata_coverage(
        self,
        articles: Dict[str, ArticleContent],
    ) -> Dict[str, MetadataCoverage]:
        """
        Compare metadata coverage across multiple articles.
        
        Args:
            articles: Dictionary mapping URL to ArticleContent
            
        Returns:
            Dictionary mapping URL to MetadataCoverage
        """
        coverage_results = {}
        
        for url, article in articles.items():
            coverage = self.extract_metadata_coverage(article)
            coverage_results[url] = coverage
        
        return coverage_results

