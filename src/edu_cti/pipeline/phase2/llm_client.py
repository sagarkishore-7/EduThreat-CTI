"""
LLM client for Ollama Cloud API.

Handles communication with Ollama Cloud for structured CTI enrichment.
"""

import os
import json
import logging
from typing import Optional, Dict, Any, Iterator
from pydantic import BaseModel

try:
    from ollama import Client
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False
    Client = None

logger = logging.getLogger(__name__)

# Model selection: deepseek-v3.1:671b-cloud is chosen for complex structured extraction
# It's one of the largest and most capable models, ideal for CTI enrichment tasks
DEFAULT_MODEL = "deepseek-v3.1:671b-cloud"


class OllamaLLMClient:
    """
    Client for interacting with Ollama Cloud API.
    
    Handles:
    - Structured output generation using Pydantic schemas
    - Streaming responses
    - Error handling and retries
    - Rate limiting considerations
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        host: str = "https://ollama.com",
        model: str = DEFAULT_MODEL,
    ):
        """
        Initialize Ollama LLM client.
        
        Args:
            api_key: Ollama API key (defaults to OLLAMA_API_KEY env var)
            host: Ollama API host (defaults to https://ollama.com)
            model: Model name to use
        """
        if not OLLAMA_AVAILABLE:
            raise ImportError(
                "ollama package not installed. Install with: pip install ollama"
            )
        
        self.api_key = api_key or os.environ.get("OLLAMA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OLLAMA_API_KEY not provided. Set environment variable or pass api_key parameter."
            )
        
        self.host = host
        self.model = model
        
        self.client = Client(
            host=host,
            headers={'Authorization': f'Bearer {self.api_key}'}
        )
    
    def chat(
        self,
        messages: list[Dict[str, str]],
        format: Optional[Dict[str, Any]] = None,
        stream: bool = False,
        temperature: float = 0.3,  # Lower temperature for more structured output
    ) -> Iterator[Dict[str, Any]] | Dict[str, Any]:
        """
        Send chat request to Ollama.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            format: Optional Pydantic model JSON schema for structured output
            stream: Whether to stream the response
            temperature: Sampling temperature (lower for more deterministic)
            
        Returns:
            Response dict or iterator of response parts if streaming
        """
        try:
            response = self.client.chat(
                model=self.model,
                messages=messages,
                format=format,
                stream=stream,
                options={
                    'temperature': temperature,
                }
            )
            return response
        except Exception as e:
            logger.error(f"Error calling Ollama API: {e}")
            raise
    
    def _normalize_llm_response(self, response: Dict[str, Any], schema_model: type[BaseModel]) -> Dict[str, Any]:
        """
        Normalize LLM response to match expected schema structure.
        
        Handles common variations in LLM responses:
        - Field name differences (confidence_score vs confidence)
        - Nested structures (cti_extraction wrapper)
        - Object vs string for reasoning fields
        - Missing fields with defaults
        - Field name mappings (mitre_attack -> mitre_attack_techniques)
        """
        if not isinstance(response, dict):
            return response
        
        normalized = response.copy()
        
        # Map incident_review -> education_relevance
        if 'incident_review' in normalized and 'education_relevance' not in normalized:
            # Check if incident_review contains education_relevance fields
            review = normalized['incident_review']
            if isinstance(review, dict) and any(k in review for k in ['is_education_related', 'confidence', 'confidence_score', 'reasoning']):
                normalized['education_relevance'] = review
                del normalized['incident_review']
                logger.debug("Mapped incident_review to education_relevance")
        
        # Handle nested wrappers (cti_extraction, incident_analysis, etc.)
        wrapper_keys = ['cti_extraction', 'incident_analysis', 'result', 'data', 'response']
        for wrapper_key in wrapper_keys:
            if wrapper_key in normalized and isinstance(normalized[wrapper_key], dict):
                wrapper_data = normalized[wrapper_key]
                # Merge wrapper fields into top level
                for key in ['education_relevance', 'timeline', 'mitre_attack_techniques', 'attack_dynamics', 'enriched_summary', 'extraction_confidence']:
                    if key in wrapper_data:
                        normalized[key] = wrapper_data[key]
                # Also merge all other keys that aren't already at top level
                for key, value in wrapper_data.items():
                    if key not in normalized:
                        normalized[key] = value
                # Remove the wrapper
                del normalized[wrapper_key]
                logger.debug(f"Unwrapped {wrapper_key} structure")
                break
        
        # Map mitre_attack -> mitre_attack_techniques
        if 'mitre_attack' in normalized and 'mitre_attack_techniques' not in normalized:
            normalized['mitre_attack_techniques'] = normalized.pop('mitre_attack')
        
        # Ensure mitre_attack_techniques is a list
        if 'mitre_attack_techniques' in normalized:
            if isinstance(normalized['mitre_attack_techniques'], dict):
                # If it's a dict, try to extract techniques from it
                mitre_dict = normalized['mitre_attack_techniques']
                if 'techniques' in mitre_dict and isinstance(mitre_dict['techniques'], list):
                    normalized['mitre_attack_techniques'] = mitre_dict['techniques']
                elif 'tactics' in mitre_dict:
                    # If it has tactics, convert to list format
                    normalized['mitre_attack_techniques'] = []
                    logger.debug("Converted mitre_attack_techniques dict to empty list (structure not compatible)")
                else:
                    # Unknown structure, set to empty list
                    normalized['mitre_attack_techniques'] = []
                    logger.debug("Converted mitre_attack_techniques dict to empty list (unknown structure)")
            elif not isinstance(normalized['mitre_attack_techniques'], list):
                # Not a list and not a dict, set to empty list
                normalized['mitre_attack_techniques'] = []
        
        # Normalize education_relevance
        if 'education_relevance' in normalized and isinstance(normalized['education_relevance'], dict):
            er = normalized['education_relevance']
            # Map confidence_score -> confidence
            if 'confidence_score' in er and 'confidence' not in er:
                er['confidence'] = er.pop('confidence_score')
            # Map institution_name -> institution_identified
            if 'institution_name' in er and 'institution_identified' not in er:
                er['institution_identified'] = er.pop('institution_name')
        
        # Normalize url_scores
        if 'url_scores' in normalized and isinstance(normalized['url_scores'], list):
            for score in normalized['url_scores']:
                if isinstance(score, dict):
                    # Handle evaluation object (some LLMs wrap fields in evaluation)
                    if 'evaluation' in score and isinstance(score['evaluation'], dict):
                        eval_data = score['evaluation']
                        # Merge evaluation fields into score
                        for key in ['article_quality', 'content_completeness', 'source_reliability', 'information_richness', 'coverage']:
                            if key in eval_data and key not in score:
                                score[key] = eval_data[key]
                        del score['evaluation']
                    
                    # Map score -> confidence_score
                    if 'score' in score and 'confidence_score' not in score:
                        score['confidence_score'] = score.pop('score')
                    # Map overall_score -> confidence_score
                    if 'overall_score' in score and 'confidence_score' not in score:
                        score['confidence_score'] = score.pop('overall_score')
                    # Handle reasoning as object -> convert to string
                    if 'reasoning' in score and isinstance(score['reasoning'], dict):
                        # Try to extract meaningful text from reasoning object
                        reasoning_parts = []
                        for key in ['detailed_assessment', 'summary', 'explanation', 'description']:
                            if key in score['reasoning']:
                                reasoning_parts.append(str(score['reasoning'][key]))
                        if reasoning_parts:
                            score['reasoning'] = ' '.join(reasoning_parts)
                        else:
                            # Fallback: stringify the dict
                            score['reasoning'] = str(score['reasoning'])
                        logger.debug("Converted reasoning object to string")
                    # Ensure reasoning exists
                    if 'reasoning' not in score:
                        score['reasoning'] = "Reasoning not provided by LLM"
                    # Ensure required fields exist
                    if 'article_quality' not in score:
                        score['article_quality'] = 'fair'
                    if 'content_completeness' not in score:
                        score['content_completeness'] = 'partial'
                    if 'source_reliability' not in score:
                        score['source_reliability'] = 'moderate'
        
        # Normalize timeline events
        if 'timeline' in normalized and isinstance(normalized['timeline'], list):
            for event in normalized['timeline']:
                if isinstance(event, dict):
                    # Map description -> event_description
                    if 'description' in event and 'event_description' not in event:
                        event['event_description'] = event.pop('description')
                    # Map event -> event_type
                    if 'event' in event and 'event_type' not in event:
                        event['event_type'] = event.pop('event')
                    # Ensure event_type exists
                    if 'event_type' not in event:
                        event['event_type'] = 'other'
                    # Ensure event_description exists
                    if 'event_description' not in event:
                        # Try to use any text field
                        for key in ['description', 'details', 'summary', 'note']:
                            if key in event:
                                event['event_description'] = str(event[key])
                                break
                        if 'event_description' not in event:
                            event['event_description'] = "Event details not provided"
        
        # Normalize MITRE attack techniques
        if 'mitre_attack_techniques' in normalized and isinstance(normalized['mitre_attack_techniques'], list):
            for tech in normalized['mitre_attack_techniques']:
                if isinstance(tech, dict):
                    # Ensure confidence exists (required field)
                    if 'confidence' not in tech:
                        # Try to infer from other fields
                        if 'confidence_level' in tech:
                            tech['confidence'] = tech.pop('confidence_level')
                        elif 'certainty' in tech:
                            tech['confidence'] = tech.pop('certainty')
                        else:
                            tech['confidence'] = 'possible'  # Default
                    # Ensure description exists (required field)
                    if 'description' not in tech:
                        # Try other field names
                        for key in ['how_used', 'usage', 'details', 'explanation']:
                            if key in tech:
                                tech['description'] = str(tech[key])
                                break
                        if 'description' not in tech:
                            tech['description'] = "Description not provided"
        
        # Normalize attack_dynamics
        if 'attack_dynamics' in normalized and isinstance(normalized['attack_dynamics'], dict):
            ad = normalized['attack_dynamics']
            # Handle data_exfiltration - convert to boolean
            if 'data_exfiltration' in ad:
                exfil_val = ad['data_exfiltration']
                if isinstance(exfil_val, dict):
                    # If it's an object with 'confirmed' or similar, extract boolean
                    ad['data_exfiltration'] = exfil_val.get('confirmed', False) or exfil_val.get('occurred', False)
                elif isinstance(exfil_val, str):
                    # If it's a string like "Unknown", "Yes", "No", convert to boolean
                    exfil_str = exfil_val.lower().strip()
                    if exfil_str in ['yes', 'true', 'confirmed', 'occurred', '1']:
                        ad['data_exfiltration'] = True
                    elif exfil_str in ['no', 'false', 'not', 'none', '0']:
                        ad['data_exfiltration'] = False
                    else:
                        # Unknown/ambiguous, default to False
                        ad['data_exfiltration'] = False
                        logger.debug(f"Converted ambiguous data_exfiltration value '{exfil_val}' to False")
                else:
                    ad['data_exfiltration'] = bool(exfil_val)
            else:
                ad['data_exfiltration'] = False
            
            # Handle impact_scope - ensure it's a dict
            if 'impact_scope' in ad and not isinstance(ad['impact_scope'], dict):
                impact_val = ad['impact_scope']
                if isinstance(impact_val, str):
                    # Convert string to dict with description
                    ad['impact_scope'] = {'description': impact_val}
                    logger.debug("Converted impact_scope string to dict")
                elif isinstance(impact_val, list):
                    # Convert list to dict
                    ad['impact_scope'] = {'affected_items': impact_val}
                    logger.debug("Converted impact_scope list to dict")
                else:
                    # Unknown type, use empty dict
                    ad['impact_scope'] = {}
            elif 'impact_scope' not in ad:
                ad['impact_scope'] = {}
            # Ensure required fields
            if 'attack_chain' not in ad:
                ad['attack_chain'] = []
            if 'ransom_demanded' not in ad:
                # Infer from ransom_amount or ransom_paid
                ad['ransom_demanded'] = bool(ad.get('ransom_amount')) or ad.get('ransom_paid') is not None
            if 'business_impact' not in ad:
                # Try to infer from other fields
                if 'impact' in ad:
                    ad['business_impact'] = ad.pop('impact')
                elif 'severity' in ad:
                    ad['business_impact'] = ad.pop('severity')
                else:
                    ad['business_impact'] = 'unknown'
            # Ensure operational_impact exists
            if 'operational_impact' not in ad:
                ad['operational_impact'] = []
        
        # Ensure required fields exist with defaults
        if 'enriched_summary' not in normalized:
            # Try 'summary' as fallback
            if 'summary' in normalized:
                normalized['enriched_summary'] = normalized.pop('summary')
            else:
                normalized['enriched_summary'] = "Summary not provided by LLM"
        if 'extraction_confidence' not in normalized:
            normalized['extraction_confidence'] = 0.5
        if 'primary_url' not in normalized and 'url_scores' in normalized and normalized['url_scores']:
            # Use first URL as primary if not specified
            first_url = normalized['url_scores'][0].get('url') if normalized['url_scores'] else None
            if first_url:
                normalized['primary_url'] = first_url
        
        return normalized
    
    def extract_structured(
        self,
        prompt: str,
        schema_model: type[BaseModel],
        system_prompt: Optional[str] = None,
        max_retries: int = 2,
    ) -> BaseModel:
        """
        Extract structured data using a Pydantic schema.
        
        Args:
            prompt: User prompt with content to extract from
            schema_model: Pydantic model class defining the expected output schema
            system_prompt: Optional system prompt for instructions
            max_retries: Maximum number of retries for empty/invalid responses
            
        Returns:
            Validated Pydantic model instance
        """
        messages = []
        
        if system_prompt:
            messages.append({
                'role': 'system',
                'content': system_prompt
            })
        
        messages.append({
            'role': 'user',
            'content': prompt
        })
        
        # Get JSON schema from Pydantic model
        format_schema = schema_model.model_json_schema()
        
        # Log message structure for debugging
        logger.debug(f"LLM call structure:")
        logger.debug(f"  System prompt present: {system_prompt is not None}")
        logger.debug(f"  User prompt length: {len(prompt)} characters")
        logger.debug(f"  Schema model: {schema_model.__name__}")
        logger.debug(f"  Messages count: {len(messages)}")
        logger.debug(f"  Format schema keys: {list(format_schema.keys())[:5] if isinstance(format_schema, dict) else 'N/A'}...")
        
        # Retry logic for empty/invalid responses
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                # Make API call
                logger.debug(f"Making LLM API call (attempt {attempt + 1}/{max_retries + 1})")
                response = self.chat(
                    messages=messages,
                    format=format_schema,
                    stream=False,
                )
                logger.debug(f"LLM API call successful, response type: {type(response)}")
                break
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying...")
                    import time
                    time.sleep(1.0 * (attempt + 1))  # Exponential backoff
                else:
                    raise
        
        # If we got here without breaking, retries exhausted
        if last_error:
            raise last_error
        
        # Extract content from response - handle various response structures
        content = None
        
        if isinstance(response, dict):
            # Try multiple possible response structures
            if 'message' in response:
                message = response['message']
                if isinstance(message, dict):
                    content = message.get('content', '')
                    if content:
                        logger.debug(f"Found content in response['message']['content']")
                elif isinstance(message, str):
                    content = message
                    logger.debug(f"Found content in response['message'] (string)")
                else:
                    # Message might be an object (like ollama._types.Message)
                    # Try attribute access
                    if hasattr(message, 'content'):
                        content = message.content
                        if content:
                            logger.debug(f"Found content in response.message.content (attribute access)")
        
        # If still no content and response is dict-like, try dict access methods
        if not content and isinstance(response, dict):
            # Try direct content field
            if 'content' in response:
                content = response['content']
                if content:
                    logger.debug(f"Found content in response['content']")
            
            # Try text field
            if not content and 'text' in response:
                content = response['text']
                if content:
                    logger.debug(f"Found content in response['text']")
            
            # Try response field
            if not content and 'response' in response:
                content = response['response']
                if content:
                    logger.debug(f"Found content in response['response']")
        
        # If response is object-like (not dict), try attribute access
        if not content and not isinstance(response, dict):
            try:
                if hasattr(response, 'message') and hasattr(response.message, 'content'):
                    content = response.message.content
                    if content:
                        logger.debug(f"Found content in response.message.content (object attribute access)")
                elif hasattr(response, 'content'):
                    content = response.content
                    if content:
                        logger.debug(f"Found content in response.content (object attribute access)")
            except Exception as e:
                logger.debug(f"Attribute access failed: {e}")
        else:
            # Handle streaming response (collect all parts)
            content = ''
            try:
                for part in response:
                    if isinstance(part, dict):
                        if 'message' in part:
                            msg = part['message']
                            if isinstance(msg, dict):
                                content += msg.get('content', '')
                            elif isinstance(msg, str):
                                content += msg
                        elif 'content' in part:
                            content += part.get('content', '')
                        elif 'text' in part:
                            content += part.get('text', '')
                    elif isinstance(part, str):
                        content += part
                
                if content:
                    logger.debug(f"Collected content from streaming response")
            except (TypeError, AttributeError) as e:
                logger.warning(f"Could not iterate response as stream: {e}")
        
        # Validate content
        if not content or not content.strip():
            logger.error(f"Empty response from LLM. Response type: {type(response)}")
            if isinstance(response, dict):
                logger.error(f"Response keys: {list(response.keys())}")
                try:
                    logger.error(f"Response structure: {json.dumps(response, indent=2, default=str)[:2000]}")
                except Exception as e:
                    logger.error(f"Response structure (str): {str(response)[:2000]}")
            else:
                logger.error(f"Response value (first 1000 chars): {str(response)[:1000]}")
            raise ValueError("Empty response from LLM - no content returned")
        
        # Parse and validate
        try:
            # Log raw content for debugging
            logger.debug(f"Raw LLM response content (first 500 chars): {content[:500]}")
            
            # Strip markdown code blocks if present (some LLMs wrap JSON in ```json ... ```)
            content_clean = content.strip()
            if content_clean.startswith('```'):
                # Remove opening ```json or ```
                lines = content_clean.split('\n')
                if lines[0].startswith('```'):
                    lines = lines[1:]
                # Remove closing ```
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                content_clean = '\n'.join(lines).strip()
                logger.debug("Stripped markdown code block markers from response")
            
            parsed = json.loads(content_clean)
            logger.debug(f"✓ JSON parsed successfully")
            
            # Check if response is wrapped in a nested structure
            # Some LLMs return {"incident_analysis": {...}} or similar
            if isinstance(parsed, dict) and len(parsed) == 1:
                # Check if there's a wrapper key (like "incident_analysis", "result", "data", etc.)
                wrapper_keys = [k for k in parsed.keys() if k not in ['education_relevance', 'primary_url', 'url_scores', 'timeline', 'mitre_attack_techniques', 'attack_dynamics', 'enriched_summary', 'extraction_confidence']]
                if wrapper_keys:
                    # Unwrap one level
                    parsed = parsed[wrapper_keys[0]]
                    logger.debug(f"Unwrapped response from wrapper key: {wrapper_keys[0]}")
            
            # Normalize LLM response to match our schema
            parsed = self._normalize_llm_response(parsed, schema_model)
            
            validated = schema_model.model_validate(parsed)
            logger.debug(f"✓ Response validated against {schema_model.__name__} schema")
            
            return validated
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Response content (first 1000 chars): {content[:1000]}")
            logger.error(f"Response content (last 500 chars): {content[-500:] if len(content) > 500 else content}")
            logger.error(f"Cleaned content (first 1000 chars): {content_clean[:1000] if 'content_clean' in locals() else 'N/A'}")
            raise ValueError(f"Invalid JSON response from LLM: {e}")
        except Exception as e:
            logger.error(f"Failed to validate response against schema: {e}")
            logger.error(f"Response content (first 1000 chars): {content[:1000]}")
            if 'parsed' in locals() and isinstance(parsed, dict):
                logger.error(f"Parsed JSON keys: {list(parsed.keys())}")
                # Try to show what keys are present vs what's expected
                expected_fields = ['education_relevance', 'primary_url', 'url_scores', 'timeline', 'mitre_attack_techniques']
                logger.error(f"Expected top-level fields: {expected_fields}")
                logger.error(f"Actual top-level fields: {list(parsed.keys())}")
            raise

