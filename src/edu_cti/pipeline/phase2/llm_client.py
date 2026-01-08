"""
LLM client for Ollama Cloud API.

Handles communication with Ollama Cloud for structured CTI enrichment.
"""

import os
import json
import logging
import time
from typing import Optional, Dict, Any, Iterator
from pydantic import BaseModel


class RateLimitError(Exception):
    """Raised when rate limit is encountered and cannot be recovered."""
    pass

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
    
    def _is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error is a rate limit error."""
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()
        
        # Check for common rate limit indicators
        rate_limit_indicators = [
            'rate limit',
            'rate_limit',
            'too many requests',
            '429',
            'quota',
            'throttle',
            'limit exceeded',
            'request limit',
        ]
        
        return any(indicator in error_str or indicator in error_type for indicator in rate_limit_indicators)
    
    def chat(
        self,
        messages: list[Dict[str, str]],
        format: Optional[Dict[str, Any]] = None,
        stream: bool = False,
        temperature: float = 0.1,  # Low temperature for deterministic structured output
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
            
        Raises:
            RateLimitError: If rate limit persists after exponential backoff
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
            if self._is_rate_limit_error(e):
                logger.warning(f"Rate limit detected in Ollama API: {e}")
                raise RateLimitError(f"Ollama API rate limit: {e}") from e
            logger.error(f"Error calling Ollama API: {e}")
            raise
    
    def extract_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_retries: int = 2,
    ) -> str:
        """
        Extract JSON from LLM using simple system/user prompt approach.
        
        Args:
            system_prompt: System prompt
            user_prompt: User prompt with article content
            max_retries: Maximum number of retries
            
        Returns:
            Raw JSON string response
        """
        messages = [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt}
        ]
        
        last_error = None
        for attempt in range(max_retries + 1):
            try:
                logger.debug(f"Making LLM API call (attempt {attempt + 1}/{max_retries + 1})")
                response = self.chat(
                    messages=messages,
                    format=None,  # No format constraint - we want raw JSON
                    stream=False,
                    temperature=0.1,  # Low for deterministic structured output
                )
                
                # Extract content from response
                content = None
                if isinstance(response, dict):
                    if 'message' in response:
                        message = response['message']
                        if isinstance(message, dict):
                            content = message.get('content', '')
                        elif isinstance(message, str):
                            content = message
                        else:
                            # Try attribute access
                            content = getattr(message, 'content', None) or str(message)
                    elif 'content' in response:
                        content = response['content']
                    else:
                        content = str(response)
                elif hasattr(response, 'content'):
                    content = response.content
                elif isinstance(response, str):
                    content = response
                else:
                    content = str(response)
                
                if content:
                    # Try to extract JSON from markdown code blocks if present
                    import re
                    json_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', content, re.DOTALL)
                    if json_match:
                        content = json_match.group(1)
                    else:
                        # Try to find JSON object in the content
                        json_match = re.search(r'\{.*\}', content, re.DOTALL)
                        if json_match:
                            content = json_match.group(0)
                    
                    # Clean up: strip whitespace
                    content = content.strip()
                    # If JSON starts with { followed by newline/whitespace, normalize it
                    # JSON allows whitespace, but some parsers are strict
                    if content.startswith('{\n'):
                        # Replace {\n with { and remove leading whitespace from next line
                        content = '{' + content[2:].lstrip()
                    elif content.startswith('{\r\n'):
                        content = '{' + content[3:].lstrip()
                    elif content.startswith('{ ') and len(content) > 2:
                        # Keep the space if it's followed by a quote
                        pass  # This is valid JSON
                    
                    logger.debug(f"Extracted content length: {len(content)}")
                    return content
                else:
                    raise ValueError("Empty response from LLM")
                    
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying...")
                    import time
                    time.sleep(1.0 * (attempt + 1))
                else:
                    raise
        
        if last_error:
            raise last_error
        
        raise ValueError("Failed to get response from LLM")
    
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
            if isinstance(review, dict) and any(k in review for k in ['is_education_related', 'reasoning']):
                normalized['education_relevance'] = review
                del normalized['incident_review']
                logger.debug("Mapped incident_review to education_relevance")
        
        # Handle nested wrappers (cti_extraction, incident_analysis, etc.)
        wrapper_keys = ['cti_extraction', 'incident_analysis', 'result', 'data', 'response']
        for wrapper_key in wrapper_keys:
            if wrapper_key in normalized and isinstance(normalized[wrapper_key], dict):
                wrapper_data = normalized[wrapper_key]
                # Merge wrapper fields into top level
                for key in ['education_relevance', 'timeline', 'mitre_attack_techniques', 'attack_dynamics', 
                           'enriched_summary', 'initial_access_description', 'primary_url',
                           'data_impact', 'system_impact', 'user_impact', 'operational_impact_metrics',
                           'financial_impact', 'regulatory_impact', 'recovery_metrics', 
                           'transparency_metrics', 'research_impact']:
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
        
        # Normalize education_relevance (simplified - no confidence score)
        # Check if education_relevance fields are at top level instead of nested
        if 'education_relevance' not in normalized:
            # Check if education relevance fields are at top level
            if 'is_education_related' in normalized:
                # Create education_relevance object from top-level fields
                er = {}
                er['is_education_related'] = normalized.pop('is_education_related')
                
                # Look for reasoning
                if 'education_reasoning' in normalized:
                    er['reasoning'] = normalized.pop('education_reasoning')
                elif 'reasoning' in normalized:
                    er['reasoning'] = normalized.pop('reasoning')
                else:
                    er['reasoning'] = "Education relevance reasoning not provided by LLM"
                
                # Look for institution
                if 'institution_identified' in normalized:
                    er['institution_identified'] = normalized.pop('institution_identified')
                elif 'institution_name' in normalized:
                    er['institution_identified'] = normalized.pop('institution_name')
                else:
                    er['institution_identified'] = None
                
                normalized['education_relevance'] = er
                logger.debug("Created education_relevance from top-level is_education_related field")
        
        if 'education_relevance' in normalized and isinstance(normalized['education_relevance'], dict):
            er = normalized['education_relevance']
            # Map institution_name -> institution_identified
            if 'institution_name' in er and 'institution_identified' not in er:
                er['institution_identified'] = er.pop('institution_name')
            
            # Remove confidence if present (no longer in schema)
            if 'confidence' in er:
                del er['confidence']
            if 'confidence_score' in er:
                del er['confidence_score']
            
            # Ensure required fields exist
            if 'is_education_related' not in er:
                er['is_education_related'] = False
                logger.warning("is_education_related missing from education_relevance, defaulting to False")
            if 'reasoning' not in er:
                er['reasoning'] = "Education relevance check not provided by LLM"
                logger.warning("reasoning missing from education_relevance, using default")
            if 'institution_identified' not in er:
                er['institution_identified'] = None
        
        # Remove url_scores if present (no longer in schema)
        if 'url_scores' in normalized:
            del normalized['url_scores']
        
        # Map initial_access_description from alternative field names
        if 'initial_access_description' not in normalized:
            for key in ['initial_access', 'access_method', 'how_attacker_gained_access', 'attack_entry_point']:
                if key in normalized:
                    normalized['initial_access_description'] = normalized.pop(key)
                    logger.debug(f"Mapped {key} to initial_access_description")
                    break
        
        # Normalize timeline events
        # Only map field names - don't set defaults, use None for missing fields
        if 'timeline' in normalized and isinstance(normalized['timeline'], list):
            # Filter out non-dict entries (strings, etc.)
            normalized_timeline = []
            for event in normalized['timeline']:
                if isinstance(event, str):
                    # If it's a string, skip it (can't parse structured data from string)
                    logger.warning(f"Timeline event is a string, skipping. String value: {event[:100]}...")
                    continue
                elif isinstance(event, dict):
                    normalized_timeline.append(event)
                else:
                    # Unknown type, skip it
                    logger.warning(f"Timeline event has unknown type {type(event)}, skipping")
            
            # Replace the list with normalized versions
            normalized['timeline'] = normalized_timeline
            
            # Now normalize each event object
            for event in normalized['timeline']:
                if isinstance(event, dict):
                    # Map description -> event_description
                    if 'description' in event and 'event_description' not in event:
                        event['event_description'] = event.pop('description')
                    # Map event -> event_type
                    if 'event' in event and 'event_type' not in event:
                        event['event_type'] = event.pop('event')
                    # Try to map alternative field names, but don't set defaults
                    if 'event_description' not in event:
                        # Try to use any text field
                        for key in ['description', 'details', 'summary', 'note']:
                            if key in event:
                                event['event_description'] = str(event[key])
                                break
                        # If still not found, set to None (don't create placeholder)
                        if 'event_description' not in event:
                            event['event_description'] = None
                    # Normalize event_type to standardized tags
                    if 'event_type' in event and event['event_type']:
                        event_type_val = str(event['event_type']).lower().strip()
                        event_type_map = {
                            'initial access': 'initial_access',
                            'discovered': 'discovery',
                            'discovery': 'discovery',
                            'exploited': 'exploitation',
                            'exploitation': 'exploitation',
                            'impacted': 'impact',
                            'impact': 'impact',
                            'contained': 'containment',
                            'containment': 'containment',
                            'eradicated': 'eradication',
                            'eradication': 'eradication',
                            'recovered': 'recovery',
                            'recovery': 'recovery',
                            'disclosed': 'disclosure',
                            'disclosure': 'disclosure',
                            'notified': 'notification',
                            'notification': 'notification',
                            'investigated': 'investigation',
                            'investigation': 'investigation',
                            'remediated': 'remediation',
                            'remediation': 'remediation',
                        }
                        if event_type_val in ['initial_access', 'discovery', 'exploitation', 'impact', 'containment',
                                            'eradication', 'recovery', 'disclosure', 'notification', 'investigation',
                                            'remediation', 'other']:
                            event['event_type'] = event_type_val
                        elif event_type_val in event_type_map:
                            event['event_type'] = event_type_map[event_type_val]
                            logger.debug(f"Mapped event_type '{event_type_val}' to '{event_type_map[event_type_val]}'")
                        else:
                            # Try partial match
                            for key, value in event_type_map.items():
                                if key in event_type_val:
                                    event['event_type'] = value
                                    logger.debug(f"Mapped event_type '{event_type_val}' to '{value}'")
                                    break
                            else:
                                event['event_type'] = 'other'
                                logger.debug(f"Could not map event_type '{event_type_val}', using 'other'")
                    
                    # Normalize date_precision
                    if 'date_precision' in event and event['date_precision']:
                        date_precision_val = str(event['date_precision']).lower().strip()
                        if date_precision_val not in ['day', 'month', 'year', 'approximate']:
                            # Map common variations
                            if 'day' in date_precision_val or 'exact' in date_precision_val:
                                event['date_precision'] = 'day'
                            elif 'month' in date_precision_val:
                                event['date_precision'] = 'month'
                            elif 'year' in date_precision_val:
                                event['date_precision'] = 'year'
                            else:
                                event['date_precision'] = 'approximate'
                    
                    # If event_type not found, set to None (don't create default)
                    if 'event_type' not in event:
                        event['event_type'] = None
                    # If date not found, set to None
                    if 'date' not in event:
                        event['date'] = None
                    # If date_precision not found, set to None
                    if 'date_precision' not in event:
                        event['date_precision'] = None
                    # If indicators not found or empty, set to None instead of empty list
                    if 'indicators' not in event:
                        event['indicators'] = None
                    elif isinstance(event.get('indicators'), list) and len(event['indicators']) == 0:
                        event['indicators'] = None
        
        # Normalize MITRE attack techniques
        # Only set values that are actually known - use None for missing fields
        if 'mitre_attack_techniques' in normalized and isinstance(normalized['mitre_attack_techniques'], list):
            # Convert string entries to objects (e.g., "T1078: Valid Accounts" -> {technique_id: "T1078", technique_name: "Valid Accounts"})
            normalized_techniques = []
            for tech in normalized['mitre_attack_techniques']:
                if isinstance(tech, str):
                    # Try to parse string like "T1078: Valid Accounts" or "T1078"
                    tech_str = tech.strip()
                    if tech_str.startswith('T') and ':' in tech_str:
                        # Format: "T1078: Valid Accounts"
                        parts = tech_str.split(':', 1)
                        technique_id = parts[0].strip()
                        technique_name = parts[1].strip() if len(parts) > 1 else None
                        normalized_techniques.append({
                            'technique_id': technique_id,
                            'technique_name': technique_name,
                            'tactic': None,
                            'confidence': None,
                            'description': None,
                            'sub_techniques': None
                        })
                        logger.debug(f"Converted MITRE technique string '{tech_str}' to object")
                    elif tech_str.startswith('T'):
                        # Format: "T1078" (just ID)
                        normalized_techniques.append({
                            'technique_id': tech_str,
                            'technique_name': None,
                            'tactic': None,
                            'confidence': None,
                            'description': None,
                            'sub_techniques': None
                        })
                        logger.debug(f"Converted MITRE technique ID string '{tech_str}' to object")
                    else:
                        # Can't parse, skip it
                        logger.warning(f"Could not parse MITRE technique string '{tech_str}', skipping")
                elif isinstance(tech, dict):
                    normalized_techniques.append(tech)
                else:
                    # Unknown type, skip it
                    logger.warning(f"MITRE technique entry has unknown type {type(tech)}, skipping")
            
            # Replace the list with normalized versions
            normalized['mitre_attack_techniques'] = normalized_techniques
            
            # Now normalize each technique object
            for tech in normalized['mitre_attack_techniques']:
                if isinstance(tech, dict):
                    # Map technique_id from alternative field names, but don't set default
                    if 'technique_id' not in tech:
                        # Try other field names
                        for key in ['id', 'technique', 'mitre_id', 'attack_id']:
                            if key in tech:
                                tech['technique_id'] = str(tech[key])
                                break
                        # If still not found, set to None (don't create placeholder)
                        if 'technique_id' not in tech:
                            tech['technique_id'] = None
                    
                    # Map technique_name from alternative field names, but don't set default
                    if 'technique_name' not in tech:
                        # Try other field names
                        for key in ['name', 'technique', 'title', 'attack_name']:
                            if key in tech:
                                tech['technique_name'] = str(tech[key])
                                break
                        # If still not found, set to None (don't create placeholder)
                        if 'technique_name' not in tech:
                            tech['technique_name'] = None
                    
                    # Map tactic from alternative field names, but don't set default
                    if 'tactic' not in tech:
                        # Try other field names
                        for key in ['tactic_name', 'phase', 'kill_chain_phase', 'mitre_tactic']:
                            if key in tech:
                                tech['tactic'] = str(tech[key])
                                break
                        # Try to infer from tactic_id if available
                        if 'tactic' not in tech and 'tactic_id' in tech:
                            tactic_id = tech['tactic_id']
                            # Map common tactic IDs to names
                            tactic_map = {
                                'TA0001': 'Initial Access',
                                'TA0002': 'Execution',
                                'TA0003': 'Persistence',
                                'TA0004': 'Privilege Escalation',
                                'TA0005': 'Defense Evasion',
                                'TA0006': 'Credential Access',
                                'TA0007': 'Discovery',
                                'TA0008': 'Lateral Movement',
                                'TA0009': 'Collection',
                                'TA0010': 'Command and Control',
                                'TA0011': 'Exfiltration',
                                'TA0040': 'Impact',
                            }
                            tech['tactic'] = tactic_map.get(tactic_id, None)
                        # If still not found, set to None
                        if 'tactic' not in tech:
                            tech['tactic'] = None
                    
                    # Remove confidence if present (no longer in schema)
                    if 'confidence' in tech:
                        del tech['confidence']
                    if 'confidence_level' in tech:
                        del tech['confidence_level']
                    if 'certainty' in tech:
                        del tech['certainty']
                    
                    # Map description from alternative field names, but don't set default
                    if 'description' not in tech:
                        # Try other field names
                        for key in ['how_used', 'usage', 'details', 'explanation']:
                            if key in tech:
                                tech['description'] = str(tech[key])
                                break
                        # If still not found, set to None (don't create placeholder)
                        if 'description' not in tech:
                            tech['description'] = None
                    
                    # If sub_techniques not found or empty, set to None instead of empty list
                    if 'sub_techniques' not in tech:
                        tech['sub_techniques'] = None
                    elif isinstance(tech.get('sub_techniques'), list) and len(tech['sub_techniques']) == 0:
                        tech['sub_techniques'] = None
        
        # Normalize attack_dynamics
        # Handle case where attack_dynamics is a string instead of an object
        if 'attack_dynamics' in normalized:
            if isinstance(normalized['attack_dynamics'], str):
                # If it's a string, it's likely a description or summary, not structured data
                # Set to None since we can't parse structured data from a string
                logger.warning(f"attack_dynamics is a string, setting to None. String value: {normalized['attack_dynamics'][:100]}...")
                normalized['attack_dynamics'] = None
            elif isinstance(normalized['attack_dynamics'], dict):
                ad = normalized['attack_dynamics']
                
                # Handle attack_vector - convert list to string and normalize to standardized tags
                if 'attack_vector' in ad:
                    attack_vector_val = ad['attack_vector']
                    if isinstance(attack_vector_val, list):
                        # If it's a list, take the first element
                        if len(attack_vector_val) > 0:
                            attack_vector_val = attack_vector_val[0]
                        else:
                            ad['attack_vector'] = None
                            attack_vector_val = None
                    
                    if attack_vector_val is not None:
                        # Normalize to standardized tags
                        attack_vector_str = str(attack_vector_val).lower().strip()
                        # Map common variations to standardized tags
                        attack_vector_map = {
                            'phishing email': 'phishing',
                            'email phishing': 'phishing',
                            'phish': 'phishing',
                            'spear phishing': 'spear_phishing',
                            'targeted phishing': 'spear_phishing',
                            'vulnerability': 'vulnerability_exploit',
                            'exploit': 'vulnerability_exploit',
                            'cve': 'vulnerability_exploit',
                            'credential stuffing': 'credential_stuffing',
                            'credential reuse': 'credential_stuffing',
                            'stolen credentials': 'credential_theft',
                            'credential theft': 'credential_theft',
                            'compromised credentials': 'credential_theft',
                            'malware attack': 'malware',
                            'ransomware attack': 'ransomware',
                            'insider': 'insider_threat',
                            'insider attack': 'insider_threat',
                            'social engineering': 'social_engineering',
                            'supply chain attack': 'supply_chain',
                            'third party': 'third_party_breach',
                            'vendor breach': 'third_party_breach',
                            'misconfig': 'misconfiguration',
                            'misconfiguration': 'misconfiguration',
                            'brute force': 'brute_force',
                            'ddos attack': 'ddos',
                            'distributed denial of service': 'ddos',
                            'sql injection': 'sql_injection',
                            'sqli': 'sql_injection',
                            'xss': 'xss',
                            'cross-site scripting': 'xss',
                        }
                        # Check if it matches a standardized tag directly
                        if attack_vector_str in ['phishing', 'spear_phishing', 'vulnerability_exploit', 'credential_stuffing',
                                                'credential_theft', 'malware', 'ransomware', 'insider_threat', 'social_engineering',
                                                'supply_chain', 'third_party_breach', 'misconfiguration', 'brute_force', 'ddos',
                                                'sql_injection', 'xss', 'other']:
                            ad['attack_vector'] = attack_vector_str
                        elif attack_vector_str in attack_vector_map:
                            ad['attack_vector'] = attack_vector_map[attack_vector_str]
                            logger.debug(f"Mapped attack_vector '{attack_vector_val}' to '{ad['attack_vector']}'")
                        else:
                            # Try to find partial match
                            for key, value in attack_vector_map.items():
                                if key in attack_vector_str:
                                    ad['attack_vector'] = value
                                    logger.debug(f"Mapped attack_vector '{attack_vector_val}' to '{value}' via partial match")
                                    break
                            else:
                                # No match found, use 'other'
                                ad['attack_vector'] = 'other'
                                logger.debug(f"Could not map attack_vector '{attack_vector_val}', using 'other'")
                
                # Handle data_exfiltration - convert to boolean only if present
                if 'data_exfiltration' in ad:
                    exfil_val = ad['data_exfiltration']
                    if isinstance(exfil_val, dict):
                        # If it's an object with 'confirmed' or similar, extract boolean
                        confirmed = exfil_val.get('confirmed', False) or exfil_val.get('occurred', False)
                        ad['data_exfiltration'] = confirmed if confirmed else None
                    elif isinstance(exfil_val, str):
                        # If it's a string like "Unknown", "Yes", "No", convert to boolean
                        exfil_str = exfil_val.lower().strip()
                        if exfil_str in ['yes', 'true', 'confirmed', 'occurred', '1']:
                            ad['data_exfiltration'] = True
                        elif exfil_str in ['no', 'false', 'not', 'none', '0']:
                            ad['data_exfiltration'] = False
                        else:
                            # Unknown/ambiguous, set to None (don't assume False)
                            ad['data_exfiltration'] = None
                            logger.debug(f"Converted ambiguous data_exfiltration value '{exfil_val}' to None")
                    else:
                        ad['data_exfiltration'] = bool(exfil_val) if exfil_val else None
                # If not present, leave as None (don't set default)
                
                # Handle impact_scope - convert types when present, but don't set defaults
                if 'impact_scope' in ad:
                    if not isinstance(ad['impact_scope'], dict):
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
                            # Unknown type, set to None (don't use empty dict)
                            ad['impact_scope'] = None
                # If not present, leave as None (don't set default)
                
                # Normalize attack_chain to standardized tags
                if 'attack_chain' in ad and ad['attack_chain']:
                    if isinstance(ad['attack_chain'], list):
                        normalized_chain = []
                        chain_map = {
                            'reconnaissance': 'reconnaissance',
                            'recon': 'reconnaissance',
                            'weaponization': 'weaponization',
                            'weaponize': 'weaponization',
                            'delivery': 'delivery',
                            'exploitation': 'exploitation',
                            'exploit': 'exploitation',
                            'installation': 'installation',
                            'install': 'installation',
                            'command and control': 'command_and_control',
                            'c2': 'command_and_control',
                            'c&c': 'command_and_control',
                            'actions on objectives': 'actions_on_objectives',
                            'actions': 'actions_on_objectives',
                            'exfiltration': 'exfiltration',
                            'exfil': 'exfiltration',
                            'impact': 'impact',
                        }
                        for stage in ad['attack_chain']:
                            stage_str = str(stage).lower().strip()
                            if stage_str in ['reconnaissance', 'weaponization', 'delivery', 'exploitation', 'installation',
                                           'command_and_control', 'actions_on_objectives', 'exfiltration', 'impact']:
                                normalized_chain.append(stage_str)
                            elif stage_str in chain_map:
                                normalized_chain.append(chain_map[stage_str])
                                logger.debug(f"Mapped attack_chain stage '{stage}' to '{chain_map[stage_str]}'")
                            else:
                                # Try partial match
                                matched = False
                                for key, value in chain_map.items():
                                    if key in stage_str:
                                        normalized_chain.append(value)
                                        logger.debug(f"Mapped attack_chain stage '{stage}' to '{value}'")
                                        matched = True
                                        break
                                if not matched:
                                    logger.debug(f"Could not map attack_chain stage '{stage}', skipping")
                        ad['attack_chain'] = normalized_chain if normalized_chain else None
                    else:
                        ad['attack_chain'] = None
                elif 'attack_chain' not in ad or (isinstance(ad.get('attack_chain'), list) and len(ad.get('attack_chain', [])) == 0):
                    ad['attack_chain'] = None
                
                if 'ransom_demanded' not in ad:
                    # Don't infer - only set if explicitly mentioned
                    ad['ransom_demanded'] = None
                
                # Handle business_impact - normalize to standardized tags
                if 'business_impact' in ad:
                    business_impact_val = ad['business_impact']
                    if isinstance(business_impact_val, list):
                        if len(business_impact_val) > 0:
                            business_impact_val = business_impact_val[0]
                        else:
                            ad['business_impact'] = None
                            business_impact_val = None
                    
                    if business_impact_val is not None:
                        business_impact_str = str(business_impact_val).lower().strip()
                        # Map to standardized tags
                        if business_impact_str in ['critical', 'severe', 'moderate', 'limited', 'minimal']:
                            ad['business_impact'] = business_impact_str
                        else:
                            # Map common variations
                            impact_map = {
                                'very critical': 'critical',
                                'extremely critical': 'critical',
                                'high': 'severe',
                                'very severe': 'severe',
                                'major': 'severe',
                                'medium': 'moderate',
                                'moderate impact': 'moderate',
                                'low': 'limited',
                                'minor': 'limited',
                                'minimal impact': 'minimal',
                                'negligible': 'minimal',
                            }
                            if business_impact_str in impact_map:
                                ad['business_impact'] = impact_map[business_impact_str]
                                logger.debug(f"Mapped business_impact '{business_impact_val}' to '{ad['business_impact']}'")
                            else:
                                # Try partial match
                                for key, value in impact_map.items():
                                    if key in business_impact_str:
                                        ad['business_impact'] = value
                                        logger.debug(f"Mapped business_impact '{business_impact_val}' to '{value}'")
                                        break
                                else:
                                    ad['business_impact'] = None
                                    logger.debug(f"Could not map business_impact '{business_impact_val}', setting to None")
                
                # Handle encryption_impact - normalize to standardized tags
                if 'encryption_impact' in ad:
                    encryption_impact_val = ad['encryption_impact']
                    if isinstance(encryption_impact_val, list):
                        if len(encryption_impact_val) > 0:
                            encryption_impact_val = encryption_impact_val[0]
                        else:
                            ad['encryption_impact'] = None
                            encryption_impact_val = None
                    
                    if encryption_impact_val is not None:
                        encryption_impact_str = str(encryption_impact_val).lower().strip()
                        # Map to standardized tags
                        if encryption_impact_str in ['full', 'partial', 'none']:
                            ad['encryption_impact'] = encryption_impact_str
                        else:
                            # Map common variations
                            if 'full' in encryption_impact_str or 'complete' in encryption_impact_str or 'entire' in encryption_impact_str:
                                ad['encryption_impact'] = 'full'
                            elif 'partial' in encryption_impact_str or 'some' in encryption_impact_str or 'portion' in encryption_impact_str:
                                ad['encryption_impact'] = 'partial'
                            elif 'none' in encryption_impact_str or 'no' in encryption_impact_str or 'not encrypted' in encryption_impact_str:
                                ad['encryption_impact'] = 'none'
                            else:
                                ad['encryption_impact'] = None
                                logger.debug(f"Could not map encryption_impact '{encryption_impact_val}', setting to None")
                
                # Handle ransomware_family - convert list to string if needed
                if 'ransomware_family' in ad:
                    ransomware_family_val = ad['ransomware_family']
                    if isinstance(ransomware_family_val, list):
                        if len(ransomware_family_val) > 0:
                            ad['ransomware_family'] = str(ransomware_family_val[0]) if len(ransomware_family_val) == 1 else ', '.join(str(v) for v in ransomware_family_val)
                        else:
                            ad['ransomware_family'] = None
                        logger.debug(f"Converted ransomware_family list to string: {ad['ransomware_family']}")
                    elif not isinstance(ransomware_family_val, str) and ransomware_family_val is not None:
                        ad['ransomware_family'] = str(ransomware_family_val)
                
                if 'business_impact' not in ad:
                    # Try to infer from other fields
                    if 'impact' in ad:
                        impact_val = ad.pop('impact')
                        # Don't use 'unknown' as a value - set to None instead
                        if isinstance(impact_val, str) and impact_val.lower().strip() in ['unknown', 'n/a', 'not available']:
                            ad['business_impact'] = None
                        else:
                            ad['business_impact'] = impact_val
                    elif 'severity' in ad:
                        severity_val = ad.pop('severity')
                        # Don't use 'unknown' as a value - set to None instead
                        if isinstance(severity_val, str) and severity_val.lower().strip() in ['unknown', 'n/a', 'not available']:
                            ad['business_impact'] = None
                        else:
                            ad['business_impact'] = severity_val
                    else:
                        # Set to None if not found (don't use 'unknown' sentinel)
                        ad['business_impact'] = None
                
                # Normalize operational_impact to standardized tags
                if 'operational_impact' in ad and ad['operational_impact']:
                    if isinstance(ad['operational_impact'], list):
                        normalized_ops = []
                        ops_map = {
                            'teaching disrupted': 'teaching_disrupted',
                            'teaching': 'teaching_disrupted',
                            'research disrupted': 'research_disrupted',
                            'research': 'research_disrupted',
                            'admissions disrupted': 'admissions_disrupted',
                            'admissions': 'admissions_disrupted',
                            'enrollment disrupted': 'enrollment_disrupted',
                            'enrollment': 'enrollment_disrupted',
                            'payroll disrupted': 'payroll_disrupted',
                            'payroll': 'payroll_disrupted',
                            'clinical operations disrupted': 'clinical_operations_disrupted',
                            'clinical': 'clinical_operations_disrupted',
                            'online learning disrupted': 'online_learning_disrupted',
                            'online learning': 'online_learning_disrupted',
                            'classes cancelled': 'classes_cancelled',
                            'classes canceled': 'classes_cancelled',
                            'exams postponed': 'exams_postponed',
                            'graduation delayed': 'graduation_delayed',
                            'email system down': 'email_system_down',
                            'email down': 'email_system_down',
                            'student portal down': 'student_portal_down',
                            'portal down': 'student_portal_down',
                            'network down': 'network_down',
                            'website down': 'website_down',
                        }
                        for impact in ad['operational_impact']:
                            impact_str = str(impact).lower().strip()
                            if impact_str in ['teaching_disrupted', 'research_disrupted', 'admissions_disrupted',
                                            'enrollment_disrupted', 'payroll_disrupted', 'clinical_operations_disrupted',
                                            'online_learning_disrupted', 'classes_cancelled', 'exams_postponed',
                                            'graduation_delayed', 'email_system_down', 'student_portal_down',
                                            'network_down', 'website_down', 'other']:
                                normalized_ops.append(impact_str)
                            elif impact_str in ops_map:
                                normalized_ops.append(ops_map[impact_str])
                                logger.debug(f"Mapped operational_impact '{impact}' to '{ops_map[impact_str]}'")
                            else:
                                # Try partial match
                                matched = False
                                for key, value in ops_map.items():
                                    if key in impact_str:
                                        normalized_ops.append(value)
                                        logger.debug(f"Mapped operational_impact '{impact}' to '{value}'")
                                        matched = True
                                        break
                                if not matched:
                                    normalized_ops.append('other')
                                    logger.debug(f"Could not map operational_impact '{impact}', using 'other'")
                        ad['operational_impact'] = normalized_ops if normalized_ops else None
                    else:
                        ad['operational_impact'] = None
                elif 'operational_impact' not in ad or (isinstance(ad.get('operational_impact'), list) and len(ad.get('operational_impact', [])) == 0):
                    ad['operational_impact'] = None
        
        # Normalize system_impact.systems_affected
        if 'system_impact' in normalized and isinstance(normalized['system_impact'], dict):
            si = normalized['system_impact']
            if 'systems_affected' in si and si['systems_affected']:
                if isinstance(si['systems_affected'], list):
                    normalized_systems = []
                    systems_map = {
                        'email system': 'email_system',
                        'email': 'email_system',
                        'mail': 'email_system',
                        'mail server': 'email_system',
                        'student portal': 'student_portal',
                        'portal': 'student_portal',
                        'student information system': 'student_portal',
                        'sis': 'student_portal',
                        'learning management system': 'learning_management_system',
                        'lms': 'learning_management_system',
                        'research systems': 'research_systems',
                        'research': 'research_systems',
                        'hospital systems': 'hospital_systems',
                        'hospital': 'hospital_systems',
                        'financial systems': 'financial_systems',
                        'financial': 'financial_systems',
                        'financial software': 'financial_systems',
                        'accounting system': 'financial_systems',
                        'admissions system': 'admissions_system',
                        'admissions': 'admissions_system',
                        'enrollment system': 'enrollment_system',
                        'enrollment': 'enrollment_system',
                        'payroll system': 'payroll_system',
                        'payroll': 'payroll_system',
                        'network infrastructure': 'network_infrastructure',
                        'network': 'network_infrastructure',
                        'internet': 'network_infrastructure',
                        'voip': 'other',
                        'voip phones': 'other',
                        'phone system': 'other',
                        'telephony': 'other',
                        'cloud services': 'cloud_services',
                        'cloud': 'cloud_services',
                        'database servers': 'database_servers',
                        'database': 'database_servers',
                        'web servers': 'web_servers',
                        'web': 'web_servers',
                        'file servers': 'file_servers',
                        'backup systems': 'backup_systems',
                        'backup': 'backup_systems',
                        'backups': 'backup_systems',
                        'third party vendor': 'third_party_vendor',
                        'vendor': 'third_party_vendor',
                    }
                    for system in si['systems_affected']:
                        system_str = str(system).lower().strip()
                        if system_str in ['email_system', 'student_portal', 'learning_management_system', 'research_systems',
                                        'hospital_systems', 'financial_systems', 'admissions_system', 'enrollment_system',
                                        'payroll_system', 'network_infrastructure', 'cloud_services', 'database_servers',
                                        'web_servers', 'file_servers', 'backup_systems', 'third_party_vendor', 'other']:
                            normalized_systems.append(system_str)
                        elif system_str in systems_map:
                            normalized_systems.append(systems_map[system_str])
                            logger.debug(f"Mapped systems_affected '{system}' to '{systems_map[system_str]}'")
                        else:
                            # Try partial match
                            matched = False
                            for key, value in systems_map.items():
                                if key in system_str:
                                    normalized_systems.append(value)
                                    logger.debug(f"Mapped systems_affected '{system}' to '{value}'")
                                    matched = True
                                    break
                            if not matched:
                                normalized_systems.append('other')
                                logger.debug(f"Could not map systems_affected '{system}', using 'other'")
                    si['systems_affected'] = normalized_systems if normalized_systems else None
        
        # Normalize recovery_metrics.recovery_phases
        if 'recovery_metrics' in normalized and isinstance(normalized['recovery_metrics'], dict):
            rm = normalized['recovery_metrics']
            if 'recovery_phases' in rm and rm['recovery_phases']:
                if isinstance(rm['recovery_phases'], list):
                    normalized_phases = []
                    phases_map = {
                        'containment': 'containment',
                        'contain': 'containment',
                        'eradication': 'eradication',
                        'eradicate': 'eradication',
                        'recovery': 'recovery',
                        'recover': 'recovery',
                        'lessons learned': 'lessons_learned',
                        'lessons_learned': 'lessons_learned',
                        'post incident review': 'post_incident_review',
                        'post-incident review': 'post_incident_review',
                        'post_incident_review': 'post_incident_review',
                    }
                    for phase in rm['recovery_phases']:
                        phase_str = str(phase).lower().strip()
                        if phase_str in ['containment', 'eradication', 'recovery', 'lessons_learned', 'post_incident_review']:
                            normalized_phases.append(phase_str)
                        elif phase_str in phases_map:
                            normalized_phases.append(phases_map[phase_str])
                            logger.debug(f"Mapped recovery_phases '{phase}' to '{phases_map[phase_str]}'")
                        else:
                            # Try partial match
                            matched = False
                            for key, value in phases_map.items():
                                if key in phase_str:
                                    normalized_phases.append(value)
                                    logger.debug(f"Mapped recovery_phases '{phase}' to '{value}'")
                                    matched = True
                                    break
                            if not matched:
                                logger.debug(f"Could not map recovery_phases '{phase}', skipping")
                    rm['recovery_phases'] = normalized_phases if normalized_phases else None
        
        # Ensure education_relevance exists (required field)
        if 'education_relevance' not in normalized:
            # Create default education_relevance if completely missing
            normalized['education_relevance'] = {
                'is_education_related': False,
                'reasoning': 'Education relevance check not provided by LLM',
                'institution_identified': None
            }
            logger.warning("education_relevance completely missing from LLM response, creating default")
        
        # Ensure required fields exist with defaults
        if 'enriched_summary' not in normalized:
            # Try 'summary' as fallback
            if 'summary' in normalized:
                normalized['enriched_summary'] = normalized.pop('summary')
            else:
                normalized['enriched_summary'] = "Summary not provided by LLM"
        
        # Remove extraction_confidence if present (no longer in schema)
        if 'extraction_confidence' in normalized:
            del normalized['extraction_confidence']
        
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
        
        # Retry logic with exponential backoff for rate limits
        last_error = None
        rate_limit_errors = 0
        max_rate_limit_retries = 5  # Maximum consecutive rate limit errors before giving up
        base_backoff = 2.0  # Base backoff in seconds (2^attempt)
        max_backoff = 300.0  # Maximum backoff: 5 minutes
        
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
                # Reset rate limit error counter on success
                rate_limit_errors = 0
                break
            except RateLimitError as e:
                rate_limit_errors += 1
                last_error = e
                
                if rate_limit_errors >= max_rate_limit_retries:
                    logger.error(f"Rate limit persisted after {max_rate_limit_retries} attempts. Stopping enrichment.")
                    print(f"[RATE LIMIT] ✗ Rate limit error persisted after {max_rate_limit_retries} attempts. Stopping enrichment.", flush=True)
                    raise RateLimitError(
                        f"Rate limit persisted after {max_rate_limit_retries} attempts. "
                        f"Please wait and retry later. Last error: {e}"
                    ) from e
                
                # Exponential backoff for rate limits
                backoff_time = min(base_backoff ** rate_limit_errors, max_backoff)
                logger.warning(
                    f"Rate limit error (attempt {rate_limit_errors}/{max_rate_limit_retries}): {e}. "
                    f"Waiting {backoff_time:.1f}s before retry..."
                )
                print(
                    f"[RATE LIMIT] ⚠ Rate limit detected (attempt {rate_limit_errors}/{max_rate_limit_retries}). "
                    f"Waiting {backoff_time:.1f}s...",
                    flush=True
                )
                time.sleep(backoff_time)
                
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying...")
                    time.sleep(1.0 * (attempt + 1))  # Linear backoff for non-rate-limit errors
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
                wrapper_keys = [k for k in parsed.keys() if k not in ['education_relevance', 'primary_url', 'timeline', 'mitre_attack_techniques', 'attack_dynamics', 'enriched_summary', 'initial_access_description', 'data_impact', 'system_impact', 'user_impact', 'operational_impact_metrics', 'financial_impact', 'regulatory_impact', 'recovery_metrics', 'transparency_metrics', 'research_impact']]
                if wrapper_keys:
                    # Unwrap one level
                    parsed = parsed[wrapper_keys[0]]
                    logger.debug(f"Unwrapped response from wrapper key: {wrapper_keys[0]}")
            
            # Normalize LLM response to match our schema
            parsed = self._normalize_llm_response(parsed, schema_model)
            
            # Try validation with lenient error handling
            try:
                validated = schema_model.model_validate(parsed)
                logger.debug(f"✓ Response validated against {schema_model.__name__} schema")
                return validated
            except Exception as validation_error:
                # Log the validation error but try to fix common issues
                logger.warning(f"Initial validation failed: {validation_error}")
                logger.debug(f"Attempting to fix validation issues...")
                
                # Try to fix common validation issues
                try:
                    # Re-normalize with more aggressive fixes
                    parsed = self._normalize_llm_response(parsed, schema_model)
                    
                    # Try validation again
                    validated = schema_model.model_validate(parsed)
                    logger.info(f"✓ Response validated after fixes")
                    return validated
                except Exception as e2:
                    # If still fails, log and re-raise
                    logger.error(f"Validation failed even after fixes: {e2}")
                    logger.error(f"Parsed data keys: {list(parsed.keys()) if isinstance(parsed, dict) else 'N/A'}")
                    raise validation_error  # Raise original error
            
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
                expected_fields = ['education_relevance', 'primary_url', 'timeline', 'mitre_attack_techniques', 'attack_dynamics', 'enriched_summary']
                logger.error(f"Expected top-level fields: {expected_fields}")
                logger.error(f"Actual top-level fields: {list(parsed.keys())}")
            raise

