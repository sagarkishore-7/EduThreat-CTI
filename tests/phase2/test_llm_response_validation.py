"""
Tests for LLM response validation.

Tests that verify LLM returns valid JSON responses and handles errors correctly.
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.schemas import CTIEnrichmentResult, EducationRelevanceCheck


class TestLLMResponseValidation:
    """Tests for LLM response validation."""
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_valid_json_response(self, mock_client_class):
        """Test that valid JSON response is parsed correctly."""
        mock_client = Mock()
        mock_response = {
            'message': {
                'content': '{"is_education_related": true, "confidence": 0.9, "reasoning": "Test", "institution_identified": "Test University"}'
            }
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )
        
        assert isinstance(result, EducationRelevanceCheck)
        assert result.is_education_related is True
        assert result.confidence == 0.9
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_empty_response(self, mock_client_class):
        """Test that empty response raises error."""
        mock_client = Mock()
        mock_response = {
            'message': {
                'content': ''
            }
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        with pytest.raises(ValueError, match="Empty response from LLM"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_none_response(self, mock_client_class):
        """Test that None response raises error."""
        mock_client = Mock()
        mock_response = {
            'message': {
                'content': None
            }
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        with pytest.raises(ValueError, match="Empty response from LLM"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_invalid_json_response(self, mock_client_class):
        """Test that invalid JSON response raises error."""
        mock_client = Mock()
        mock_response = {
            'message': {
                'content': 'This is not valid JSON'
            }
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        with pytest.raises(ValueError, match="Invalid JSON response"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_different_response_structures(self, mock_client_class):
        """Test handling of different response structures."""
        # Test 1: Direct content field
        mock_client = Mock()
        mock_response = {
            'content': '{"is_education_related": true, "confidence": 0.8, "reasoning": "Test", "institution_identified": null}'
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )
        
        assert isinstance(result, EducationRelevanceCheck)
        assert result.is_education_related is True
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_response_with_text_field(self, mock_client_class):
        """Test handling of response with 'text' field."""
        mock_client = Mock()
        mock_response = {
            'text': '{"is_education_related": true, "confidence": 0.7, "reasoning": "Test", "institution_identified": null}'
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )
        
        assert isinstance(result, EducationRelevanceCheck)
        assert result.is_education_related is True
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_response_validation_against_schema(self, mock_client_class):
        """Test that response is validated against Pydantic schema."""
        mock_client = Mock()
        # Missing required fields
        mock_response = {
            'message': {
                'content': '{"is_education_related": true}'
            }
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        # Should raise validation error
        with pytest.raises(Exception):  # Pydantic validation error
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_api_error_handling(self, mock_client_class):
        """Test handling of API errors."""
        mock_client = Mock()
        mock_client.chat.side_effect = Exception("API connection error")
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        with pytest.raises(Exception, match="API connection error"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )

