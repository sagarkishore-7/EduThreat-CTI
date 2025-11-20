"""
Tests for Phase 2: LLM Client

Tests Ollama LLM client integration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.schemas import EducationRelevanceCheck


class TestOllamaLLMClient:
    """Tests for Ollama LLM client."""
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_client_initialization(self, mock_client_class):
        """Test LLM client initialization."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        assert client.model == "test-model"
        assert client.timeout == 120
        mock_client_class.assert_called_once()
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_extract_structured(self, mock_client_class):
        """Test structured extraction from LLM."""
        # Mock the client's chat method
        mock_client = Mock()
        mock_response = Mock()
        mock_response.message.content = '{"is_education_related": true, "confidence": 0.9, "reasoning": "Test", "institution_name": "Test University"}'
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )
        
        # Extract structured data
        result = client.extract_structured(
            prompt="Test prompt",
            schema=EducationRelevanceCheck,
        )
        
        assert isinstance(result, EducationRelevanceCheck)
        assert result.is_education_related is True
        assert result.confidence == 0.9
        assert result.institution_name == "Test University"
    
    @patch('src.edu_cti.pipeline.phase2.llm_client.Client')
    def test_extract_structured_error_handling(self, mock_client_class):
        """Test error handling in structured extraction."""
        # Mock the client to raise an error
        mock_client = Mock()
        mock_client.chat.side_effect = Exception("API error")
        mock_client_class.return_value = mock_client
        
        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
        )
        
        # Should handle error gracefully
        with pytest.raises(Exception):
            client.extract_structured(
                prompt="Test prompt",
                schema=EducationRelevanceCheck,
            )

