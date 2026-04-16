"""Tests for the Ollama LLM client."""

from unittest.mock import Mock, patch

import pytest

from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.schemas import EducationRelevanceCheck


class TestOllamaLLMClient:
    """Tests for Ollama LLM client."""

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_client_initialization(self, mock_client_class):
        """Client should be initialized with the configured host and auth header."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
            model="test-model",
        )

        assert client.model == "test-model"
        assert client.host == "https://ollama.com"
        kwargs = mock_client_class.call_args.kwargs
        assert kwargs["host"] == "https://ollama.com"
        assert kwargs["headers"] == {"Authorization": "Bearer test_key"}

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_extract_structured(self, mock_client_class):
        """Structured extraction should validate into the target schema."""
        mock_client = Mock()
        mock_response = {
            "message": {
                "content": (
                    '{"is_education_related": true, '
                    '"reasoning": "Education-targeted incident", '
                    '"institution_identified": "Test University"}'
                )
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
        assert result.reasoning == "Education-targeted incident"
        assert result.institution_identified == "Test University"

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_extract_structured_normalizes_legacy_field_names(self, mock_client_class):
        """Legacy aliases should still normalize into the current schema."""
        mock_client = Mock()
        mock_response = {
            "message": {
                "content": (
                    '{"incident_review": {"is_education_related": true, '
                    '"reasoning": "Legacy wrapper", '
                    '"institution_name": "Legacy University"}}'
                )
            }
        }
        mock_client.chat.return_value = mock_response
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key")
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )

        assert result.is_education_related is True
        assert result.institution_identified == "Legacy University"

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_extract_structured_error_handling(self, mock_client_class):
        """API errors should surface to the caller."""
        mock_client = Mock()
        mock_client.chat.side_effect = Exception("API error")
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(
            api_key="test_key",
            host="https://ollama.com",
        )

        with pytest.raises(Exception, match="API error"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )
