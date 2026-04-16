"""Tests for LLM response parsing and validation."""

from unittest.mock import Mock, patch

import pytest

from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.schemas import EducationRelevanceCheck


class TestLLMResponseValidation:
    """Tests for LLM response validation."""

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_valid_json_response(self, mock_client_class):
        """Valid JSON responses should parse into the requested schema."""
        mock_client = Mock()
        mock_client.chat.return_value = {
            "message": {
                "content": (
                    '{"is_education_related": true, '
                    '"reasoning": "Test", '
                    '"institution_identified": "Test University"}'
                )
            }
        }
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )

        assert isinstance(result, EducationRelevanceCheck)
        assert result.is_education_related is True
        assert result.institution_identified == "Test University"

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_empty_response(self, mock_client_class):
        """Empty response content should raise a clear error."""
        mock_client = Mock()
        mock_client.chat.return_value = {"message": {"content": ""}}
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")

        with pytest.raises(ValueError, match="Empty response from LLM"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_none_response(self, mock_client_class):
        """None content should be treated as empty."""
        mock_client = Mock()
        mock_client.chat.return_value = {"message": {"content": None}}
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")

        with pytest.raises(ValueError, match="Empty response from LLM"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_invalid_json_response(self, mock_client_class):
        """Invalid JSON should raise a parsing error."""
        mock_client = Mock()
        mock_client.chat.return_value = {"message": {"content": "This is not valid JSON"}}
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")

        with pytest.raises(ValueError, match="Invalid JSON response"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_different_response_structures(self, mock_client_class):
        """Alternate response layouts should still be parsed."""
        mock_client = Mock()
        mock_client.chat.return_value = {
            "content": (
                '{"is_education_related": true, '
                '"reasoning": "Test", '
                '"institution_identified": null}'
            )
        }
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )

        assert result.is_education_related is True

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_response_with_text_field(self, mock_client_class):
        """Responses in a top-level text field should be supported."""
        mock_client = Mock()
        mock_client.chat.return_value = {
            "text": (
                '{"is_education_related": true, '
                '"reasoning": "Text field", '
                '"institution_identified": null}'
            )
        }
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )

        assert result.reasoning == "Text field"

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_response_validation_against_schema(self, mock_client_class):
        """Normalization should fill legacy top-level relevance fields into the schema."""
        mock_client = Mock()
        mock_client.chat.return_value = {
            "message": {
                "content": '{"is_education_related": true, "institution_name": "Test University"}'
            }
        }
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )

        assert result.is_education_related is True
        assert result.reasoning == "Education relevance reasoning not provided by LLM"
        assert result.institution_identified == "Test University"

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_api_error_handling(self, mock_client_class):
        """Transport errors should bubble up cleanly."""
        mock_client = Mock()
        mock_client.chat.side_effect = Exception("API connection error")
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")

        with pytest.raises(Exception, match="API connection error"):
            client.extract_structured(
                prompt="Test prompt",
                schema_model=EducationRelevanceCheck,
            )

    @patch("src.edu_cti.pipeline.phase2.llm_client.Client")
    def test_markdown_wrapped_json_response(self, mock_client_class):
        """Markdown code fences around JSON should be stripped before parsing."""
        mock_client = Mock()
        mock_client.chat.return_value = {
            "message": {
                "content": (
                    "```json\n"
                    '{"is_education_related": true, "reasoning": "Wrapped", "institution_identified": "Fence University"}\n'
                    "```"
                )
            }
        }
        mock_client_class.return_value = mock_client

        client = OllamaLLMClient(api_key="test_key", host="https://ollama.com", model="test-model")
        result = client.extract_structured(
            prompt="Test prompt",
            schema_model=EducationRelevanceCheck,
        )

        assert result.reasoning == "Wrapped"
        assert result.institution_identified == "Fence University"
