"""Tests for RSS feed pipeline functionality."""

from unittest.mock import patch, MagicMock

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.pipeline.phase1.rss import collect_rss_incidents

class TestRSSPipeline:
    """Test RSS feed pipeline functionality."""
    
    @patch("src.edu_cti.pipeline.phase1.rss.validate_source_names")
    @patch("src.edu_cti.pipeline.phase1.rss.get_rss_builder")
    def test_collect_rss_incidents_with_sources(self, mock_get_builder, mock_validate_sources):
        """Test collecting incidents from specific RSS sources."""
        # Mock RSS builder
        mock_builder = MagicMock(return_value=[
            BaseIncident(
                incident_id="rss_test_1",
                source="databreaches_rss",
                source_event_id="guid_123",
                institution_name="Test University",
                victim_raw_name="Test University",
                institution_type="University",
                country="US",
                region=None,
                city=None,
                incident_date="2024-01-01",
                date_precision="day",
                source_published_date="2024-01-01",
                ingested_at="2024-01-01T00:00:00Z",
                title="Test RSS Incident",
                subtitle=None,
                primary_url=None,
                all_urls=["https://example.com/rss"],
                leak_site_url=None,
                source_detail_url=None,
                screenshot_url=None,
                attack_type_hint=None,
                status="suspected",
                source_confidence="medium",
                notes=None,
            )
        ])
        
        mock_get_builder.return_value = mock_builder
        mock_validate_sources.return_value = ["databreaches_rss"]

        results = collect_rss_incidents(sources=["databreaches_rss"], max_age_days=1)

        assert "databreaches_rss" in results
        assert len(results["databreaches_rss"]) == 1
        assert results["databreaches_rss"][0].incident_id == "rss_test_1"
        mock_validate_sources.assert_called_once_with("rss", ["databreaches_rss"], include_paid=False)
        mock_get_builder.assert_called_once_with("databreaches_rss", include_paid=False)
        mock_builder.assert_called_once_with(max_age_days=1)
    
    @patch("src.edu_cti.pipeline.phase1.rss.validate_source_names")
    @patch("src.edu_cti.pipeline.phase1.rss.get_rss_builder")
    def test_collect_rss_incidents_with_max_age_days(self, mock_get_builder, mock_validate_sources):
        """Test that max_age_days parameter is passed to RSS builders."""
        mock_builder = MagicMock(return_value=[])
        mock_get_builder.return_value = mock_builder
        mock_validate_sources.return_value = ["databreaches_rss"]

        collect_rss_incidents(sources=["databreaches_rss"], max_age_days=7)

        mock_get_builder.assert_called_once_with("databreaches_rss", include_paid=False)
        mock_builder.assert_called_once_with(max_age_days=7)

    @patch("src.edu_cti.pipeline.phase1.rss.get_rss_sources")
    @patch("src.edu_cti.pipeline.phase1.rss.get_rss_builder")
    def test_collect_rss_incidents_can_include_paid_sources(self, mock_get_builder, mock_get_rss_sources):
        """Historical runs can opt into paid RSS/search sources such as Oxylabs News."""
        free_incident = BaseIncident(
            incident_id="rss_test_1",
            source="databreaches_rss",
            source_event_id="guid_123",
            institution_name="Test University",
            victim_raw_name="Test University",
            institution_type="University",
            country="US",
            region=None,
            city=None,
            incident_date="2024-01-01",
            date_precision="day",
            source_published_date="2024-01-01",
            ingested_at="2024-01-01T00:00:00Z",
            title="Test RSS Incident",
            subtitle=None,
            primary_url=None,
            all_urls=["https://example.com/rss"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )
        paid_incident = BaseIncident(
            incident_id="ox_test_1",
            source="oxylabs_news",
            source_event_id="guid_456",
            institution_name="Paid Source University",
            victim_raw_name="Paid Source University",
            institution_type="University",
            country="US",
            region=None,
            city=None,
            incident_date="2024-01-02",
            date_precision="day",
            source_published_date="2024-01-02",
            ingested_at="2024-01-02T00:00:00Z",
            title="Oxylabs Incident",
            subtitle=None,
            primary_url=None,
            all_urls=["https://example.com/oxylabs"],
            leak_site_url=None,
            source_detail_url=None,
            screenshot_url=None,
            attack_type_hint=None,
            status="suspected",
            source_confidence="medium",
            notes=None,
        )

        mock_get_rss_sources.return_value = ["databreaches_rss", "oxylabs_news"]
        mock_get_builder.side_effect = lambda name, include_paid=False: {
            "databreaches_rss": MagicMock(return_value=[free_incident]),
            "oxylabs_news": MagicMock(return_value=[paid_incident]),
        }[name]

        results = collect_rss_incidents(include_paid=True, max_age_days=30)

        assert set(results) == {"databreaches_rss", "oxylabs_news"}
        assert results["oxylabs_news"][0].incident_id == "ox_test_1"
        mock_get_rss_sources.assert_called_once_with(include_paid=True)
        mock_get_builder.assert_any_call("databreaches_rss", include_paid=True)
        mock_get_builder.assert_any_call("oxylabs_news", include_paid=True)
