from src.edu_cti.core.discovery_policy import (
    QUERY_SCOPED_HIGH_RECALL,
    discovery_policy_for_source,
    semantic_prefilter_allowed,
)
from src.edu_cti.sources.rss import oxylabs_news


def test_oxylabs_news_policy_forbids_semantic_prefiltering():
    assert discovery_policy_for_source("oxylabs_news") == QUERY_SCOPED_HIGH_RECALL
    assert semantic_prefilter_allowed("oxylabs_news") is False


def test_oxylabs_news_does_not_seed_institution_name_from_title(monkeypatch):
    class DummyOxylabsClient:
        def _is_configured(self):
            return True

        def search_news(self, query, *, max_results, date_from, date_to):
            return [
                {
                    "url": "https://example.com/incidents/leiden-ddos",
                    "title": "Leiden University website down in cyberattack",
                    "description": "Leiden University websites were hit by a DDoS attack.",
                    "source": "Example News",
                }
            ]

    monkeypatch.setattr(oxylabs_news, "OxylabsClient", DummyOxylabsClient)
    monkeypatch.setattr(oxylabs_news, "OXYLABS_QUERIES", ["education cyber attack"])
    monkeypatch.setattr(oxylabs_news, "REQUEST_DELAY", 0)

    incidents = oxylabs_news.build_oxylabs_news_incidents(incremental=True, max_age_days=1)

    assert len(incidents) == 1
    assert incidents[0].institution_name == ""
    assert incidents[0].title == "Leiden University website down in cyberattack"
    assert incidents[0].raw_source_payload["discovery_policy"] == QUERY_SCOPED_HIGH_RECALL


def test_oxylabs_news_keeps_broad_query_hits_for_llm_review(monkeypatch):
    class DummyOxylabsClient:
        def _is_configured(self):
            return True

        def search_news(self, query, *, max_results, date_from, date_to):
            return [
                {
                    "url": "https://example.com/features/cyber-profile",
                    "title": "El guardián del ciberespacio",
                    "description": (
                        "Identificar los puntos débiles de los sistemas de información "
                        "de instituciones públicas y privadas es parte del trabajo..."
                    ),
                    "source": "Example News",
                }
            ]

    monkeypatch.setattr(oxylabs_news, "OxylabsClient", DummyOxylabsClient)
    monkeypatch.setattr(oxylabs_news, "OXYLABS_QUERIES", ["colegio ciberataque"])
    monkeypatch.setattr(oxylabs_news, "REQUEST_DELAY", 0)

    incidents = oxylabs_news.build_oxylabs_news_incidents(incremental=True, max_age_days=1)

    assert len(incidents) == 1
    assert incidents[0].title == "El guardián del ciberespacio"
    assert incidents[0].all_urls == ["https://example.com/features/cyber-profile"]
