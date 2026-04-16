"""Unit tests for Google News RSS ingestion behavior."""

from src.edu_cti.sources.rss import googlenews_rss


def test_build_googlenews_rss_incidents_serializes_pub_date(monkeypatch):
    """Google News RSS should emit date strings that Phase 1 dedup can safely compare."""

    monkeypatch.setattr(
        googlenews_rss,
        "GOOGLE_NEWS_QUERIES",
        [("university cyberattack", "en", "US")],
    )
    monkeypatch.setattr(
        googlenews_rss,
        "_fetch_google_news_rss",
        lambda _url: [
            {
                "title": "University cyberattack disrupts classes",
                "link": "https://news.google.com/articles/test",
                "pub_date": "Wed, 15 Apr 2026 16:23:06 +0000",
                "description": "Ransomware attack affected student systems",
                "source_name": "Example News",
            }
        ],
    )
    monkeypatch.setattr(
        googlenews_rss,
        "_resolve_google_news_link",
        lambda _url: "https://example.com/story",
    )
    monkeypatch.setattr(googlenews_rss, "_is_cyber_relevant", lambda _text: True)
    monkeypatch.setattr(googlenews_rss.time, "sleep", lambda _seconds: None)

    saved = []
    incidents = googlenews_rss.build_googlenews_rss_incidents(
        incremental=True,
        max_age_days=3650,
        save_callback=saved.extend,
    )

    assert len(incidents) == 1
    assert len(saved) == 1
    assert incidents[0].incident_date == "2026-04-15"
    assert incidents[0].source_published_date == "2026-04-15"
    assert isinstance(incidents[0].incident_date, str)
    assert isinstance(saved[0].incident_date, str)
