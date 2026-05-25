"""Unit tests for Google News RSS ingestion behavior."""

from src.edu_cti.sources.rss import googlenews_rss


def test_clean_google_news_description_strips_feed_html():
    """Google RSS descriptions should be plain text before they reach incident subtitles."""

    raw = (
        '<a href="https://news.google.com/rss/articles/CBMi...">Georgia Tech Security Breach '
        'Exposes 1.3 Million Records</a>&nbsp;&nbsp;'
        '<font color="#6f6f6f">Security Magazine</font>'
    )

    cleaned = googlenews_rss._clean_google_news_description(raw)

    assert "<a " not in cleaned
    assert "news.google.com/rss/articles" not in cleaned
    assert "Georgia Tech Security Breach Exposes 1.3 Million Records" in cleaned
    assert "Security Magazine" in cleaned


def test_google_news_relevance_filter_keeps_institutional_incidents():
    assert googlenews_rss._looks_relevant_education_incident(
        title="Canvas outage impacts universities after cyberattack",
        description="Multiple schools reported disrupted access to student systems.",
        query="university cyberattack",
    )
    assert googlenews_rss._looks_relevant_education_incident(
        title="School district ransomware attack exposes student records",
        description="Officials said the security incident affected staff and students.",
        query="school district ransomware",
    )


def test_google_news_relevance_filter_skips_common_noise():
    assert not googlenews_rss._looks_relevant_education_incident(
        title="Top universities launch cybersecurity courses for online learners",
        description="The program offers certificates and training for students.",
        query="university cyberattack",
    )
    assert not googlenews_rss._looks_relevant_education_incident(
        title="College football security plans announced before rivalry game",
        description="Police discussed campus traffic and stadium safety.",
        query="college cyberattack",
    )
    assert not googlenews_rss._looks_relevant_education_incident(
        title="Former University of Michigan football coach indicted for hacking accounts",
        description="Prosecutors described a personal account-hacking case.",
        query="university hacked",
    )


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
        "_resolve_google_news_article_url",
        lambda _link: "https://example.com/resolved-article",
    )
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
    assert "search_country=US" in incidents[0].notes
    assert "search_lang=en" in incidents[0].notes
    assert incidents[0].all_urls == ["https://example.com/resolved-article"]
    assert incidents[0].institution_name is None
    assert isinstance(incidents[0].incident_date, str)
    assert isinstance(saved[0].incident_date, str)


def test_build_googlenews_rss_incidents_filters_low_relevance_items(monkeypatch):
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
                "title": "University ransomware attack disrupts student services",
                "link": "https://news.google.com/articles/relevant",
                "pub_date": "Wed, 15 Apr 2026 16:23:06 +0000",
                "description": "Officials said the security incident affected campus systems.",
                "source_name": "Example News",
            },
            {
                "title": "University launches new cybersecurity certification course",
                "link": "https://news.google.com/articles/course",
                "pub_date": "Wed, 15 Apr 2026 16:23:06 +0000",
                "description": "Online learners can enroll in the training program.",
                "source_name": "Example News",
            },
        ],
    )
    monkeypatch.setattr(
        googlenews_rss,
        "_resolve_google_news_article_url",
        lambda link: f"https://example.com/{link.rsplit('/', 1)[-1]}",
    )
    monkeypatch.setattr(googlenews_rss.time, "sleep", lambda _seconds: None)

    saved = []
    incidents = googlenews_rss.build_googlenews_rss_incidents(
        incremental=True,
        max_age_days=3650,
        save_callback=saved.extend,
    )

    assert len(incidents) == 1
    assert len(saved) == 1
    assert incidents[0].title == "University ransomware attack disrupts student services"
    assert incidents[0].all_urls == ["https://example.com/relevant"]


def test_build_googlenews_rss_incidents_keeps_item_when_wrapper_cannot_be_resolved(monkeypatch):
    monkeypatch.setattr(
        googlenews_rss,
        "GOOGLE_NEWS_QUERIES",
        [("canvas cyberattack", "ja", "JP")],
    )
    monkeypatch.setattr(
        googlenews_rss,
        "_fetch_google_news_rss",
        lambda _url: [
            {
                "title": "Canvas outage impacts universities",
                "link": "https://news.google.com/rss/articles/test-wrapper",
                "pub_date": "Wed, 15 Apr 2026 16:23:06 +0000",
                "description": "Service outage reported by multiple universities",
                "source_name": "Example News",
            }
        ],
    )
    monkeypatch.setattr(googlenews_rss, "_resolve_google_news_article_url", lambda _link: None)
    monkeypatch.setattr(googlenews_rss.time, "sleep", lambda _seconds: None)

    incidents = googlenews_rss.build_googlenews_rss_incidents(
        incremental=True,
        max_age_days=3650,
        save_callback=None,
    )

    assert len(incidents) == 1
    assert incidents[0].source_event_id == "https://news.google.com/rss/articles/test-wrapper"
    assert incidents[0].all_urls == []
