from datetime import timezone

from src.edu_cti.core.models import BaseIncident, make_incident_id
from src.edu_cti_v2.phase1_dual_write import (
    build_source_incident_record,
    build_source_incident_urls,
    classify_source_group,
    parse_datetime_like,
)


def _incident(source: str, event_key: str) -> BaseIncident:
    return BaseIncident(
        incident_id=make_incident_id(source, event_key),
        source=source,
        source_event_id=event_key,
        institution_name="Penn State University",
        victim_raw_name="Penn State University",
        institution_type="University",
        country="United States",
        region="Pennsylvania",
        city="State College",
        incident_date="2026-05-09",
        date_precision="day",
        source_published_date="2026-05-09",
        ingested_at="2026-05-09T10:15:00Z",
        title="Penn State reports cyberattack",
        subtitle="Roundup coverage",
        primary_url=None,
        all_urls=[
            "https://example.com/story",
            "https://news.google.com/rss/articles/CBMiTmh0dHBzOi8vZXhhbXBsZS5jb20vZ3Jvb3Blcl9saW5r0gEA?oc=5",
        ],
        leak_site_url="https://leak.example/onion",
        source_detail_url="https://source.example/detail/123",
        screenshot_url="https://cdn.example/screenshot.png",
        attack_type_hint="ransomware",
        status="confirmed",
        source_confidence="high",
        notes="Records affected: 1234",
        threat_actor="SomeGroup",
    )


def test_classify_source_group_uses_registered_source_maps():
    assert classify_source_group("therecord") == "news"
    assert classify_source_group("ransomwarelive") == "api"
    assert classify_source_group("oxylabs_news") == "rss"


def test_parse_datetime_like_supports_date_and_zulu_timestamp():
    zulu = parse_datetime_like("2026-05-09T10:15:00Z")
    plain = parse_datetime_like("2026-05-09")

    assert zulu is not None
    assert zulu.tzinfo == timezone.utc
    assert zulu.hour == 10

    assert plain is not None
    assert plain.tzinfo == timezone.utc
    assert plain.hour == 0


def test_build_source_incident_record_maps_v1_fields_to_raw_observation():
    incident = _incident("googlenews_rss", "story-1")

    row = build_source_incident_record(incident, "story-1")

    assert row.source_name == "googlenews_rss"
    assert row.source_group == "rss"
    assert row.source_event_key == "story-1"
    assert row.raw_institution_name == "Penn State University"
    assert row.raw_attack_hint == "ransomware"
    assert row.raw_payload["v1_incident_id"] == incident.incident_id
    assert row.raw_payload["source_event_key"] == "story-1"


def test_build_source_incident_urls_classifies_article_wrapper_and_reference_urls():
    incident = _incident("googlenews_rss", "story-1")

    rows = build_source_incident_urls(incident)
    by_kind = {row.url_kind: row for row in rows}

    assert by_kind["article"].url == "https://example.com/story"
    assert by_kind["article"].is_primary_from_source is True
    assert by_kind["article"].resolved_url == "https://example.com/story"

    assert by_kind["rss_wrapper"].is_wrapper is True
    assert by_kind["rss_wrapper"].resolved_url is None

    assert by_kind["detail"].url == "https://source.example/detail/123"
    assert by_kind["leak_site"].url == "https://leak.example/onion"
    assert by_kind["screenshot"].url == "https://cdn.example/screenshot.png"
