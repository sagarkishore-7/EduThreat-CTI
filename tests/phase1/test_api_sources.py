from src.edu_cti.sources.api import ransomware_live, ransomwatch


def test_ransomlook_historical_uses_archive_and_preserves_structured_fields(monkeypatch):
    calls = []

    def fake_fetch(url):
        calls.append(url)
        if url == ransomwatch.RANSOMLOOK_ARCHIVE_API_URL:
            return [
                {
                    "post_title": "Example University",
                    "group_name": "lockbit",
                    "discovered": "2024-01-02 03:04:05.000000",
                }
            ]
        return [
            {
                "post_title": "Example University",
                "group_name": "lockbit",
                "discovered": "2024-01-02 03:04:05.000000",
                "description": "Example University is a public university.",
                "link": "/page_company.php?id=42",
                "screen": "screenshots/lockbit/example.png",
                "post_url": "http://lockbit.example.onion/example-university",
                "website": "example.edu",
                "magnet": "magnet:?xt=urn:btih:abc",
            }
        ]

    monkeypatch.setattr(ransomwatch, "_fetch_records", fake_fetch)

    incidents = ransomwatch.build_ransomlook_incidents(incremental=False)

    assert calls == [
        ransomwatch.RANSOMLOOK_ARCHIVE_API_URL,
        ransomwatch.RANSOMLOOK_RECENT_API_URL,
    ]
    assert len(incidents) == 1
    incident = incidents[0]
    assert incident.institution_name == "Example University"
    assert incident.discovery_date == "2024-01-02"
    assert incident.all_urls == []
    assert incident.leak_site_url == "http://lockbit.example.onion/example-university"
    assert incident.source_detail_url == "https://www.ransomlook.io/page_company.php?id=42"
    assert incident.screenshot_url == "https://www.ransomlook.io/screenshots/lockbit/example.png"
    assert incident.threat_actor == "lockbit"
    assert "victim_website=example.edu" in incident.notes
    assert incident.raw_source_payload["screen"] == "screenshots/lockbit/example.png"


def test_ransomlook_incremental_does_not_hit_archive_when_recent_succeeds(monkeypatch):
    calls = []

    def fake_fetch(url):
        calls.append(url)
        assert url == ransomwatch.RANSOMLOOK_RECENT_API_URL
        return [
            {
                "post_title": "Recent School District",
                "group_name": "akira",
                "discovered": "2026-05-21 01:02:03.000000",
                "description": "Recent School District is a K-12 district.",
            }
        ]

    monkeypatch.setattr(ransomwatch, "_fetch_records", fake_fetch)

    incidents = ransomwatch.build_ransomlook_incidents(incremental=True)

    assert calls == [ransomwatch.RANSOMLOOK_RECENT_API_URL]
    assert len(incidents) == 1
    assert incidents[0].institution_name == "Recent School District"


def test_ransomwarelive_public_data_json_maps_education_rows(monkeypatch):
    monkeypatch.setattr(
        ransomware_live,
        "_get_public_data_victims",
        lambda: [
            {
                "post_title": "Not A School Ltd",
                "group_name": "akira",
                "activity": "Manufacturing",
                "published": "2026-05-18T00:00:00+00:00",
            },
            {
                "post_title": "Example College",
                "group_name": "nova",
                "discovered": "2026-05-20T18:33:59.896113+00:00",
                "published": "2026-05-19T00:00:00+00:00",
                "website": "example.edu",
                "country": "US",
                "activity": "Education",
                "description": "Example College is an educational institution.",
                "post_url": "http://nova.example.onion/example-college",
                "extrainfos": {"source": "dls"},
            },
        ],
    )

    incidents = ransomware_live.build_ransomwarelive_incidents()

    assert len(incidents) == 1
    incident = incidents[0]
    assert incident.source == "ransomwarelive"
    assert incident.institution_name == "Example College"
    assert incident.country == "US"
    assert incident.incident_date == "2026-05-19"
    assert incident.source_published_date == "2026-05-19"
    assert incident.discovery_date == "2026-05-20"
    assert incident.all_urls == []
    assert incident.leak_site_url == "http://nova.example.onion/example-college"
    assert incident.threat_actor == "nova"
    assert "victim_website=example.edu" in incident.notes
    assert "activity=Education" in incident.notes
    assert incident.raw_source_payload["extrainfos"] == {"source": "dls"}
