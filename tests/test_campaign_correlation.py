from src.edu_cti.analysis.campaign_correlation import (
    build_campaign_outputs,
    build_candidate_edges,
    build_evidence_items,
    build_profiles,
)


def _row(
    canonical_id: str,
    *,
    status: str = "open",
    victim: str = "Example University",
    incident_date: str = "2026-05-07",
    article_id: str | None = None,
    source_id: str | None = None,
    title: str = "Canvas outage affects university after Instructure breach",
    content: str = (
        "Instructure said Canvas was affected by a cybersecurity incident. "
        "ShinyHunters claimed responsibility and multiple schools lost access."
    ),
    attack_category: str = "third_party_compromise",
    manual_review_required: bool = False,
    projection: dict | None = None,
):
    if projection is None:
        projection = {
            "system_impact": {
                "third_party_vendor_impact": True,
                "vendor_name": "Instructure",
                "systems_affected": ["Canvas"],
            },
            "data_impact": {"data_categories": ["student_pii"]},
        }
    return {
        "canonical_incident_id": canonical_id,
        "canonical_status": status,
        "institution_name": victim,
        "institution_type": "university",
        "vendor_name": None,
        "country": "United States",
        "country_code": "US",
        "incident_date": incident_date,
        "source_published_at": incident_date,
        "attack_category": attack_category,
        "attack_vector": "third_party_vendor",
        "threat_actor_name": None,
        "ransomware_family": None,
        "source_incident_id": source_id or f"source-{canonical_id}",
        "source_name": "googlenews_rss",
        "source_group": "rss",
        "raw_title": title,
        "raw_notes": None,
        "manual_review_required": manual_review_required,
        "manual_review_reason": "review me" if manual_review_required else None,
        "article_document_id": article_id or f"article-{canonical_id}",
        "article_title": title,
        "article_publish_date": incident_date,
        "article_url": "https://example.test/article",
        "content_hash": f"hash-{canonical_id}",
        "content_text": content,
        "canonical_projection": projection,
    }


def test_canvas_rows_cluster_by_shared_vendor_platform_and_time_window():
    rows = [
        _row("c1", victim="University A", incident_date="2026-05-06"),
        _row("c2", victim="University B", incident_date="2026-05-09"),
        _row(
            "c3",
            victim="Unrelated School",
            incident_date="2026-05-07",
            title="School reports data breach",
            content="The school reported a data breach. No vendor, product, or actor was named.",
            attack_category="data_breach_external",
            projection={},
        ),
    ]

    items = build_evidence_items(rows)
    edges = build_candidate_edges(build_profiles(items))
    candidates, memberships = build_campaign_outputs(items, edges)

    assert len(candidates) == 1
    assert candidates[0].campaign_type == "shared_vendor_incident"
    assert candidates[0].platforms == ["Canvas"]
    assert candidates[0].vendors == ["Instructure"]
    assert candidates[0].confirmed_member_count == 2
    assert {membership.canonical_incident_id for membership in memberships} == {"c1", "c2"}
    assert {membership.role for membership in memberships} == {"affected_via_vendor"}


def test_excluded_rows_remain_evidence_only_not_confirmed_members():
    rows = [
        _row("c1", victim="University A", incident_date="2026-05-06"),
        _row("c2", victim="University B", incident_date="2026-05-09", status="excluded"),
    ]

    items = build_evidence_items(rows)
    edges = build_candidate_edges(build_profiles(items))
    candidates, memberships = build_campaign_outputs(items, edges)

    assert candidates[0].member_count == 2
    assert candidates[0].confirmed_member_count == 1
    evidence_only = [item for item in memberships if item.canonical_incident_id == "c2"]
    assert evidence_only[0].role == "needs_review"
    assert evidence_only[0].review_status == "excluded_evidence_only"


def test_manual_review_rows_are_not_counted_as_confirmed_members():
    rows = [
        _row("c1", victim="University A", incident_date="2026-05-06"),
        _row(
            "c2",
            victim="University B",
            incident_date="2026-05-09",
            manual_review_required=True,
        ),
    ]

    items = build_evidence_items(rows)
    edges = build_candidate_edges(build_profiles(items))
    candidates, memberships = build_campaign_outputs(items, edges)

    assert candidates[0].member_count == 2
    assert candidates[0].confirmed_member_count == 1
    review_membership = [item for item in memberships if item.canonical_incident_id == "c2"][0]
    assert review_membership.role == "needs_review"
    assert review_membership.review_status == "manual_review_required"


def test_shared_actor_without_close_time_or_shared_ttp_does_not_cluster():
    rows = [
        _row(
            "c1",
            victim="University A",
            incident_date="2022-01-01",
            title="Akira ransomware hits university",
            content="Akira claimed a ransomware attack against the university.",
            attack_category="ransomware_double_extortion",
            projection={},
        ),
        _row(
            "c2",
            victim="University B",
            incident_date="2026-01-01",
            title="Akira ransomware hits another school",
            content="Akira claimed a ransomware attack against another school.",
            attack_category="ransomware_double_extortion",
            projection={},
        ),
    ]

    items = build_evidence_items(rows)
    edges = build_candidate_edges(build_profiles(items))
    candidates, memberships = build_campaign_outputs(items, edges)

    assert edges == []
    assert candidates == []
    assert memberships == []


def test_stale_source_titles_do_not_create_platform_cluster_without_article_support():
    rows = [
        _row(
            "c1",
            victim="University A",
            incident_date="2026-05-06",
            title="Canvas breach affects universities",
            content="The selected article discusses a generic data breach at University A.",
            attack_category="data_breach_external",
            projection={},
        ),
        _row(
            "c2",
            victim="University B",
            incident_date="2026-05-07",
            title="Canvas breach affects universities",
            content="The selected article discusses a generic data breach at University B.",
            attack_category="data_breach_external",
            projection={},
        ),
    ]
    for row in rows:
        row["article_title"] = "University reports data breach"

    items = build_evidence_items(rows)
    edges = build_candidate_edges(build_profiles(items))

    assert edges == []
