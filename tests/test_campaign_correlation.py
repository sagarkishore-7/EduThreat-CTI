from src.edu_cti.analysis.campaign_correlation import (
    CampaignCandidate,
    CampaignMembership,
    CampaignProfile,
    _assign_families,
    _consensus_values,
    _dominant_year,
    _extract_cves,
    _normalize_cve,
    _trim_incoherent_members,
    build_campaign_outputs,
    build_candidate_edges,
    build_evidence_items,
    build_profiles,
)


def _profile(canonical_id: str, *, date: str | None = None, cves=()):
    profile = CampaignProfile(
        canonical_incident_id=canonical_id,
        canonical_status="open",
        victim_name="V",
        institution_type="university",
        country="United States",
        country_code="US",
        representative_date=date,
    )
    profile.cves.update(cves)
    return profile


def _candidate(campaign_id: str, *, member_count: int, actors=(), platforms=(), cves=(), vendors=(), first_seen="2025-01-01", confidence=0.7):
    return CampaignCandidate(
        campaign_id=campaign_id,
        campaign_name=campaign_id,
        campaign_type="actor_activity_wave",
        first_seen_date=first_seen,
        last_seen_date=first_seen,
        actors=list(actors),
        vendors=list(vendors),
        platforms=list(platforms),
        cves=list(cves),
        campaign_names=[],
        attack_categories=[],
        member_count=member_count,
        confirmed_member_count=member_count,
        evidence_only_member_count=0,
        confidence=confidence,
        analyst_summary="",
    )


def _membership(campaign_id: str, canonical_id: str):
    return CampaignMembership(
        campaign_id=campaign_id,
        canonical_incident_id=canonical_id,
        role="direct_victim",
        confidence=0.7,
        evidence_article_ids=[],
        evidence_source_incident_ids=[],
        evidence_quotes=[],
        review_status="candidate_unreviewed",
        victim_name="V",
        canonical_status="open",
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


def test_structured_vendor_name_seeds_platform_key_without_text_mention():
    # vendor_name carries "Instructure" but the article body never names the
    # vendor/platform — the structured field alone must still seed the indicator
    # so the third-party victim fans out to the shared-vendor campaign.
    rows = [
        _row(
            "v1",
            victim="Victim University",
            incident_date="2026-05-07",
            title="University reports data breach via software supplier",
            content="The university disclosed a data breach traced to a third-party software supplier.",
            attack_category="supply_chain_software",
            projection={},
        ),
    ]
    rows[0]["vendor_name"] = "Instructure"

    items = build_evidence_items(rows)
    assert items[0].platform_keys == ["instructure_canvas"]
    assert "Instructure" in items[0].vendors
    assert "Canvas" in items[0].platforms


def test_assign_families_groups_same_actor_year_and_marks_primary():
    big = _candidate("camp_actor_wave", member_count=5, actors=["Cl0p"], first_seen="2025-06-01")
    small = _candidate("camp_cve_exposure", member_count=2, cves=["CVE-2025-61882"], actors=["Cl0p"], first_seen="2025-06-10")
    memberships = [
        _membership("camp_actor_wave", "a1"),
        _membership("camp_actor_wave", "a2"),
        _membership("camp_cve_exposure", "b1"),
    ]

    _assign_families([big, small], memberships)

    assert big.family_id == small.family_id is not None
    assert big.is_primary_in_family is True
    assert small.is_primary_in_family is False
    assert small.campaign_id in big.related_campaign_ids
    assert big.campaign_id in small.related_campaign_ids


def test_assign_families_does_not_chain_via_shared_member():
    # Different anchors and different years that merely share a canonical incident
    # must NOT be grouped — member-overlap union-find transitively merges
    # unrelated campaigns into one blob, so we group by shared primary token only.
    left = _candidate("camp_left", member_count=3, platforms=["MOVEit"], first_seen="2023-06-01")
    right = _candidate("camp_right", member_count=4, actors=["Cl0p"], first_seen="2024-02-01")
    memberships = [
        _membership("camp_left", "x1"),
        _membership("camp_right", "x1"),
        _membership("camp_right", "x2"),
    ]

    _assign_families([left, right], memberships)

    assert left.family_id != right.family_id


def test_assign_families_groups_cve_and_actor_sharing_actor_token():
    # The CVE "exposure" view also carries the responsible actor as its top actor,
    # so it groups with the actor "wave" view of the same event (same year).
    wave = _candidate("camp_wave", member_count=5, actors=["Cl0p"], first_seen="2025-06-01")
    cve = _candidate(
        "camp_cve", member_count=3, actors=["Cl0p"], cves=["CVE-2025-61882"], first_seen="2025-06-10"
    )
    _assign_families([wave, cve], [_membership("camp_wave", "a1"), _membership("camp_cve", "b1")])
    assert wave.family_id == cve.family_id
    assert wave.is_primary_in_family is True


def test_assign_families_keeps_unrelated_campaigns_separate():
    a = _candidate("camp_a", member_count=3, actors=["LockBit"], first_seen="2023-01-01")
    b = _candidate("camp_b", member_count=3, actors=["Akira"], first_seen="2026-01-01")
    memberships = [_membership("camp_a", "m1"), _membership("camp_b", "m2")]

    _assign_families([a, b], memberships)

    assert a.family_id != b.family_id
    assert a.related_campaign_ids == []
    assert b.related_campaign_ids == []


def test_assign_families_distinct_actors_sharing_a_vendor_do_not_group():
    # The 2023 MOVEit bleed: distinct ransomware actors all carrying
    # vendor "Progress Software" must NOT be merged into one family — only a
    # shared top actor groups campaigns.
    clop = _candidate("camp_clop", member_count=14, actors=["Cl0p"], vendors=["Progress Software"], first_seen="2023-04-07")
    lockbit = _candidate("camp_lockbit", member_count=12, actors=["LockBit"], vendors=["Progress Software"], first_seen="2023-04-05")
    rhysida = _candidate("camp_rhysida", member_count=9, actors=["Rhysida"], vendors=["Progress Software"], first_seen="2023-06-10")
    _assign_families([clop, lockbit, rhysida], [])
    assert clop.family_id != lockbit.family_id
    assert clop.family_id != rhysida.family_id
    assert lockbit.family_id != rhysida.family_id


# ── A3: CVE hygiene ──────────────────────────────────────────────────────────

def test_normalize_cve_rejects_malformed_and_canonicalizes():
    assert _normalize_cve("cve-2023-34362") == "CVE-2023-34362"
    assert _normalize_cve("CVE-2025-61882") == "CVE-2025-61882"
    # 8-digit tail (beyond the 4-7 canonical range) is rejected.
    assert _normalize_cve("CVE-2025-61884212") is None
    assert _normalize_cve("CVE-25-6188") is None  # 2-digit year, wrong shape
    assert _normalize_cve("CVE-1998-0001") is None  # year out of range
    assert _normalize_cve("not a cve") is None
    assert _normalize_cve(None) is None


def test_extract_cves_rejects_out_of_range_tail():
    found = _extract_cves("Affected by CVE-2023-34362 and the 8-digit CVE-2025-61884212 token")
    assert found == ["CVE-2023-34362"]


def test_evidence_cve_consensus_keeps_multiply_reported_cve():
    # The MOVEit case: the real zero-day appears in several evidence items even
    # though few member projections carry it; off-event CVEs surface once each.
    from src.edu_cti.analysis.campaign_correlation import (
        _evidence_cve_consensus, CampaignEvidenceItem,
    )

    def _item(cid, cves):
        return CampaignEvidenceItem(
            evidence_item_id=f"e-{cid}-{','.join(cves)}", canonical_incident_id=cid,
            canonical_status="open", source_incident_id=None, article_document_id=None,
            victim_name="V", institution_type=None, country=None, country_code=None,
            incident_date="2023-06-01", publication_date="2023-06-01", source_name="s",
            source_group="rss", source_title=None, article_title=None, source_url=None,
            attack_category=None, attack_vector=None, threat_actor=None, ransomware_family=None,
            vendors=[], platforms=["MOVEit"], affected_systems=[], platform_keys=["moveit"],
            actors=[], cves=cves, campaign_names=[], mitre_tactics=[],
            records_affected_exact=None,
        )

    component = {"m1", "m2", "m3"}
    items = [
        _item("m1", ["CVE-2023-34362"]),
        _item("m2", ["CVE-2023-34362"]),         # 2nd evidence item -> consensus
        _item("m3", ["CVE-2024-5655"]),          # off-event, single -> dropped
        _item("m4", ["CVE-2023-34362"]),          # outside component -> ignored
    ]
    component_only = {"m1", "m2", "m3"}
    out = _evidence_cve_consensus(items, component_only, 2)
    assert out == ["CVE-2023-34362"]


def test_consensus_values_drops_single_member_cve():
    # The real-data "CVE-2025-618842" artifact is format-valid (6-digit tail)
    # but spurious; the >=2-member consensus rule is what removes such one-offs.
    profiles = [
        _profile("m1", cves=["CVE-2023-34362"]),
        _profile("m2", cves=["CVE-2023-34362"]),
        _profile("m3", cves=["CVE-2025-618842"]),  # one-off pollution, kept by regex
    ]
    assert _consensus_values(profiles, "cves", 2) == ["CVE-2023-34362"]


# ── A2: dominant-year naming ─────────────────────────────────────────────────

def test_dominant_year_is_mode_not_min():
    profiles = [
        _profile("m1", date="2022-03-28"),  # lone outlier
        _profile("m2", date="2023-05-31"),
        _profile("m3", date="2023-06-01"),
        _profile("m4", date="2023-05-30"),
    ]
    assert _dominant_year(profiles) == "2023"


# ── A1: cohesion trim ────────────────────────────────────────────────────────

def test_trim_incoherent_members_drops_off_window_outliers():
    profiles = {
        "m1": _profile("m1", date="2023-05-31"),
        "m2": _profile("m2", date="2023-06-01"),
        "m3": _profile("m3", date="2023-06-07"),
        "m4": _profile("m4", date="2025-09-01"),  # >400d off the 2023 core
        "m5": _profile("m5", date=None),          # undated → kept (cannot judge)
    }
    kept = _trim_incoherent_members(set(profiles), profiles, platform_keys=["moveit"])
    assert "m4" not in kept
    assert {"m1", "m2", "m3", "m5"} <= kept


def test_trim_keeps_all_when_too_few_dated():
    profiles = {
        "m1": _profile("m1", date="2023-05-31"),
        "m2": _profile("m2", date="2025-09-01"),
    }
    # Fewer than 3 dated members → no trim (not enough to define a core).
    assert _trim_incoherent_members(set(profiles), profiles, platform_keys=["moveit"]) == set(profiles)


# ── A4: family grouping by strong member overlap ─────────────────────────────

def test_assign_families_groups_fragments_by_member_overlap_without_shared_actor():
    # Two per-signal views of one MOVEit event (platform cluster + CVE cluster)
    # share most members but carry no single shared top actor. They must land in
    # ONE family via the overlap rule.
    platform_view = _candidate("camp_moveit_platform", member_count=6, platforms=["MOVEit"], first_seen="2023-05-31")
    cve_view = _candidate("camp_moveit_cve", member_count=6, cves=["CVE-2023-34362"], first_seen="2023-05-31")
    shared_ids = ["i1", "i2", "i3", "i4", "i5"]
    memberships = [_membership("camp_moveit_platform", cid) for cid in shared_ids + ["i6"]]
    memberships += [_membership("camp_moveit_cve", cid) for cid in shared_ids + ["i7"]]
    _assign_families([platform_view, cve_view], memberships)
    assert platform_view.family_id == cve_view.family_id
    assert platform_view.is_primary_in_family != cve_view.is_primary_in_family  # exactly one primary


def test_assign_families_low_overlap_stays_separate():
    a = _candidate("camp_a", member_count=5, first_seen="2023-01-01")
    b = _candidate("camp_b", member_count=5, first_seen="2023-01-01")
    # Only 1 shared incident out of 9 union → below threshold.
    memberships = [_membership("camp_a", c) for c in ["i1", "i2", "i3", "i4", "i5"]]
    memberships += [_membership("camp_b", c) for c in ["i5", "i6", "i7", "i8", "i9"]]
    _assign_families([a, b], memberships)
    assert a.family_id != b.family_id
