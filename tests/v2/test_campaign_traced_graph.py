"""Unit tests for the actor-centred traced attack-chain campaign graph builder."""

from src.edu_cti_v2.services.campaigns import V2CampaignService


def _campaign(**overrides):
    base = {
        "campaign_id": "camp-1",
        "campaign_name": "Qilin 2023 Education Activity Wave",
        "member_count": 3,
        "confidence": 0.8,
        "status": "published",
        "actors": ["Qilin"],
        "vendors": ["Progress Software"],
        "platforms": ["MOVEit"],
        "cves": ["CVE-2023-34362"],
    }
    base.update(overrides)
    return base


def _membership(cid, *, victim, role="affected_via_vendor", confidence=0.7):
    return {
        "canonical_incident_id": cid,
        "victim_name": victim,
        "role": role,
        "confidence": confidence,
        "reasons": ["shared_vendor"],
        "review_status": "confirmed",
    }


def _evidence(cid, *, vendors=(), platforms=(), cves=(), actors=()):
    return {
        "canonical_incident_id": cid,
        "vendors": list(vendors),
        "platforms": list(platforms),
        "cves": list(cves),
        "actors": list(actors),
    }


def _build(campaign, memberships, evidence):
    return V2CampaignService._build_traced_graph(
        campaign_id=campaign["campaign_id"],
        campaign=campaign,
        memberships=memberships,
        evidence_items=evidence,
        member_limit=250,
    )


def _edge(graph, relation):
    return [e for e in graph["edges"] if e["relation"] == relation]


def test_actor_is_centre_and_attributes_campaign():
    graph = _build(
        _campaign(),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"], cves=["CVE-2023-34362"], actors=["Qilin"])],
    )
    assert graph["meta"]["layout"] == "traced"
    assert graph["meta"]["center_type"] == "actor"
    assert graph["meta"]["center_id"] == "actor:qilin"
    attributed = _edge(graph, "attributed_to")
    assert any(e["source"] == "actor:qilin" and e["target"] == "campaign:camp-1" for e in attributed)


def test_attack_chain_is_traced_actor_cve_platform_institution():
    graph = _build(
        _campaign(),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"], cves=["CVE-2023-34362"], actors=["Qilin"])],
    )
    # actor --used_cve--> CVE
    assert any(
        e["source"] == "actor:qilin" and e["target"] == "cve:cve-2023-34362"
        for e in _edge(graph, "used_cve")
    )
    # CVE --exploits--> platform (incident co-occurrence)
    assert any(
        e["source"] == "cve:cve-2023-34362" and e["target"] == "platform:moveit"
        for e in _edge(graph, "exploits")
    )
    # platform --affected--> institution (hung off the platform, not the centre)
    assert any(
        e["source"] == "platform:moveit" and e["target"] == "institution:inc-1"
        for e in _edge(graph, "affected")
    )
    # vendor --makes--> platform (registry)
    assert any(
        e["source"] == "vendor:progress software" and e["target"] == "platform:moveit"
        for e in _edge(graph, "makes")
    )


def test_unlinked_institution_falls_back_to_centre():
    # An incident with no campaign vendor/platform must still connect to the centre.
    graph = _build(
        _campaign(),
        [_membership("inc-2", victim="Direct Victim High School", role="direct_victim")],
        [_evidence("inc-2", actors=["Qilin"])],
    )
    direct = _edge(graph, "direct_victim")
    assert any(
        e["source"] == "actor:qilin" and e["target"] == "institution:inc-2" for e in direct
    )


def test_campaign_is_centre_when_no_actor():
    graph = _build(
        _campaign(actors=[]),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"])],
    )
    assert graph["meta"]["center_type"] == "campaign"
    assert graph["meta"]["center_id"] == "campaign:camp-1"
    # CVE with no incident co-occurrence still links from the campaign centre.
    assert any(
        e["source"] == "campaign:camp-1" and e["target"] == "cve:cve-2023-34362"
        for e in _edge(graph, "used_cve")
    )


def test_cve_node_type_is_cve_not_product():
    graph = _build(_campaign(), [], [])
    cve_nodes = [n for n in graph["nodes"] if n["type"] == "cve"]
    assert cve_nodes and all(n["label"].startswith("CVE-") for n in cve_nodes)
    assert not any(n["type"] == "cve_or_product" for n in graph["nodes"])
