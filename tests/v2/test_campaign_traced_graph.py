"""Unit tests for the platform-rooted attack-chain campaign graph builder."""

from src.edu_cti_v2.services.campaigns import V2CampaignService


def _campaign(**overrides):
    base = {
        "campaign_id": "camp-1",
        "campaign_name": "MOVEit 2023 education impact",
        "member_count": 3,
        "confidence": 0.8,
        "status": "published",
        "actors": ["Cl0p", "criminal", "Russian cyber-extortion"],
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
    return V2CampaignService._build_chain_graph(
        campaign_id=campaign["campaign_id"],
        campaign=campaign,
        memberships=memberships,
        evidence_items=evidence,
        member_limit=250,
    )


def _nodes_by_type(graph, t):
    return [n for n in graph["nodes"] if n["type"] == t]


def _edge(graph, relation):
    return [e for e in graph["edges"] if e["relation"] == relation]


def test_chain_is_platform_rooted_no_campaign_or_institution_nodes():
    graph = _build(
        _campaign(),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"], cves=["CVE-2023-34362"], actors=["Cl0p"])],
    )
    assert graph["meta"]["layout"] == "chain"
    # platform is a layer-0 root; no campaign/institution nodes in the graph
    platforms = _nodes_by_type(graph, "platform")
    assert platforms and platforms[0]["layer"] == 0
    assert platforms[0]["id"] in graph["meta"]["roots"]
    assert not _nodes_by_type(graph, "campaign")
    assert not _nodes_by_type(graph, "institution")
    # platform carries its vendor as a sublabel rather than a separate node
    assert platforms[0]["metadata"].get("vendor") == "Progress Software"
    assert not _nodes_by_type(graph, "vendor")


def test_chain_edges_platform_cve_actor():
    graph = _build(
        _campaign(),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"], cves=["CVE-2023-34362"], actors=["Cl0p"])],
    )
    assert any(
        e["source"] == "platform:moveit" and e["target"] == "cve:cve-2023-34362"
        for e in _edge(graph, "has_vuln")
    )
    assert any(
        e["source"] == "cve:cve-2023-34362" and e["target"] == "actor:cl0p"
        for e in _edge(graph, "exploited_by")
    )
    # layers: platform=0, cve=1, actor=2
    layer = {n["id"]: n["layer"] for n in graph["nodes"]}
    assert layer["platform:moveit"] == 0
    assert layer["cve:cve-2023-34362"] == 1
    assert layer["actor:cl0p"] == 2


def test_generic_actors_dropped_from_chain():
    graph = _build(
        _campaign(),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"], actors=["Cl0p", "criminal"])],
    )
    actor_labels = {n["label"] for n in _nodes_by_type(graph, "actor")}
    assert actor_labels == {"Cl0p"}
    assert "criminal" not in actor_labels
    assert "Russian cyber-extortion" not in actor_labels


def test_victim_groups_grouped_by_asset():
    graph = _build(
        _campaign(member_count=2),
        [
            _membership("inc-1", victim="Alpha University"),
            _membership("inc-2", victim="Beta College"),
        ],
        [
            _evidence("inc-1", platforms=["MOVEit"], cves=["CVE-2023-34362"], actors=["Cl0p"]),
            _evidence("inc-2", platforms=["MOVEit"], actors=["Cl0p"]),
        ],
    )
    groups = graph["victim_groups"]
    assert len(groups) == 1
    g = groups[0]
    assert g["key"] == "platform:moveit" and g["via"] == "platform" and g["count"] == 2
    names = {i["victim_name"] for i in g["institutions"]}
    assert names == {"Alpha University", "Beta College"}


def test_targeted_by_when_no_cve():
    graph = _build(
        _campaign(cves=[]),
        [_membership("inc-1", victim="State University")],
        [_evidence("inc-1", platforms=["MOVEit"], actors=["Cl0p"])],
    )
    assert not _nodes_by_type(graph, "cve")
    assert any(
        e["source"] == "platform:moveit" and e["target"] == "actor:cl0p"
        for e in _edge(graph, "targeted_by")
    )
