"""Unit tests for the Wazuh threat-feed exporter (parsing only, no network)."""

import importlib.util
from pathlib import Path

_MOD_PATH = Path(__file__).resolve().parents[2] / "wazuh" / "export_threat_feed.py"
_spec = importlib.util.spec_from_file_location("export_threat_feed", _MOD_PATH)
feed = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(feed)


def test_build_cdb_lists_extracts_iocs_and_lowercases_keys():
    incidents = [
        {"threat_actor": "Akira", "ransomware_family": "Akira", "country": "United States",
         "cves": "CVE-2023-1234|CVE-2019-19781"},
        {"threat_actor": "Vice Society", "ransomware_family": "", "country": "United Kingdom",
         "cves": ""},
    ]
    lists = feed.build_cdb_lists(incidents, cves=[])
    assert "akira" in lists["eduthreat_actors"]
    assert "vice society" in lists["eduthreat_actors"]
    assert "akira" in lists["eduthreat_ransomware_families"]
    assert "united states" in lists["eduthreat_victim_countries"]
    # CVEs uppercased + validated
    assert "CVE-2023-1234" in lists["eduthreat_cves"]
    assert "CVE-2019-19781" in lists["eduthreat_cves"]
    # CDB value carries a context tag
    assert lists["eduthreat_actors"]["akira"] == "edu_threat_actor"


def test_build_cdb_lists_rejects_malformed_cves_and_blanks():
    incidents = [{"threat_actor": "", "cves": "not-a-cve|CVE-bad", "country": ""}]
    lists = feed.build_cdb_lists(incidents, cves=[{"cve_id": "CVE-2025-61882"}])
    assert lists["eduthreat_cves"] == {"CVE-2025-61882": "edu_exploited_cve"}
    assert lists["eduthreat_actors"] == {}


def test_split_pipe_field():
    assert feed._split("A|B| C ") == ["A", "B", "C"]
    assert feed._split(None) == []
    assert feed._split("") == []


def test_build_stix_bundle_shapes_actors_and_cves():
    lists = {
        "eduthreat_actors": {"akira": "edu_threat_actor"},
        "eduthreat_cves": {"CVE-2019-19781": "edu_exploited_cve"},
        "eduthreat_ransomware_families": {},
        "eduthreat_victim_countries": {},
    }
    bundle = feed.build_stix_bundle(lists)
    assert bundle["type"] == "bundle"
    types = {o["type"] for o in bundle["objects"]}
    assert types == {"threat-actor", "indicator"}
    cve_obj = next(o for o in bundle["objects"] if o["type"] == "indicator")
    assert "CVE-2019-19781" in cve_obj["pattern"]
