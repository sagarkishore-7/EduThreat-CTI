"""Regression tests: trust structured (curated/api) victims + discovery fetch cap.

Covers the Elmbrook class of false-negative — a curated/api incident whose victim
is named in the structured record but whose fetched supporting article is weak —
plus the word-order identity-validator fix and the discovery fetch-cap knob.
"""

from types import SimpleNamespace

import src.edu_cti_v2.services.enrichment as enr
import src.edu_cti_v2.services.fetching as fetch


def _si(*, group, inst, victim=None, title="x", subtitle=None):
    return SimpleNamespace(
        source_group=group,
        raw_institution_name=inst,
        raw_victim_name=victim if victim is not None else inst,
        raw_subtitle=subtitle,
        raw_title=title,
    )


# --------------------------------------------------------------------------- #
# Fix 1: word-order false positive in the collective-identity regex
# --------------------------------------------------------------------------- #
def test_named_district_typefirst_is_not_collective():
    rx = enr._COLLECTIVE_IDENTITY_RE
    # specific named district written type-first → NOT collective
    assert not rx.match("School District of Elmbrook")
    assert not rx.match("District of Columbia")
    # genuine collectives → still collective
    assert rx.match("school districts")
    assert rx.match("districts")
    assert rx.match("5 universities")
    # lowercase after "of" is not a proper noun → still collective
    assert rx.match("school districts of texas")


def test_invalid_primary_identity_accepts_named_district_typefirst():
    f = enr._looks_invalid_primary_identity
    assert f("School District of Elmbrook", title="x") is False
    assert f("School District of Elmbrook (2022)", title="x") is False
    assert f("school districts", title="x") is True


# --------------------------------------------------------------------------- #
# Fix 2: trust structured curated/api victim names
# --------------------------------------------------------------------------- #
def test_structured_trust_covers_curated_and_api_but_not_news():
    g = enr._structured_source_authoritative_identity
    assert g(_si(group="curated", inst="School District of Elmbrook")) == "School District of Elmbrook"
    assert g(_si(group="api", inst="Springfield Public Schools")) == "Springfield Public Schools"
    # genuinely non-specific structured names are still refused
    assert g(_si(group="curated", inst="several school districts")) is None
    assert g(_si(group="api", inst="multiple universities")) is None
    # news/rss carry no authoritative structured victim → never trusted here
    assert g(_si(group="news", inst="Springfield Public Schools")) is None
    assert g(_si(group="rss", inst="Some College")) is None
    assert g(_si(group="curated", inst=None)) is None


def _repair(si):
    return enr._repair_or_reject_primary_identity(
        si,
        raw_json_data={"institution_name": None, "is_edu_cyber_incident": True},
        typed_enrichment={"institution_name": None},
        article_title="Unrelated ransomware roundup",
        article_content="A roundup of ransomware attacks across many sectors this year.",
    )


def test_curated_incident_kept_with_clean_name_despite_weak_article():
    si = _si(
        group="curated",
        inst="School District of Elmbrook",
        title="Ransomware attack on School District of Elmbrook (2022)",
    )
    out, _typed, status = _repair(si)
    assert status == "ok"
    # kept with the clean structured name — no "(2022)" leaked from the headline
    assert out.get("institution_name") == "School District of Elmbrook"


def test_api_leaksite_incident_kept_with_structured_victim():
    si = _si(group="api", inst="Springfield Public Schools", title="Springfield Public Schools")
    out, _typed, status = _repair(si)
    assert status == "ok"
    assert out.get("institution_name") == "Springfield Public Schools"


def test_news_incident_with_no_victim_still_rejected():
    si = _si(group="news", inst=None, title="A generic security trends headline")
    _out, _typed, status = _repair(si)
    assert status == "reject"


# --------------------------------------------------------------------------- #
# Fix 3 (Path A): curated + weak/unrelated article -> KEEP on structured record
# (not parked in manual review), recovering real curated coverage.
# --------------------------------------------------------------------------- #
def test_structured_curated_in_scope_keeps_named_victim_on_weak_article():
    si = _si(
        group="curated",
        inst="University of Guelph",
        title="Cyber attack on a university in Canada",
    )
    raw = {"is_edu_cyber_incident": False, "institution_name": None}
    out_raw, out_typed = enr._mark_structured_curated_in_scope(si, raw, {"institution_name": None})
    # promoted back in-scope on the authoritative structured victim, flagged weak-article
    assert out_raw["is_edu_cyber_incident"] is True
    assert out_raw.get("_weak_article_support") is True
    assert out_raw["institution_name"] == "University of Guelph"
    assert out_typed["institution_name"] == "University of Guelph"


def test_structured_curated_should_review_predicate_targets_curated_edu_incidents():
    f = enr._structured_curated_source_should_review_non_edu_article
    # curated source naming an edu incident with attack language -> Path A applies
    assert f(_si(group="curated", inst="Knox College", title="Ransomware attack on a college in Illinois")) is True
    # news/rss never qualify (no authoritative structured victim)
    assert f(_si(group="news", inst="Knox College", title="Ransomware attack on Knox College")) is False


# --------------------------------------------------------------------------- #
# Discovery fetch cap
# --------------------------------------------------------------------------- #
def test_discovery_fetch_top_n_default_and_override(monkeypatch):
    monkeypatch.delenv("DISCOVERY_FETCH_TOP_N", raising=False)
    monkeypatch.delenv("EDU_CTI_DISCOVERY_FETCH_TOP_N", raising=False)
    assert fetch._discovery_fetch_top_n() == 3
    monkeypatch.setenv("DISCOVERY_FETCH_TOP_N", "2")
    assert fetch._discovery_fetch_top_n() == 2
    monkeypatch.setenv("DISCOVERY_FETCH_TOP_N", "0")  # 0 = no cap
    assert fetch._discovery_fetch_top_n() == 0
