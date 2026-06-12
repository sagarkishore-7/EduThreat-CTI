"""Tier-1 CI gate: validate the eval gold sets are well-formed (no LLM needed).

Catches a malformed/duplicate gold row before the (expensive) tier-2 LLM eval
ever runs, and guards against accidental edits to the labelled data.
"""

import json
from pathlib import Path

import pytest

_GOLD = Path(__file__).resolve().parents[2] / "evals" / "gold"
_EXPECTED_FIELDS = {"institution_name", "incident_date", "country", "attack_category", "threat_actor", "is_edu_cyber"}


def _load(name: str) -> list[dict]:
    rows = []
    with open(_GOLD / name, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))  # raises on malformed JSON
    return rows


def test_titles_gold_schema_and_unique_ids():
    rows = _load("titles.jsonl")
    assert len(rows) >= 12, "title gold set is suspiciously small"
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)), "duplicate ids in titles.jsonl"
    for r in rows:
        assert r["title"] and isinstance(r["title"], str)
        assert isinstance(r["label_relevant"], bool)
    # the set must contain both classes or precision/recall is meaningless
    labels = {r["label_relevant"] for r in rows}
    assert labels == {True, False}, "titles gold must contain both relevant and irrelevant"


def test_extractions_gold_schema_and_unique_ids():
    rows = _load("extractions.jsonl")
    assert len(rows) >= 4
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids)), "duplicate ids in extractions.jsonl"
    for r in rows:
        assert r.get("article_text"), f"{r['id']} missing article_text"
        exp = r["expected"]
        assert isinstance(exp, dict)
        unknown = set(exp) - _EXPECTED_FIELDS
        assert not unknown, f"{r['id']} has unexpected expected-fields: {unknown}"
        assert isinstance(exp["is_edu_cyber"], bool), f"{r['id']} is_edu_cyber must be bool"
    # must include at least one negative control (is_edu_cyber=false)
    assert any(r["expected"]["is_edu_cyber"] is False for r in rows), "need a non-education negative control"


@pytest.mark.parametrize("name", ["titles.jsonl", "extractions.jsonl"])
def test_gold_files_exist(name):
    assert (_GOLD / name).exists(), f"missing gold file {name}"
