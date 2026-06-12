"""Unit tests for the LLM-eval scoring metrics (deterministic, no LLM)."""

from evals.metrics import (
    BinaryScores,
    date_match,
    exact_match,
    fuzzy_match,
    normalize_text,
    score_binary,
    score_fields,
)


def test_score_binary_precision_recall_f1():
    # 2 TP, 1 FP, 1 FN, 1 TN
    s = score_binary([(True, True), (True, True), (False, True), (True, False), (False, False)])
    d = s.as_dict()
    assert d["confusion"] == {"tp": 2, "fp": 1, "tn": 1, "fn": 1}
    assert d["precision_pct"] == 66.7  # 2/3
    assert d["recall_pct"] == 66.7  # 2/3
    assert d["f1_pct"] == 66.7
    assert d["accuracy_pct"] == 60.0  # 3/5


def test_score_binary_handles_empty_and_degenerate():
    assert score_binary([]).as_dict()["precision_pct"] is None
    only_neg = score_binary([(False, False), (False, False)])
    assert only_neg.as_dict()["precision_pct"] is None  # no predicted positives
    assert only_neg.accuracy == 1.0


def test_normalize_text_strips_accents_case_punct():
    assert normalize_text("Aix-Marseille Université") == "aix marseille universite"
    assert normalize_text("  THE   University, of  X. ") == "the university of x"


def test_fuzzy_match_word_order_and_boilerplate():
    assert fuzzy_match("University of California", "California University") is True
    assert fuzzy_match("School District of Elmbrook", "Elmbrook School District") is True
    assert fuzzy_match("Jefferson County Public Schools", "Jefferson County Schools") is True


def test_fuzzy_match_distinguishes_different_institutions():
    assert fuzzy_match("Stanford University", "Harvard University") is False
    assert fuzzy_match("University of Nottingham", "University of Manchester") is False


def test_fuzzy_match_none_handling():
    assert fuzzy_match(None, None) is True
    assert fuzzy_match("Stanford", None) is False
    assert fuzzy_match(None, "Stanford") is False


def test_exact_match_is_normalized():
    assert exact_match("Stanford University", "stanford  university") is True
    assert exact_match("Stanford", "Stanford University") is False


def test_date_match_precision_levels():
    assert date_match("2025-04-13", "2025-04-30", precision="month") is True
    assert date_match("2025-04-13", "2025-04-30", precision="day") is False
    assert date_match("2025-04-13", "2025-09-01", precision="year") is True
    assert date_match(None, None) is True
    assert date_match("2025-04-13", None) is False


def test_score_fields_mixed_exact_fuzzy_date():
    gold = [{"institution_name": "School District of Elmbrook", "incident_date": "2022-01-01", "country": "US"}]
    pred = [{"institution_name": "Elmbrook School District", "incident_date": "2022-01-15", "country": "US"}]
    rep = score_fields(gold, pred, ["institution_name", "incident_date", "country"], date_precision="month")
    assert rep["institution_name"].as_dict()["fuzzy_pct"] == 100.0
    assert rep["institution_name"].as_dict()["exact_pct"] == 0.0  # word order differs
    assert rep["incident_date"].as_dict()["fuzzy_pct"] == 100.0  # same month
    assert rep["country"].as_dict()["exact_pct"] == 100.0


def test_score_fields_penalizes_wrong_country():
    gold = [{"country": "US"}]
    pred = [{"country": "GB"}]  # wrong country
    rep = score_fields(gold, pred, ["country"])
    assert rep["country"].as_dict()["exact_pct"] == 0.0


def test_score_fields_skips_null_gold_fields():
    # A field whose gold is None is NOT scored as a field miss (it's the
    # faithfulness judge's job to catch a hallucinated value against null gold).
    gold = [{"threat_actor": None}]
    pred = [{"threat_actor": "LockBit"}]
    rep = score_fields(gold, pred, ["threat_actor"], skip_null_gold=True)
    assert rep["threat_actor"].as_dict()["support"] == 0
    assert rep["threat_actor"].as_dict()["fuzzy_pct"] is None
    # but with skip disabled it counts as a miss
    rep2 = score_fields(gold, pred, ["threat_actor"], skip_null_gold=False)
    assert rep2["threat_actor"].as_dict()["fuzzy_pct"] == 0.0


def test_score_fields_requires_aligned_lengths():
    import pytest

    with pytest.raises(ValueError):
        score_fields([{}], [{}, {}], ["country"])


def test_wilson_interval_small_n_is_wide():
    from evals.metrics import wilson_interval
    lo, hi = wilson_interval(8, 8)
    assert lo < 0.70 and hi == 1.0  # 8/8 -> roughly [0.68, 1.0], honestly wide
    lo2, hi2 = wilson_interval(135, 150)
    assert hi2 - lo2 < 0.12  # ~150 samples -> tight (±~5pp)
    assert wilson_interval(0, 0) is None


def test_binary_scores_expose_confidence_intervals():
    from evals.metrics import score_binary
    d = score_binary([(True, True)] * 8 + [(True, False)]).as_dict()
    assert isinstance(d["precision_ci95"], list) and len(d["precision_ci95"]) == 2
    assert d["recall_ci95"][0] < 60.0  # recall 8/9 lower bound is well below the point estimate
