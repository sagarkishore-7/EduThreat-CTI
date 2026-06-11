"""The legacy keyword pre-filters must be bypassed when the LLM title gate is on."""

from src.edu_cti.sources.news.common import matches_keywords
from src.edu_cti.sources.rss.bleepingcomputer_rss import contains_education_keywords

_KEYWORDS = ["university", "college", "school"]


def test_matches_keywords_drops_junk_when_flag_off(monkeypatch):
    monkeypatch.delenv("TITLE_CLASSIFY_ENABLED", raising=False)
    # edu keyword but no cyber keyword → dropped by the AND-gate
    assert matches_keywords("University announces new admissions policy", _KEYWORDS) is False
    # cyber keyword but no education signal → dropped
    assert matches_keywords("New ransomware strain spreads worldwide", _KEYWORDS) is False
    # both edu + cyber present → kept
    assert matches_keywords("University suffers ransomware breach", _KEYWORDS) is True


def test_matches_keywords_admits_everything_when_flag_on(monkeypatch):
    monkeypatch.setenv("TITLE_CLASSIFY_ENABLED", "1")
    # the keyword gate is disabled — even low-signal titles are admitted so the
    # LLM (not a keyword AND) makes the relevance call.
    assert matches_keywords("University announces new admissions policy", _KEYWORDS) is True
    assert matches_keywords("New ransomware strain spreads worldwide", _KEYWORDS) is True
    # empty text is still nothing to classify
    assert matches_keywords("", _KEYWORDS) is False


def test_contains_education_keywords_bypassed_when_flag_on(monkeypatch):
    monkeypatch.delenv("TITLE_CLASSIFY_ENABLED", raising=False)
    assert contains_education_keywords("New ransomware strain spreads") is False

    monkeypatch.setenv("TITLE_CLASSIFY_ENABLED", "1")
    assert contains_education_keywords("New ransomware strain spreads") is True
    assert contains_education_keywords("") is False
