import json

from src.edu_cti.core import utils


def _reset_keywords_cache() -> None:
    utils._EDU_KEYWORDS_CACHE = []
    utils._EDU_KEYWORDS_CACHE_KEY = None


def test_load_edu_keywords_uses_packaged_default():
    _reset_keywords_cache()

    keywords = utils.load_edu_keywords()

    assert "university" in keywords
    assert "universidad" in keywords


def test_load_edu_keywords_honors_explicit_override(tmp_path):
    override = tmp_path / "edu_keywords.json"
    override.write_text(
        json.dumps({"custom": ["Cyber Academy", "Learning Center"]}),
        encoding="utf-8",
    )
    _reset_keywords_cache()

    keywords = utils.load_edu_keywords(str(override))

    assert keywords == ["cyber academy", "learning center"]


def test_load_edu_keywords_honors_env_override(tmp_path, monkeypatch):
    override = tmp_path / "edu_keywords_env.json"
    override.write_text(
        json.dumps({"custom": ["District School"]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("EDU_CTI_KEYWORDS_PATH", str(override))
    _reset_keywords_cache()

    keywords = utils.load_edu_keywords()

    assert keywords == ["district school"]
