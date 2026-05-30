from src.edu_cti.sources.news import (
    darkreading,
    krebsonsecurity,
    securityweek,
    thehackernews,
    therecord,
)
from src.edu_cti.sources.news.common import (
    EXACT_PHRASE_VARIANT,
    UNQUOTED_VARIANT,
    SearchQueryMetrics,
    build_search_query_variants,
    can_exact_phrase_query,
    page_limit_for_query_variant,
    should_continue_to_next_query_variant,
)
from src.edu_cti.sources.news_query_audit_cli import run_news_query_audit


def test_broad_sources_generate_exact_phrase_then_unquoted_variants():
    for source in [
        "therecord",
        "securityweek",
        "darkreading",
        "krebsonsecurity",
        "thehackernews",
    ]:
        variants = build_search_query_variants(source, "education cyber attack")

        assert [variant.variant_type for variant in variants] == [
            EXACT_PHRASE_VARIANT,
            UNQUOTED_VARIANT,
        ]
        assert variants[0].search_query == '"education cyber attack"'
        assert variants[1].search_query == "education cyber attack"


def test_high_recall_sources_do_not_receive_exact_phrase_variants():
    for source in ["googlenews_rss", "oxylabs_news"]:
        variants = build_search_query_variants(source, "education cyber attack")

        assert [variant.variant_type for variant in variants] == [UNQUOTED_VARIANT]
        assert variants[0].search_query == "education cyber attack"


def test_exact_phrase_skips_operators_quotes_and_non_ascii_queries():
    assert not can_exact_phrase_query("site:therecord.media university ransomware")
    assert not can_exact_phrase_query('"university ransomware"')
    assert not can_exact_phrase_query("université ransomware")
    assert can_exact_phrase_query("university ransomware")


def test_search_url_builders_encode_quoted_variants():
    quoted = '"education cyber attack"'

    assert "term=%22education+cyber+attack%22" in therecord._search_url(quoted)
    assert "s=%22education+cyber+attack%22" in securityweek._search_url(quoted, 1)
    assert "q=%22education+cyber+attack%22" in darkreading._build_search_url(quoted, page=1)
    assert "s=%22education+cyber+attack%22" in krebsonsecurity._search_url(quoted, 1)
    assert "q=%22education+cyber+attack%22" in thehackernews._build_native_search_url(quoted)


def test_exact_phrase_always_continues_to_unquoted_baseline():
    exact = SearchQueryMetrics(
        source="krebsonsecurity",
        original_query="university ransomware",
        search_query='"university ransomware"',
        variant_type=EXACT_PHRASE_VARIANT,
    )
    unquoted = SearchQueryMetrics(
        source="krebsonsecurity",
        original_query="university ransomware",
        search_query="university ransomware",
        variant_type=UNQUOTED_VARIANT,
    )

    assert should_continue_to_next_query_variant(exact)
    assert not should_continue_to_next_query_variant(unquoted)


def test_exact_phrase_probe_has_small_page_cap_without_reducing_unquoted_baseline():
    assert page_limit_for_query_variant(EXACT_PHRASE_VARIANT, None) == 2
    assert page_limit_for_query_variant(EXACT_PHRASE_VARIANT, 50) == 2
    assert page_limit_for_query_variant(EXACT_PHRASE_VARIANT, 1) == 1
    assert page_limit_for_query_variant(UNQUOTED_VARIANT, None) is None
    assert page_limit_for_query_variant(UNQUOTED_VARIANT, 50) == 50



def test_news_query_audit_is_read_only_and_reports_metrics(monkeypatch):
    def fake_builder(*, search_terms, max_pages, save_callback):
        save_callback([])
        return []

    monkeypatch.setitem(run_news_query_audit.__globals__["SOURCE_BUILDERS"], "fake", fake_builder)

    payload = run_news_query_audit(sources=["fake"], terms=["school data breach"], max_pages=1)

    assert payload["writes_database"] is False
    assert payload["sources"][0]["source"] == "fake"
    assert payload["sources"][0]["incidents_returned"] == 0
