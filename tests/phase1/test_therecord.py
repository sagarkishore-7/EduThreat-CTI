"""Regression tests for The Record ingestion behavior."""

from unittest.mock import Mock

from bs4 import BeautifulSoup

from src.edu_cti.sources.news import common, therecord


def _make_result_soup(page_number: int, title: str) -> BeautifulSoup:
    return BeautifulSoup(
        f"""
            <html>
              <body>
                <li class="ais-Hits-item">
                  <a class="article-tile article-tile--primary" href="/story-{page_number}"></a>
                  <h2 class="article-tile__title">{title}</h2>
                  <span class="ais-Snippet">Student records exposed after unauthorized access.</span>
                  <span class="article-tile__meta__date">April 16, 2026</span>
                </li>
              </body>
            </html>
        """,
        "html.parser",
    )


def test_matches_keywords_handles_config_terms_with_punctuation():
    keywords = common.prepare_keywords()

    assert common.matches_keywords(
        "School-board systems hit by cyber-attack after unauthorized access",
        keywords,
    )


def test_build_therecord_incidents_stops_after_consecutive_stale_pages(monkeypatch):
    soups = [_make_result_soup(i, f"School board cyber-attack page {i}") for i in range(1, 6)]
    save_calls = []

    def _iter_pages(_client, _term, _max_pages):
        for page_number, soup in enumerate(soups, start=1):
            yield page_number, soup

    def _save_callback(incidents):
        save_calls.append([incident.incident_id for incident in incidents])
        return 0

    monkeypatch.setattr(therecord, "_iter_pages", _iter_pages)
    monkeypatch.setattr(therecord, "STALE_PAGE_STOP", 2)
    monkeypatch.setattr(therecord, "EMPTY_PAGE_STOP", 5)

    incidents = therecord.build_therecord_incidents(
        search_terms=["school data breach"],
        client=Mock(),
        save_callback=_save_callback,
    )

    assert len(incidents) == 2
    assert len(save_calls) == 2

