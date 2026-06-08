"""Tests for accurate article/incident date extraction.

Guards the fix for date pollution where an unextracted publish date defaulted to
"today", which then became the LLM's relative-date anchor and the incident date.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import Mock

from src.edu_cti.core.date_parsing import (
    PRECISION_DAY,
    PRECISION_MONTH,
    PRECISION_YEAR,
    parse_date_strict,
)


# ── strict parser: never invents a year (kills the dateutil "-> today" bug) ──

def test_parse_full_dates():
    assert parse_date_strict("Thu, Oct 29 2020  06:48:26 PM") == (date(2020, 10, 29), PRECISION_DAY)
    assert parse_date_strict("29/10/2020") == (date(2020, 10, 29), PRECISION_DAY)
    assert parse_date_strict("October 29, 2020") == (date(2020, 10, 29), PRECISION_DAY)
    assert parse_date_strict("2019-05-12") == (date(2019, 5, 12), PRECISION_DAY)


def test_partial_dates_keep_their_precision():
    assert parse_date_strict("August 2020") == (date(2020, 8, 1), PRECISION_MONTH)
    assert parse_date_strict("Dec 2021") == (date(2021, 12, 1), PRECISION_MONTH)
    assert parse_date_strict("2020") == (date(2020, 1, 1), PRECISION_YEAR)


def test_yearless_and_relative_return_none_not_today():
    # The whole point: these must NOT become the current date.
    for junk in ("August", "Monday", "yesterday", "last Sunday", "06:48 PM", "", "n/a"):
        assert parse_date_strict(junk) == (None, None), junk


def test_absurd_years_rejected():
    assert parse_date_strict("1850-01-01") == (None, None)
    assert parse_date_strict(f"{date.today().year + 5}-01-01") == (None, None)


# ── multi-signal extractor on real-world-shaped HTML ─────────────────────────

def _fetcher():
    from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleFetcher

    return ArticleFetcher(http_client=Mock())


def _soup(html):
    from bs4 import BeautifulSoup

    return BeautifulSoup(html, "html.parser")


def test_extracts_og_updated_time_and_post_tags_calendar():
    """daijiworld-shaped page: date only in og:updated_time + a .post-tags
    calendar-icon span, no JSON-LD / <time>. Both were missed before."""
    html = """
    <html><head>
      <meta property="og:updated_time" content="29/10/2020 18:48:26">
    </head><body>
      <article><p>Some news content about a cyber incident.</p></article>
      <ul class="post-tags">
        <li><i class="fa fa-calendar"></i>&nbsp;
          <span id="">Thu, Oct 29 2020  06:48:26 PM</span></li>
      </ul>
    </body></html>
    """
    assert _fetcher()._extract_publish_date(_soup(html)) == "2020-10-29"


def test_rejects_current_date_furniture_without_corroboration():
    """A 'current date' widget equal to today, with no real published signal,
    must be discarded rather than reported as the publish date."""
    today = date.today().strftime("%B %d, %Y")
    html = f"""
    <html><body>
      <div class="site-clock">Today is {today}</div>
      <article><p>Article body with no real publication date anywhere.</p></article>
    </body></html>
    """
    assert _fetcher()._extract_publish_date(_soup(html)) is None


def test_url_path_date_used_as_signal():
    html = "<html><body><article><p>content</p></article></body></html>"
    got = _fetcher()._extract_publish_date(
        _soup(html), "https://example.com/news/2021/03/14/some-school-breach/"
    )
    assert got == "2021-03-14"


def test_json_ld_still_wins():
    html = """
    <html><head><script type="application/ld+json">
    {"@type":"NewsArticle","datePublished":"2018-09-14T00:09:33.000Z"}
    </script></head><body></body></html>
    """
    assert _fetcher()._extract_publish_date(_soup(html)) == "2018-09-14"


# ── incident_date guards: never future, never silently "today" ───────────────

def test_future_incident_date_dropped():
    from src.edu_cti_v2.services.canonicalization import _safe_projection_incident_date

    future = date.today() + timedelta(days=30)
    assert _safe_projection_incident_date(future, None, "news") is None
    past = date(2020, 10, 29)
    assert _safe_projection_incident_date(past, None, "news") == past


def test_none_incident_date_stays_none():
    from src.edu_cti_v2.services.canonicalization import _safe_projection_incident_date

    # No real date -> stays None; it is never replaced with a source/collection date.
    assert _safe_projection_incident_date(None, datetime.utcnow(), "rss") is None
