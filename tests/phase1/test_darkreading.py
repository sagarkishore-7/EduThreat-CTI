"""Unit tests for Dark Reading source fallbacks."""

from unittest.mock import Mock

from bs4 import BeautifulSoup

from src.edu_cti.sources.news import darkreading


def test_fetch_search_soup_prefers_oxylabs_when_available(monkeypatch):
    """Dark Reading search pages should use Oxylabs first to avoid Cloudflare blocks."""

    class _FakeOxylabs:
        def _is_configured(self):
            return True

        def fetch_url(self, url, render_js=True):
            assert render_js is True
            assert "darkreading.com/search" in url
            return """
                <html>
                  <body>
                    <div class="SearchResult-Content">
                      <div class="ContentPreview SearchResult-ContentPreview"></div>
                    </div>
                  </body>
                </html>
            """

    client = Mock()
    monkeypatch.setattr(darkreading, "OxylabsClient", _FakeOxylabs)

    soup = darkreading._fetch_search_soup(
        client,
        "https://www.darkreading.com/search?q=university+cyberattack",
    )

    assert soup.select_one("div.SearchResult-Content") is not None
    client.get_soup.assert_not_called()


def test_fetch_search_soup_falls_back_after_challenge(monkeypatch):
    """If Oxylabs returns a challenge page, the regular HTTP client still gets a chance."""

    class _FakeOxylabs:
        def _is_configured(self):
            return True

        def fetch_url(self, url, render_js=True):
            return """
                <html>
                  <head><title>Just a moment...</title></head>
                  <body>Enable JavaScript and cookies to continue</body>
                </html>
            """

    fallback_soup = BeautifulSoup(
        """
            <html>
              <body>
                <div class="SearchResult-Content">
                  <div class="ContentPreview SearchResult-ContentPreview"></div>
                </div>
              </body>
            </html>
        """,
        "html.parser",
    )
    client = Mock()
    client.get_soup.return_value = fallback_soup
    monkeypatch.setattr(darkreading, "OxylabsClient", _FakeOxylabs)

    soup = darkreading._fetch_search_soup(
        client,
        "https://www.darkreading.com/search?q=school+data+breach",
    )

    assert soup is fallback_soup
    client.get_soup.assert_called_once_with(
        "https://www.darkreading.com/search?q=school+data+breach",
        wait_selector="div.ContentPreview.SearchResult-ContentPreview",
    )
