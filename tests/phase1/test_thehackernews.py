from bs4 import BeautifulSoup

from src.edu_cti.sources.news import thehackernews
from src.edu_cti.sources.news.common import consume_news_query_metrics


class DummyClient:
    def __init__(self, html: str):
        self.html = html
        self.urls: list[str] = []

    def get_soup(self, url: str, **_kwargs):
        self.urls.append(url)
        return BeautifulSoup(self.html, "html.parser")


NATIVE_SEARCH_HTML = """
<html>
  <body>
    <a class="story-link" href="https://thehackernews.com/2022/10/why-ransomware-in-education-on-rise-and.html">
      <div class="home-right">
        <h2 class="home-title">Why Ransomware in Education on the Rise and What That Means for 2023</h2>
        <div class="item-label">
          <span class="h-datetime"> Oct 24, 2022</span>
          <span class="h-tags">Ransomware / Education</span>
        </div>
        <div class="home-desc">
          The breach of LA Unified School District highlights increasingly frequent
          ransomware attacks on education.
        </div>
      </div>
    </a>
    <a class="story-link" href="https://thehackernews.com/2026/01/security-training.html">
      <div class="home-right">
        <h2 class="home-title">Security Awareness Training Roundup</h2>
        <div class="item-label"><span class="h-datetime"> Jan 01, 2026</span></div>
        <div class="home-desc">A general training article without an education-sector incident.</div>
      </div>
    </a>
  </body>
</html>
"""


def test_thehackernews_native_search_url_uses_site_search():
    url = thehackernews._build_native_search_url("school data breach")

    assert url.startswith("https://thehackernews.com/search?")
    assert "q=school+data+breach" in url


def test_thehackernews_challenge_word_does_not_trigger_captcha_false_positive():
    html = """
    <html>
      <body>
        <a class="story-link" href="https://thehackernews.com/example">
          <h2 class="home-title">Cybersecurity Challenge Facing Schools</h2>
          <div class="home-desc">A real article card, not an anti-bot challenge.</div>
        </a>
      </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")

    assert thehackernews._detect_captcha(soup) is False
    assert len(thehackernews._extract_native_articles_from_page(soup)) == 1


def test_thehackernews_builds_incidents_from_native_search():
    consume_news_query_metrics()
    client = DummyClient(NATIVE_SEARCH_HTML)

    incidents = thehackernews.build_thehackernews_incidents(
        search_terms=["school data breach"],
        client=client,
    )

    assert len(incidents) == 1
    incident = incidents[0]
    assert incident.source == "thehackernews"
    assert incident.title == "Why Ransomware in Education on the Rise and What That Means for 2023"
    assert incident.incident_date == "2022-10-24"
    assert incident.date_precision == "day"
    assert incident.all_urls == [
        "https://thehackernews.com/2022/10/why-ransomware-in-education-on-rise-and.html"
    ]
    assert "search=native" in (incident.notes or "")
    assert "query_variant=exact_phrase" in (incident.notes or "")
    assert client.urls == [
        thehackernews._build_native_search_url('"school data breach"'),
        thehackernews._build_native_search_url("school data breach"),
    ]

    metrics = consume_news_query_metrics("thehackernews")
    assert [record["variant_type"] for record in metrics] == ["exact_phrase", "unquoted"]
