"""
Compare single-call vs 3-call split extraction on real articles.

Usage:
  python tests/test_split_extraction.py [URL ...]

If no URLs are provided, uses a built-in set of education cyber-incident articles
that exercise the chronically-null fields: attack_chain, timeline.event_description,
mitre_attack_techniques (all 4 fields), and regulatory_impact.

Requires: OLLAMA_API_KEY env var.
Oxylabs credentials are hardcoded for the project (sagarkishore_x1cvp).
"""

import os
import sys
import json
import time
import requests
from typing import Optional
from dataclasses import dataclass

# --- resolve project root ---------------------------------------------------
import pathlib
ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent
from src.edu_cti.core.models import BaseIncident

# Articles we verified during monitoring — known to contain named ransomware,
# MITRE-mappable actions, regulatory implications, and multi-event timelines.
DEFAULT_TEST_URLS = [
    # San Felipe-Del Rio CISD ransomware — US school district, FERPA applicable
    "https://dysruptionhub.com/san-felipe-del-rio-cisd-network-outage-tx/",
    # French Catholic schools hack — GDPR applicable, large breach, named actor
    "https://www.europe-infos.fr/english/8309/massive-hack-hits-french-catholic-schools-network-exposing-",
    # Kanagawa University ransomware — Japanese institution, nikkei.com
    "https://xtech.nikkei.com/atcl/nxt/column/18/00598/042400011/",
]

OXYLABS_URL = "https://realtime.oxylabs.io/v1/queries"
OXYLABS_USER = "sagarkishore_x1cvp"
OXYLABS_PASS = "Qwerty748159263_"

# Fields to measure — the ones that are chronically null
TRACKED_FIELDS = [
    "attack_chain",
    "timeline",           # list — count non-null event_descriptions
    "mitre_attack_techniques",  # list — count fully-populated entries
    "applicable_regulations",
    "breach_notification_required",
    "regulators_notified",
    "ir_firm_engaged",
    "law_enforcement_involved",
    "recovery_method",
    "security_improvements",
    "key_quotes",
    "ransomware_family",
    "threat_actor_name",
    "institution_name",
    "attack_category",
    "attack_vector",
    "data_categories",
    "records_affected_exact",
    "incident_date",
]


def fetch_article_oxylabs(url: str) -> Optional[str]:
    """Fetch article HTML via Oxylabs universal scraper."""
    try:
        resp = requests.post(
            OXYLABS_URL,
            auth=(OXYLABS_USER, OXYLABS_PASS),
            json={"source": "universal", "url": url, "render": "html"},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["results"][0]["content"]
    except Exception as e:
        print(f"  Oxylabs fetch failed: {e}")
        return None


def html_to_text(html: str) -> str:
    """Rough HTML → plain text using BeautifulSoup if available, else strip tags."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)[:60_000]
    except ImportError:
        import re
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:60_000]


@dataclass
class FieldScore:
    populated: bool
    detail: str = ""


def score_field(key: str, value) -> FieldScore:
    """Return (populated, detail_note) for a given field value."""
    if value is None:
        return FieldScore(False, "null")
    if isinstance(value, list):
        if not value:
            return FieldScore(False, "[]")
        if key == "timeline":
            with_desc = [e for e in value if isinstance(e, dict) and e.get("event_description")]
            return FieldScore(
                bool(with_desc),
                f"{len(with_desc)}/{len(value)} entries have event_description",
            )
        if key == "mitre_attack_techniques":
            full = [
                t for t in value
                if isinstance(t, dict)
                and t.get("technique_id") and t.get("technique_name")
                and t.get("tactic") and t.get("description")
            ]
            return FieldScore(
                bool(full),
                f"{len(full)}/{len(value)} fully-populated (all 4 fields)",
            )
        return FieldScore(True, f"{len(value)} items")
    if isinstance(value, str) and value.strip():
        return FieldScore(True, value[:60])
    if isinstance(value, (int, float, bool)):
        return FieldScore(True, str(value))
    return FieldScore(False, repr(value)[:60])


def print_comparison(url: str, single_json: dict, split_json: dict):
    print(f"\n{'='*80}")
    print(f"URL: {url}")
    print(f"{'='*80}")
    print(f"{'FIELD':<35} {'SINGLE-CALL':<25} {'SPLIT (3-call)':<25} DIFF")
    print("-" * 100)

    improved = 0
    regressed = 0

    for key in TRACKED_FIELDS:
        sv = single_json.get(key)
        dv = split_json.get(key)
        ss = score_field(key, sv)
        ds = score_field(key, dv)

        diff = ""
        if ss.populated and not ds.populated:
            diff = "⬇ REGRESSED"
            regressed += 1
        elif not ss.populated and ds.populated:
            diff = "⬆ IMPROVED"
            improved += 1
        elif ss.populated and ds.populated and ss.detail != ds.detail:
            diff = "~ changed"

        print(
            f"{key:<35} "
            f"{'✓ ' + ss.detail if ss.populated else '✗ null':<25} "
            f"{'✓ ' + ds.detail if ds.populated else '✗ null':<25} "
            f"{diff}"
        )

    print("-" * 100)
    print(f"Summary: {improved} fields IMPROVED, {regressed} fields REGRESSED by split")

    # Count total nulls
    single_nulls = sum(1 for k in TRACKED_FIELDS if not score_field(k, single_json.get(k)).populated)
    split_nulls = sum(1 for k in TRACKED_FIELDS if not score_field(k, split_json.get(k)).populated)
    print(f"Null count: single={single_nulls}/{len(TRACKED_FIELDS)}, split={split_nulls}/{len(TRACKED_FIELDS)}")


def make_fake_incident(url: str) -> BaseIncident:
    return BaseIncident(
        incident_id=f"test_{hash(url) % 10000:04d}",
        source="test",
        source_event_id=None,
        institution_name="unknown",
        victim_raw_name=None,
        institution_type=None,
        country=None,
        region=None,
        city=None,
        incident_date=None,
        date_precision="unknown",
        source_published_date=None,
        ingested_at=None,
        title=None,
        subtitle=None,
        primary_url=url,
        all_urls=[url],
        notes="",
    )


def make_article_content(url: str, text: str, title: str = "") -> ArticleContent:
    return ArticleContent(
        url=url,
        title=title,
        content=text,
        author=None,
        publish_date=None,
        fetch_successful=True,
        error_message=None,
        content_length=len(text),
    )


def run_comparison(urls: list[str], enricher: IncidentEnricher):
    for url in urls:
        print(f"\nFetching: {url}")
        html = fetch_article_oxylabs(url)
        if not html:
            print("  SKIP — could not fetch article")
            continue

        text = html_to_text(html)
        print(f"  Article: {len(text):,} chars")
        if len(text) < 200:
            print("  SKIP — too short")
            continue

        # Extract title from first line
        title = text.split("\n")[0][:120] if text else ""

        incident = make_fake_incident(url)
        article_map = {url: make_article_content(url, text, title)}

        # --- single-call ---
        print("  Running single-call extraction...")
        t0 = time.time()
        _, single_json = enricher._enrich_article(incident, article_map)
        t_single = time.time() - t0
        if single_json is None:
            single_json = {}
        print(f"  Single-call: {t_single:.1f}s")

        # --- split (3-call) ---
        print("  Running split (3-call) extraction...")
        t1 = time.time()
        _, split_json = enricher._enrich_article_split(incident, article_map)
        t_split = time.time() - t1
        if split_json is None:
            split_json = {}
        print(f"  Split: {t_split:.1f}s")

        print_comparison(url, single_json, split_json)
        print(f"\nLatency: single={t_single:.1f}s vs split={t_split:.1f}s ({t_split/t_single:.1f}x)")


def main():
    api_key = os.environ.get("OLLAMA_API_KEY")
    if not api_key:
        print("ERROR: OLLAMA_API_KEY not set")
        sys.exit(1)

    urls = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_TEST_URLS

    print(f"Testing split extraction on {len(urls)} article(s)")
    print("Building LLM client...")
    llm_client = OllamaLLMClient(api_key=api_key)
    enricher = IncidentEnricher(llm_client=llm_client)

    run_comparison(urls, enricher)
    print("\nDone.")


if __name__ == "__main__":
    main()
