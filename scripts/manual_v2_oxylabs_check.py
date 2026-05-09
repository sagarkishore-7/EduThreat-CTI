"""Manual QA helper for comparing live v2 incidents against Oxylabs-fetched articles."""

from __future__ import annotations

import argparse
import json
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from src.edu_cti.core.oxylabs import OxylabsClient


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().casefold()


def _derive_keyword(attack_category: str | None) -> str:
    category = (attack_category or "").strip().lower()
    if "ransomware" in category:
        return "ransomware"
    if "data_breach" in category or "breach" in category:
        return "data breach"
    if "unauthorized_access" in category:
        return "cyberattack"
    if not category:
        return "cyberattack"
    return category.replace("_", " ")


def _build_query(incident: dict[str, Any]) -> str:
    display_name = incident.get("display_name") or incident.get("institution_name") or incident.get("vendor_name") or ""
    keyword = _derive_keyword(incident.get("attack_category"))
    incident_date = incident.get("incident_date") or ""
    year = incident_date[:4] if incident_date else ""
    parts = [f'"{display_name}"', keyword]
    if year:
        parts.append(year)
    return " ".join(part for part in parts if part).strip()


def _extract_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _pick_snippet(text: str, display_name: str) -> str:
    normalized_display = _normalize(display_name)
    if normalized_display:
        lowered = _normalize(text)
        idx = lowered.find(normalized_display)
        if idx >= 0:
            start = max(idx - 140, 0)
            end = min(idx + 500, len(text))
            return text[start:end]
    return text[:640]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare live v2 incidents to Oxylabs-fetched article text")
    parser.add_argument("--api-base", default="https://v2-api-production-e3d1.up.railway.app")
    parser.add_argument("--limit", type=int, default=4)
    parser.add_argument("--search", default=None, help="Optional incident search filter")
    parser.add_argument("--results", type=int, default=3, help="Oxylabs news results per incident")
    args = parser.parse_args()

    api_url = f"{args.api_base.rstrip('/')}/api/v2/incidents"
    params = {"limit": args.limit}
    if args.search:
        params["search"] = args.search
    response = requests.get(api_url, params=params, timeout=60)
    response.raise_for_status()
    incidents = response.json().get("items", [])

    client = OxylabsClient()
    payload: list[dict[str, Any]] = []
    for incident in incidents:
        display_name = incident.get("display_name") or ""
        query = _build_query(incident)
        serp_results = client.search_news(query, max_results=args.results)
        top_result = serp_results[0] if serp_results else {}
        html = client.fetch_url(top_result.get("url", "")) if top_result.get("url") else None
        article_text = _extract_text(html) if html else ""
        detail = {
            "incident": {
                "canonical_incident_id": incident.get("canonical_incident_id"),
                "display_name": display_name,
                "country": incident.get("country"),
                "country_code": incident.get("country_code"),
                "incident_date": incident.get("incident_date"),
                "attack_category": incident.get("attack_category"),
                "summary": incident.get("canonical_summary"),
            },
            "oxylabs_query": query,
            "serp_count": len(serp_results),
            "top_result": top_result,
            "checks": {
                "institution_in_title": _normalize(display_name) in _normalize(top_result.get("title", "")),
                "institution_in_text": _normalize(display_name) in _normalize(article_text),
                "article_text_length": len(article_text),
            },
            "article_snippet": _pick_snippet(article_text, display_name),
        }
        payload.append(detail)

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
