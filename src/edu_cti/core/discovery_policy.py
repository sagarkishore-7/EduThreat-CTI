"""Source discovery policy and lightweight collection metrics."""

from __future__ import annotations

from typing import Mapping

QUERY_SCOPED_HIGH_RECALL = "query_scoped_high_recall"
BROAD_CYBER_FILTERED = "broad_cyber_filtered"
CURATED_EDUCATION = "curated_education"
EDUCATION_API = "education_api"

BROAD_CYBER_SOURCE_DOMAINS: Mapping[str, str] = {
    "securityweek": "securityweek.com",
    "therecord": "therecord.media",
    "darkreading": "darkreading.com",
    "thehackernews": "thehackernews.com",
    "krebsonsecurity": "krebsonsecurity.com",
    "bleepingcomputer": "bleepingcomputer.com",
}

SOURCE_DISCOVERY_POLICIES: Mapping[str, str] = {
    "googlenews_rss": QUERY_SCOPED_HIGH_RECALL,
    "oxylabs_news": QUERY_SCOPED_HIGH_RECALL,
    **{source_name: BROAD_CYBER_FILTERED for source_name in BROAD_CYBER_SOURCE_DOMAINS},
    "cisa_rss": BROAD_CYBER_FILTERED,
    "international_rss": BROAD_CYBER_FILTERED,
    "databreaches_rss": CURATED_EDUCATION,
    "databreach": CURATED_EDUCATION,
    "konbriefing": CURATED_EDUCATION,
    "comparitech": CURATED_EDUCATION,
    "ransomlook": EDUCATION_API,
    "ransomwarelive": EDUCATION_API,
}

_DISCOVERY_METRICS: dict[str, dict[str, int]] = {}


def discovery_policy_for_source(source_name: str) -> str:
    return SOURCE_DISCOVERY_POLICIES.get(source_name, BROAD_CYBER_FILTERED)


def semantic_prefilter_allowed(source_name: str) -> bool:
    return discovery_policy_for_source(source_name) != QUERY_SCOPED_HIGH_RECALL


def record_source_discovery_metrics(source_name: str, metrics: Mapping[str, int]) -> None:
    _DISCOVERY_METRICS[source_name] = {str(key): int(value) for key, value in metrics.items()}


def consume_source_discovery_metrics(source_names: object = None) -> dict[str, dict[str, int]]:
    if source_names is None:
        names = list(_DISCOVERY_METRICS)
    else:
        names = [str(name) for name in source_names]
    return {name: _DISCOVERY_METRICS.pop(name) for name in names if name in _DISCOVERY_METRICS}
