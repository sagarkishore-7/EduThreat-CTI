from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Sequence
from typing import Any

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.sources.news import (
    build_darkreading_incidents,
    build_krebsonsecurity_incidents,
    build_securityweek_incidents,
    build_thehackernews_incidents,
    build_therecord_incidents,
)
from src.edu_cti.sources.news.common import consume_news_query_metrics

DEFAULT_AUDIT_TERMS = [
    "education cyber attack",
    "university ransomware",
    "school data breach",
    "student records breach",
]

SOURCE_BUILDERS: dict[str, Callable[..., list[BaseIncident]]] = {
    "therecord": build_therecord_incidents,
    "securityweek": build_securityweek_incidents,
    "darkreading": build_darkreading_incidents,
    "krebsonsecurity": build_krebsonsecurity_incidents,
    "thehackernews": build_thehackernews_incidents,
}


def _count_without_saving(batch: list[BaseIncident]) -> int:
    return len(batch)


def run_news_query_audit(
    *,
    sources: Sequence[str],
    terms: Sequence[str],
    max_pages: int,
) -> dict[str, Any]:
    """Run broad-source search collectors without writing to the database."""
    consume_news_query_metrics()
    source_results: list[dict[str, Any]] = []

    for source in sources:
        builder = SOURCE_BUILDERS.get(source)
        if builder is None:
            source_results.append(
                {
                    "source": source,
                    "error": f"unknown source; choose one of {sorted(SOURCE_BUILDERS)}",
                    "incidents_returned": 0,
                    "query_metrics": [],
                }
            )
            continue

        error = None
        incidents: list[BaseIncident] = []
        try:
            incidents = builder(
                search_terms=list(terms),
                max_pages=max_pages,
                save_callback=_count_without_saving,
            )
        except Exception as exc:  # pragma: no cover - CLI guardrail around live sites.
            error = f"{type(exc).__name__}: {exc}"

        source_results.append(
            {
                "source": source,
                "error": error,
                "incidents_returned": len(incidents),
                "query_metrics": consume_news_query_metrics(source),
            }
        )

    return {
        "sources": source_results,
        "terms": list(terms),
        "max_pages": max_pages,
        "writes_database": False,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run exact-phrase vs unquoted broad news source queries.",
    )
    parser.add_argument(
        "--source",
        dest="sources",
        action="append",
        choices=sorted(SOURCE_BUILDERS),
        help="Source to audit. Repeat to audit multiple sources. Defaults to all broad sources.",
    )
    parser.add_argument(
        "--term",
        dest="terms",
        action="append",
        help="Search term to audit. Repeat to audit multiple terms.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Maximum result pages to fetch per query variant.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output.",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Collector log verbosity.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    payload = run_news_query_audit(
        sources=args.sources or sorted(SOURCE_BUILDERS),
        terms=args.terms or DEFAULT_AUDIT_TERMS,
        max_pages=max(args.max_pages, 0),
    )
    print(json.dumps(payload, indent=2 if args.pretty else None, sort_keys=True))


if __name__ == "__main__":
    main()
