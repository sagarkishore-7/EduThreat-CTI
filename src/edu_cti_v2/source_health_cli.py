"""CLI for the read-only v2 source health audit."""

from __future__ import annotations

import argparse
import json
from typing import Any

from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.services.source_health import V2SourceHealthService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print a read-only v2 source health audit.")
    parser.add_argument("--sample-limit", type=int, default=25, help="Rows to include in samples.")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON.")
    return parser


def run_source_health_report(*, sample_limit: int = 25) -> dict[str, Any]:
    session_factory = create_session_factory()
    service = V2SourceHealthService()
    with session_factory() as session:
        return service.get_source_health(session, sample_limit=sample_limit)


def main() -> None:
    args = build_parser().parse_args()
    payload = run_source_health_report(sample_limit=args.sample_limit)
    if args.compact:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
