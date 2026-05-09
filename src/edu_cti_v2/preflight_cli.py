"""CLI wrapper around the v2 preflight checks."""

from __future__ import annotations

import argparse
import json
from typing import Any, Callable, Optional

from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.services.preflight import V2PreflightService


def run_preflight(
    *,
    session_factory: Optional[Callable[[], Any]] = None,
    service: Optional[V2PreflightService] = None,
) -> dict[str, Any]:
    session_factory = session_factory or create_session_factory()
    service = service or V2PreflightService()

    with session_factory() as session:
        return service.get_status(session)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run EduThreat-CTI v2 preflight checks")
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit with code 1 when the stack is not ready",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    status = run_preflight()
    print(json.dumps(status, indent=2, sort_keys=True))
    if args.require_ready and not status.get("ready"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
