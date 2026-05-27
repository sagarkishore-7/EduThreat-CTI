"""CLI entry point for running named v2 orchestration plans."""

from __future__ import annotations

import argparse
import json

from src.edu_cti_v2.services.orchestration import V2OrchestrationService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a named EduThreat-CTI v2 orchestration plan")
    parser.add_argument("plan_name", help="Named v2 plan to execute, for example historical")
    parser.add_argument("--worker-id", default="cli-v2-plan", help="Worker ID label for persisted run metadata")
    parser.add_argument(
        "--worker-max-tasks",
        type=int,
        default=None,
        help="Optional override for the maximum number of tasks to drain synchronously",
    )
    parser.add_argument(
        "--no-drain",
        action="store_true",
        help="Collect only; do not drain tasks synchronously in this command",
    )
    parser.add_argument(
        "--include-paid-rss",
        action="store_true",
        help="Force paid RSS/search coverage on regardless of env defaults",
    )
    parser.add_argument(
        "--exclude-paid-rss",
        action="store_true",
        help="Force paid RSS/search coverage off regardless of env defaults",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.include_paid_rss and args.exclude_paid_rss:
        parser.error("--include-paid-rss and --exclude-paid-rss are mutually exclusive")

    include_paid_rss = None
    if args.include_paid_rss:
        include_paid_rss = True
    elif args.exclude_paid_rss:
        include_paid_rss = False

    result = V2OrchestrationService().run_plan(
        plan_name=args.plan_name,
        worker_id=args.worker_id,
        worker_max_tasks=args.worker_max_tasks,
        drain_tasks=not args.no_drain,
        collect_overrides={"include_paid_rss": include_paid_rss} if include_paid_rss is not None else None,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
