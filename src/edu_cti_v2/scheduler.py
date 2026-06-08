"""CLI entrypoint for the recurring v2 scheduler."""

from __future__ import annotations

import argparse
import logging
import signal
import time

from src.edu_cti_v2.services.scheduler import V2SchedulerService

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the EduThreat-CTI v2 recurring scheduler")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Scheduler poll interval in seconds",
    )
    return parser


def main() -> None:
    from src.edu_cti.core.logging_utils import setup_logging

    setup_logging()
    args = build_parser().parse_args()
    service = V2SchedulerService(poll_interval_seconds=args.poll_interval)

    stop = False

    def _handle_stop(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    service.start()
    logger.info("v2 scheduler running")
    try:
        while not stop:
            time.sleep(1.0)
    finally:
        service.stop()
        logger.info("v2 scheduler stopped")


if __name__ == "__main__":
    main()
