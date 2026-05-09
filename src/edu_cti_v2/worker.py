"""Worker loop and CLI for the Postgres-backed v2 runtime."""

from __future__ import annotations

import argparse
import logging
import os
import socket
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Callable, Optional

from sqlalchemy.orm import Session, sessionmaker

from src.edu_cti_v2.db import V2DatabaseSettings, create_session_factory
from src.edu_cti_v2.services.task_runtime import V2TaskRuntime

logger = logging.getLogger(__name__)


@dataclass
class V2WorkerRunSummary:
    processed_tasks: int
    idle_polls: int
    stop_reason: str
    worker_id: str
    task_type: str | None


def _default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def run_worker_loop(
    *,
    session_factory: Optional[Callable[[], AbstractContextManager[Session]]] = None,
    runtime: Optional[V2TaskRuntime] = None,
    worker_id: Optional[str] = None,
    task_type: Optional[str] = None,
    poll_interval: float = 5.0,
    max_tasks: Optional[int] = None,
    stop_when_idle: bool = False,
    lease_seconds: Optional[int] = None,
) -> V2WorkerRunSummary:
    """Run the v2 task worker loop once, until idle, or forever."""
    settings = V2DatabaseSettings.from_env()
    session_factory = session_factory or create_session_factory(settings)
    runtime = runtime or V2TaskRuntime()
    worker_id = worker_id or _default_worker_id()
    lease_seconds = lease_seconds or settings.task_lease_seconds

    processed_tasks = 0
    idle_polls = 0

    while True:
        with session_factory() as session:
            try:
                processed = runtime.process_next_task(
                    session,
                    worker_id=worker_id,
                    task_type=task_type,
                    lease_seconds=lease_seconds,
                )
                session.commit()
            except Exception:
                session.rollback()
                raise

        if processed is None:
            idle_polls += 1
            if stop_when_idle:
                return V2WorkerRunSummary(
                    processed_tasks=processed_tasks,
                    idle_polls=idle_polls,
                    stop_reason="idle",
                    worker_id=worker_id,
                    task_type=task_type,
                )
            time.sleep(max(poll_interval, 0.0))
            continue

        processed_tasks += 1
        if max_tasks is not None and processed_tasks >= max_tasks:
            return V2WorkerRunSummary(
                processed_tasks=processed_tasks,
                idle_polls=idle_polls,
                stop_reason="max_tasks",
                worker_id=worker_id,
                task_type=task_type,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the EduThreat-CTI v2 Postgres worker")
    parser.add_argument("--worker-id", type=str, default=None, help="Override worker identifier")
    parser.add_argument("--task-type", type=str, default=None, help="Lease only one task type")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Idle poll interval in seconds")
    parser.add_argument("--max-tasks", type=int, default=None, help="Stop after processing this many tasks")
    parser.add_argument("--lease-seconds", type=int, default=None, help="Task lease duration in seconds")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process at most one task and then stop",
    )
    parser.add_argument(
        "--drain",
        action="store_true",
        help="Process until the queue becomes idle, then stop",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()

    stop_when_idle = bool(args.once or args.drain)
    max_tasks = 1 if args.once else args.max_tasks

    logger.info(
        "Starting v2 worker: worker_id=%s task_type=%s once=%s drain=%s max_tasks=%s",
        args.worker_id or _default_worker_id(),
        args.task_type,
        args.once,
        args.drain,
        max_tasks,
    )
    summary = run_worker_loop(
        worker_id=args.worker_id,
        task_type=args.task_type,
        poll_interval=args.poll_interval,
        max_tasks=max_tasks,
        stop_when_idle=stop_when_idle,
        lease_seconds=args.lease_seconds,
    )
    logger.info(
        "v2 worker stopped: reason=%s processed_tasks=%s idle_polls=%s worker_id=%s task_type=%s",
        summary.stop_reason,
        summary.processed_tasks,
        summary.idle_polls,
        summary.worker_id,
        summary.task_type,
    )


if __name__ == "__main__":
    main()
