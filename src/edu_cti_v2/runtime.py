"""Unified long-running v2 worker runtime for split-service deployment."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
from dataclasses import asdict, dataclass
from typing import Optional

from src.edu_cti_v2.services.scheduler import V2SchedulerService
from src.edu_cti_v2.worker import V2WorkerRunSummary, run_worker_loop

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class _WorkerThreadState:
    worker_id: str
    thread: Optional[threading.Thread]
    summary: Optional[V2WorkerRunSummary] = None
    error: Optional[str] = None


class V2RuntimeService:
    """Run recurring scheduling and task workers inside one long-lived service."""

    def __init__(
        self,
        *,
        worker_count: int = 2,
        task_type: Optional[str] = None,
        poll_interval_seconds: float = 5.0,
        lease_seconds: Optional[int] = None,
        enable_scheduler: bool = True,
        scheduler_service: Optional[V2SchedulerService] = None,
        scheduler_poll_interval_seconds: float = 5.0,
    ) -> None:
        self.worker_count = max(worker_count, 1)
        self.task_type = task_type
        self.poll_interval_seconds = poll_interval_seconds
        self.lease_seconds = lease_seconds
        self.enable_scheduler = enable_scheduler
        self.scheduler_service = scheduler_service or V2SchedulerService(
            poll_interval_seconds=scheduler_poll_interval_seconds,
        )

        self._stop_event = threading.Event()
        self._worker_states: list[_WorkerThreadState] = []
        self._running = False

    def _worker_target(self, worker_id: str, state: _WorkerThreadState) -> None:
        try:
            state.summary = run_worker_loop(
                worker_id=worker_id,
                task_type=self.task_type,
                poll_interval=self.poll_interval_seconds,
                lease_seconds=self.lease_seconds,
                stop_event=self._stop_event,
            )
            logger.info(
                "v2 runtime worker stopped: worker_id=%s reason=%s processed=%s idle_polls=%s",
                worker_id,
                state.summary.stop_reason,
                state.summary.processed_tasks,
                state.summary.idle_polls,
            )
        except Exception as exc:
            state.error = str(exc)
            logger.exception("v2 runtime worker crashed: worker_id=%s", worker_id)

    def start(self) -> dict[str, object]:
        if self._running:
            return self.get_status()

        self._stop_event.clear()
        self._worker_states = []

        if self.enable_scheduler:
            self.scheduler_service.start()

        for index in range(self.worker_count):
            worker_id = f"v2-runtime:{index + 1}"
            state = _WorkerThreadState(worker_id=worker_id, thread=None)
            thread = threading.Thread(
                target=self._worker_target,
                args=(worker_id, state),
                name=f"v2-worker-{index + 1}",
                daemon=True,
            )
            state.thread = thread
            self._worker_states.append(state)
            thread.start()

        self._running = True
        logger.info(
            "Started v2 runtime: workers=%s task_type=%s scheduler=%s",
            self.worker_count,
            self.task_type,
            self.enable_scheduler,
        )
        return self.get_status()

    def stop(self) -> dict[str, object]:
        self._stop_event.set()

        if self.enable_scheduler:
            self.scheduler_service.stop()

        for state in self._worker_states:
            if state.thread and state.thread.is_alive():
                state.thread.join(timeout=max(self.poll_interval_seconds * 2, 0.1))

        self._running = False
        logger.info("Stopped v2 runtime")
        return self.get_status()

    def get_status(self) -> dict[str, object]:
        return {
            "running": self._running,
            "worker_count": self.worker_count,
            "task_type": self.task_type,
            "scheduler_enabled": self.enable_scheduler,
            "scheduler": self.scheduler_service.get_status() if self.enable_scheduler else None,
            "workers": [
                {
                    "worker_id": state.worker_id,
                    "alive": state.thread.is_alive() if state.thread else False,
                    "summary": asdict(state.summary) if state.summary else None,
                    "error": state.error,
                }
                for state in self._worker_states
            ],
        }


def build_parser() -> argparse.ArgumentParser:
    default_enable_scheduler = _env_flag("EDU_CTI_V2_ENABLE_SCHEDULER", "1")
    parser = argparse.ArgumentParser(description="Run the EduThreat-CTI v2 unified worker runtime")
    parser.add_argument(
        "--workers",
        type=int,
        default=int(os.environ.get("EDU_CTI_V2_WORKER_COUNT", "2")),
        help="Number of long-running worker threads",
    )
    parser.add_argument("--task-type", type=str, default=None, help="Restrict all workers to one task type")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Idle task poll interval in seconds",
    )
    parser.add_argument(
        "--lease-seconds",
        type=int,
        default=None,
        help="Task lease duration in seconds",
    )
    parser.add_argument(
        "--scheduler-poll-interval",
        type=float,
        default=5.0,
        help="Recurring scheduler poll interval in seconds",
    )
    parser.add_argument(
        "--no-scheduler",
        action="store_true",
        help="Disable recurring plan scheduling and run workers only",
    )
    parser.set_defaults(enable_scheduler=default_enable_scheduler)
    return parser


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    args = build_parser().parse_args()
    runtime = V2RuntimeService(
        worker_count=args.workers,
        task_type=args.task_type,
        poll_interval_seconds=args.poll_interval,
        lease_seconds=args.lease_seconds,
        enable_scheduler=args.enable_scheduler and not args.no_scheduler,
        scheduler_poll_interval_seconds=args.scheduler_poll_interval,
    )

    stop = False

    def _handle_stop(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    runtime.start()
    try:
        while not stop:
            time.sleep(1.0)
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
