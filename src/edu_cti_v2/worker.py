"""Worker loop and CLI for the Postgres-backed v2 runtime."""

from __future__ import annotations

import argparse
import ctypes
import ctypes.util
import gc
import logging
import os
import socket
import sys
import threading
import time
from contextlib import AbstractContextManager
from dataclasses import dataclass
from threading import Event
from typing import Callable, Optional, Sequence

from sqlalchemy.orm import Session, sessionmaker

from src.edu_cti_v2.db import V2DatabaseSettings, create_session_factory
from src.edu_cti_v2.repositories import PipelineTaskRepository
from src.edu_cti_v2.resource_limits import current_rss_mb, memory_high_water_mb
from src.edu_cti_v2.services.task_runtime import V2TaskRuntime

logger = logging.getLogger(__name__)

# Process-wide memory backstop: when resident memory crosses the high-water
# mark, worker threads pause leasing new tasks until it recedes, so a burst of
# large articles can't OOM the container mid-drain. Shared across all threads
# (RSS is process-global). Logged at most once per cooldown to avoid spam.
_MEMORY_GUARD_COOLDOWN_S = 30.0
_last_memory_guard_log = 0.0

# glibc malloc handle for returning freed heap arenas to the OS. Loaded once.
try:
    _LIBC = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
    if not hasattr(_LIBC, "malloc_trim"):
        _LIBC = None
except Exception:  # pragma: no cover - non-glibc platforms
    _LIBC = None


def _reclaim_memory() -> None:
    """Return freed heap to the OS to counter RSS ratcheting from ML inference.

    The NER/RAG pre-pass (torch / sentence-transformers / GLiNER) and large-article
    buffers fragment the heap. ``gc.collect()`` frees Python cycles but glibc keeps
    the freed arenas, so process RSS climbs across tasks until the container is
    OOM-killed even at a safe worker count. ``malloc_trim(0)`` releases those
    arenas back to the OS; the torch allocator cache is cleared if torch is loaded.
    Called after each task and when the memory guard trips so RSS actually recedes.
    """
    gc.collect()
    if _LIBC is not None:
        try:
            _LIBC.malloc_trim(0)
        except Exception:
            pass
    torch = sys.modules.get("torch")
    if torch is not None:
        try:
            if torch.cuda.is_available():  # type: ignore[attr-defined]
                torch.cuda.empty_cache()  # type: ignore[attr-defined]
        except Exception:
            pass


def _memory_guard_should_pause(high_water_mb: Optional[float]) -> Optional[float]:
    """Return current RSS (MB) if it exceeds the high-water mark, else None."""
    if not high_water_mb:
        return None
    rss = current_rss_mb()
    if rss is not None and rss >= high_water_mb:
        return rss
    return None


@dataclass
class V2WorkerRunSummary:
    processed_tasks: int
    idle_polls: int
    stop_reason: str
    worker_id: str
    task_type: str | None


def _default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _lease_heartbeat_interval(lease_seconds: int) -> float:
    return max(min(float(lease_seconds) / 3.0, 30.0), 1.0)


def _lease_heartbeat_loop(
    *,
    session_factory: Callable[[], AbstractContextManager[Session]],
    task_id,
    worker_id: str,
    lease_seconds: int,
    stop_event: Event,
) -> None:
    task_repository = PipelineTaskRepository()
    interval = _lease_heartbeat_interval(lease_seconds)

    while not stop_event.wait(interval):
        with session_factory() as session:
            try:
                renewed = task_repository.renew_lease(
                    session,
                    task_id=task_id,
                    worker_id=worker_id,
                    lease_seconds=lease_seconds,
                )
                session.commit()
            except Exception as exc:
                session.rollback()
                logger.warning(
                    "v2 worker lease heartbeat failed: worker_id=%s task_id=%s error=%s",
                    worker_id,
                    task_id,
                    exc,
                )
                continue
        if not renewed:
            return


class _ReusableLeaseHeartbeat:
    """Renew one leased task at a time without spawning a new thread per task."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], AbstractContextManager[Session]],
        worker_id: str,
        lease_seconds: int,
    ) -> None:
        self._session_factory = session_factory
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._interval = _lease_heartbeat_interval(lease_seconds)
        self._condition = threading.Condition()
        self._stop_requested = False
        self._task_id = None
        self._thread: Optional[threading.Thread] = None
        self._start_error: Optional[Exception] = None
        self._start_thread()

    def _start_thread(self) -> None:
        thread = threading.Thread(
            target=self._run,
            name=f"lease-heartbeat-{self._worker_id.replace(':', '-')}",
            daemon=True,
        )
        try:
            thread.start()
        except Exception as exc:
            self._start_error = exc
            logger.warning(
                "v2 worker lease heartbeat disabled: worker_id=%s error=%s",
                self._worker_id,
                exc,
            )
            return
        self._thread = thread

    def activate(self, task_id) -> None:
        if self._thread is None:
            return
        with self._condition:
            self._task_id = task_id
            self._condition.notify_all()

    def clear(self, task_id=None) -> None:
        if self._thread is None:
            return
        with self._condition:
            if task_id is None or self._task_id == task_id:
                self._task_id = None
            self._condition.notify_all()

    def stop(self) -> None:
        if self._thread is None:
            return
        with self._condition:
            self._stop_requested = True
            self._task_id = None
            self._condition.notify_all()
        self._thread.join(timeout=max(min(self._interval, 1.0), 0.1))

    def _run(self) -> None:
        task_repository = PipelineTaskRepository()
        while True:
            with self._condition:
                while not self._stop_requested and self._task_id is None:
                    self._condition.wait()
                if self._stop_requested:
                    return
                task_id = self._task_id
                self._condition.wait(timeout=self._interval)
                if self._stop_requested:
                    return
                if task_id is None or self._task_id != task_id:
                    continue

            with self._session_factory() as session:
                try:
                    renewed = task_repository.renew_lease(
                        session,
                        task_id=task_id,
                        worker_id=self._worker_id,
                        lease_seconds=self._lease_seconds,
                    )
                    session.commit()
                except Exception as exc:
                    session.rollback()
                    logger.warning(
                        "v2 worker lease heartbeat failed: worker_id=%s task_id=%s error=%s",
                        self._worker_id,
                        task_id,
                        exc,
                    )
                    continue

            if not renewed:
                # Lease was lost without raising (expired or reclaimed by another
                # worker). Log it so expired-lease churn is debuggable rather than
                # silently dropping the heartbeat for this task.
                logger.warning(
                    "v2 worker lease heartbeat could not renew (lease lost): worker_id=%s task_id=%s",
                    self._worker_id,
                    task_id,
                )
                self.clear(task_id)


def run_worker_loop(
    *,
    session_factory: Optional[Callable[[], AbstractContextManager[Session]]] = None,
    runtime: Optional[V2TaskRuntime] = None,
    worker_id: Optional[str] = None,
    task_type: Optional[str] = None,
    exclude_task_types: Optional[Sequence[str]] = None,
    poll_interval: float = 5.0,
    max_tasks: Optional[int] = None,
    stop_when_idle: bool = False,
    lease_seconds: Optional[int] = None,
    stop_event: Optional[Event] = None,
) -> V2WorkerRunSummary:
    """Run the v2 task worker loop once, until idle, or forever."""
    settings = V2DatabaseSettings.from_env()
    session_factory = session_factory or create_session_factory(settings)
    runtime = runtime or V2TaskRuntime()
    worker_id = worker_id or _default_worker_id()
    lease_seconds = lease_seconds or settings.task_lease_seconds

    processed_tasks = 0
    idle_polls = 0
    high_water_mb = memory_high_water_mb()
    heartbeat = _ReusableLeaseHeartbeat(
        session_factory=session_factory,
        worker_id=worker_id,
        lease_seconds=lease_seconds,
    )

    try:
        while True:
            if stop_event and stop_event.is_set():
                return V2WorkerRunSummary(
                    processed_tasks=processed_tasks,
                    idle_polls=idle_polls,
                    stop_reason="stopped",
                    worker_id=worker_id,
                    task_type=task_type,
                )

            # Memory backstop: pause leasing new work while RSS is above the
            # high-water mark so a spike can't OOM the container.
            rss_over = _memory_guard_should_pause(high_water_mb)
            if rss_over is not None:
                global _last_memory_guard_log
                now = time.monotonic()
                if now - _last_memory_guard_log >= _MEMORY_GUARD_COOLDOWN_S:
                    logger.warning(
                        "v2 worker memory guard: RSS %.0fMB ≥ high-water %.0fMB — "
                        "pausing new task leases until memory recedes (worker_id=%s)",
                        rss_over,
                        high_water_mb,
                        worker_id,
                    )
                    _last_memory_guard_log = now
                # Actively return freed heap to the OS so RSS recedes instead of
                # merely waiting (the ratchet does not recede on its own).
                _reclaim_memory()
                if stop_event:
                    stop_event.wait(max(poll_interval, 1.0))
                else:
                    time.sleep(max(poll_interval, 1.0))
                continue

            with session_factory() as session:
                try:
                    leased_task_id = runtime.lease_next_task(
                        session,
                        worker_id=worker_id,
                        task_type=task_type,
                        lease_seconds=lease_seconds,
                        exclude_task_types=exclude_task_types,
                    )
                    session.commit()
                except Exception:
                    session.rollback()
                    raise

            if leased_task_id is None:
                idle_polls += 1
                if stop_when_idle:
                    return V2WorkerRunSummary(
                        processed_tasks=processed_tasks,
                        idle_polls=idle_polls,
                        stop_reason="idle",
                        worker_id=worker_id,
                        task_type=task_type,
                    )
                if stop_event:
                    stop_event.wait(max(poll_interval, 0.0))
                else:
                    time.sleep(max(poll_interval, 0.0))
                continue

            heartbeat.activate(leased_task_id)
            with session_factory() as session:
                try:
                    processed = runtime.process_leased_task(
                        session,
                        task_id=leased_task_id,
                        worker_id=worker_id,
                    )
                    session.commit()
                except Exception:
                    session.rollback()
                    raise
                finally:
                    heartbeat.clear(leased_task_id)

            # Reclaim after every task: enrichment runs take tens of seconds, so a
            # few-ms gc + malloc_trim is negligible and keeps RSS flat across the
            # drain instead of ratcheting up to an OOM kill.
            _reclaim_memory()

            if processed is None:
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
    finally:
        heartbeat.stop()


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
    from src.edu_cti.core.logging_utils import setup_logging

    setup_logging()
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
