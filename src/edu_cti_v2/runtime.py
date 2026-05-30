"""Unified long-running v2 worker runtime for split-service deployment."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
from dataclasses import asdict, dataclass
from typing import Callable, Optional

from src.edu_cti_v2.db import V2DatabaseSettings, create_session_factory
from src.edu_cti_v2.services.scheduler import V2SchedulerService
from src.edu_cti_v2.worker import V2WorkerRunSummary, run_worker_loop

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str) -> Optional[int]:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; ignoring override", name, value)
        return None


def _env_float_optional(name: str) -> Optional[float]:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float for %s=%r; ignoring override", name, value)
        return None


def _default_idle_resource_release_seconds() -> float:
    configured = _env_float_optional("EDU_CTI_V2_IDLE_RESOURCE_RELEASE_SECONDS")
    if configured is not None:
        return max(configured, 0.0)
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        return 300.0
    return 0.0


def _default_prewarm_models_enabled() -> bool:
    configured = os.environ.get("EDU_CTI_V2_PREWARM_MODELS")
    if configured is not None and configured.strip():
        return configured.strip().lower() in {"1", "true", "yes", "on"}
    # Railway workers are memory-constrained and can lazy-load the extraction
    # stack on first enrich task instead of eagerly materializing it at boot.
    if os.environ.get("RAILWAY_ENVIRONMENT"):
        return False
    return True


def _prewarm_ml_models() -> None:
    """Warm the shared process-wide ML helpers once before worker threads start."""
    try:
        from src.edu_cti.pipeline.phase2.extraction.mitre_rag import (
            _load_embed_model as _rag_load,
            _load_index as _rag_index,
        )
        from src.edu_cti.pipeline.phase2.extraction.ner_preprocessor import _load_model as _ner_load

        logger.info("Pre-warming v2 ML models before worker threads start...")
        _ner_load()
        _rag_load()
        _rag_index()
        logger.info("v2 ML model pre-warm complete.")
    except Exception as exc:
        logger.warning("v2 ML model pre-warm failed (non-fatal): %s", exc)


@dataclass
class _WorkerThreadState:
    worker_id: str
    thread: Optional[threading.Thread]
    task_type: Optional[str] = None
    exclude_task_types: tuple[str, ...] = ()
    summary: Optional[V2WorkerRunSummary] = None
    error: Optional[str] = None


class V2RuntimeService:
    """Run recurring scheduling and task workers inside one long-lived service."""

    def __init__(
        self,
        *,
        worker_count: int = 2,
        fetch_worker_count: Optional[int] = None,
        resolve_worker_count: Optional[int] = None,
        canonicalize_worker_count: Optional[int] = None,
        task_type: Optional[str] = None,
        poll_interval_seconds: float = 5.0,
        lease_seconds: Optional[int] = None,
        enable_scheduler: bool = True,
        scheduler_service: Optional[V2SchedulerService] = None,
        scheduler_poll_interval_seconds: float = 5.0,
        prewarm_models: bool = True,
        lease_recovery_interval_seconds: float = 15.0,
        idle_resource_release_seconds: Optional[float] = None,
        session_factory: Optional[Callable] = None,
    ) -> None:
        self.worker_count = max(worker_count, 1)
        if fetch_worker_count is None:
            fetch_worker_count = max(1, min(2, self.worker_count))
        if resolve_worker_count is None:
            resolve_worker_count = 1
        if canonicalize_worker_count is None:
            canonicalize_worker_count = 1 if task_type is None else 0
        self.fetch_worker_count = max(fetch_worker_count, 0)
        self.resolve_worker_count = max(resolve_worker_count, 0)
        self.canonicalize_worker_count = max(canonicalize_worker_count, 0)
        self.task_type = task_type
        self.poll_interval_seconds = poll_interval_seconds
        self.lease_seconds = lease_seconds
        self.enable_scheduler = enable_scheduler
        self.prewarm_models = prewarm_models
        self.lease_recovery_interval_seconds = max(float(lease_recovery_interval_seconds), 0.0)
        self.idle_resource_release_seconds = (
            _default_idle_resource_release_seconds()
            if idle_resource_release_seconds is None
            else max(float(idle_resource_release_seconds), 0.0)
        )
        self.session_factory = session_factory
        self.scheduler_service = scheduler_service or V2SchedulerService(
            poll_interval_seconds=scheduler_poll_interval_seconds,
        )

        self._stop_event = threading.Event()
        self._worker_states: list[_WorkerThreadState] = []
        self._running = False
        self._last_lease_recovery_monotonic = 0.0
        self._last_active_tasks_monotonic = time.monotonic()
        self._resources_released_for_idle = False

    def _start_worker_thread(self, state: _WorkerThreadState) -> None:
        state.summary = None
        state.error = None
        thread = threading.Thread(
            target=self._worker_target,
            args=(state,),
            name=state.worker_id.replace(":", "-"),
            daemon=True,
        )
        state.thread = thread
        thread.start()

    def _worker_target(self, state: _WorkerThreadState) -> None:
        worker_id = state.worker_id
        try:
            state.summary = run_worker_loop(
                session_factory=self.session_factory,
                worker_id=worker_id,
                task_type=state.task_type,
                exclude_task_types=state.exclude_task_types,
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
        self._last_lease_recovery_monotonic = 0.0
        self._last_active_tasks_monotonic = time.monotonic()
        self._resources_released_for_idle = False
        if self.session_factory is None:
            self.session_factory = create_session_factory(V2DatabaseSettings.from_env())

        if self.prewarm_models and (self.task_type is None or self.task_type in {"enrich_source", "reenrich"}):
            _prewarm_ml_models()

        if self.enable_scheduler:
            self.scheduler_service.start()

        if self.task_type is None:
            orchestrator_state = _WorkerThreadState(
                worker_id="v2-runtime:orchestrator",
                thread=None,
                task_type="orchestrate_plan",
            )
            self._worker_states.append(orchestrator_state)
            self._start_worker_thread(orchestrator_state)

            analytics_state = _WorkerThreadState(
                worker_id="v2-runtime:analytics",
                thread=None,
                task_type="refresh_analytics",
            )
            self._worker_states.append(analytics_state)
            self._start_worker_thread(analytics_state)

            for index in range(self.worker_count):
                worker_id = f"v2-runtime:{index + 1}"
                state = _WorkerThreadState(
                    worker_id=worker_id,
                    thread=None,
                    exclude_task_types=(
                        "orchestrate_plan",
                        "refresh_analytics",
                        "fetch_article",
                        "resolve_url",
                        "canonicalize",
                    ),
                )
                self._worker_states.append(state)
                self._start_worker_thread(state)

            for index in range(self.fetch_worker_count):
                worker_id = f"v2-runtime:fetch:{index + 1}"
                state = _WorkerThreadState(
                    worker_id=worker_id,
                    thread=None,
                    task_type="fetch_article",
                )
                self._worker_states.append(state)
                self._start_worker_thread(state)

            for index in range(self.resolve_worker_count):
                worker_id = f"v2-runtime:resolve:{index + 1}"
                state = _WorkerThreadState(
                    worker_id=worker_id,
                    thread=None,
                    task_type="resolve_url",
                )
                self._worker_states.append(state)
                self._start_worker_thread(state)

            for index in range(self.canonicalize_worker_count):
                worker_id = f"v2-runtime:canonicalize:{index + 1}"
                state = _WorkerThreadState(
                    worker_id=worker_id,
                    thread=None,
                    task_type="canonicalize",
                )
                self._worker_states.append(state)
                self._start_worker_thread(state)
        else:
            for index in range(self.worker_count):
                worker_id = f"v2-runtime:{index + 1}"
                state = _WorkerThreadState(
                    worker_id=worker_id,
                    thread=None,
                    task_type=self.task_type,
                )
                self._worker_states.append(state)
                self._start_worker_thread(state)

        self._running = True
        logger.info(
            "Started v2 runtime: workers=%s fetch_workers=%s resolve_workers=%s canonicalize_workers=%s task_type=%s scheduler=%s",
            self.worker_count,
            self.fetch_worker_count,
            self.resolve_worker_count,
            self.canonicalize_worker_count,
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

        self._release_idle_resources(reason="stop")
        self._running = False
        logger.info("Stopped v2 runtime")
        return self.get_status()

    def _recover_expired_leases(self) -> int:
        try:
            from src.edu_cti_v2.repositories import PipelineTaskRepository

            session_factory = self.session_factory or create_session_factory(V2DatabaseSettings.from_env())
            task_repository = PipelineTaskRepository()
            with session_factory() as session:
                recovered = task_repository.requeue_expired_leases(session, limit=200)
                session.commit()
            if recovered:
                logger.warning("Recovered %d expired v2 task lease(s)", recovered)
            return recovered
        except Exception as exc:
            logger.warning("v2 expired-lease recovery failed (non-fatal): %s", exc)
            return 0

    def _count_active_tasks(self) -> int:
        try:
            from src.edu_cti_v2.repositories import PipelineTaskRepository

            session_factory = self.session_factory or create_session_factory(V2DatabaseSettings.from_env())
            task_repository = PipelineTaskRepository()
            with session_factory() as session:
                return task_repository.count_active(session)
        except Exception as exc:
            logger.debug("v2 active task count failed during idle cleanup check: %s", exc)
            return 0

    def _release_idle_resources(self, *, reason: str) -> None:
        try:
            from src.edu_cti_v2.services.resource_cleanup import release_idle_ml_resources

            released = release_idle_ml_resources()
            if any(bool(value) for value in released.values()):
                logger.info("Released idle v2 ML resources: reason=%s released=%s", reason, released)
        except Exception as exc:
            logger.warning("v2 idle resource cleanup failed (non-fatal): %s", exc)

    def _maybe_release_idle_resources(self) -> None:
        if self.idle_resource_release_seconds <= 0:
            return

        active_tasks = self._count_active_tasks()
        now = time.monotonic()
        if active_tasks > 0:
            self._last_active_tasks_monotonic = now
            self._resources_released_for_idle = False
            return

        idle_for = now - self._last_active_tasks_monotonic
        if self._resources_released_for_idle or idle_for < self.idle_resource_release_seconds:
            return

        self._release_idle_resources(reason=f"idle_for_{idle_for:.0f}s")
        self._resources_released_for_idle = True

    def tick(self) -> None:
        """Restart any dead worker thread while the runtime is supposed to be running."""
        if not self._running or self._stop_event.is_set():
            return

        now = time.monotonic()
        if (
            self.lease_recovery_interval_seconds > 0
            and (
                self._last_lease_recovery_monotonic == 0.0
                or now - self._last_lease_recovery_monotonic >= self.lease_recovery_interval_seconds
            )
        ):
            self._recover_expired_leases()
            self._last_lease_recovery_monotonic = now

        self._maybe_release_idle_resources()

        for state in self._worker_states:
            if state.thread and state.thread.is_alive():
                continue
            if state.summary and state.summary.stop_reason == "stopped":
                continue
            logger.warning(
                "Restarting v2 runtime worker after unexpected stop: worker_id=%s error=%s summary=%s",
                state.worker_id,
                state.error,
                state.summary.stop_reason if state.summary else None,
            )
            self._start_worker_thread(state)

    def get_status(self) -> dict[str, object]:
        return {
            "running": self._running,
            "worker_count": self.worker_count,
            "fetch_worker_count": self.fetch_worker_count,
            "resolve_worker_count": self.resolve_worker_count,
            "canonicalize_worker_count": self.canonicalize_worker_count,
            "task_type": self.task_type,
            "scheduler_enabled": self.enable_scheduler,
            "idle_resource_release_seconds": self.idle_resource_release_seconds,
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
        help="Number of general long-running worker threads",
    )
    parser.add_argument(
        "--fetch-workers",
        type=int,
        default=_env_int("EDU_CTI_V2_FETCH_WORKER_COUNT"),
        help="Number of dedicated fetch_article worker threads",
    )
    parser.add_argument(
        "--resolve-workers",
        type=int,
        default=_env_int("EDU_CTI_V2_RESOLVE_WORKER_COUNT"),
        help="Number of dedicated resolve_url worker threads",
    )
    parser.add_argument(
        "--canonicalize-workers",
        type=int,
        default=_env_int("EDU_CTI_V2_CANONICALIZE_WORKER_COUNT"),
        help="Number of dedicated canonicalize worker threads",
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
    parser.add_argument(
        "--no-prewarm-models",
        action="store_true",
        help="Skip eager GLiNER/MITRE model warm-up before worker threads start",
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
        fetch_worker_count=args.fetch_workers,
        resolve_worker_count=args.resolve_workers,
        canonicalize_worker_count=args.canonicalize_workers,
        task_type=args.task_type,
        poll_interval_seconds=args.poll_interval,
        lease_seconds=args.lease_seconds,
        enable_scheduler=args.enable_scheduler and not args.no_scheduler,
        scheduler_poll_interval_seconds=args.scheduler_poll_interval,
        prewarm_models=not args.no_prewarm_models and _default_prewarm_models_enabled(),
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
            runtime.tick()
            time.sleep(1.0)
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
