"""Lightweight scheduler for recurring v2 orchestration plans."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import schedule

from src.edu_cti_v2.services.orchestration import V2OrchestrationService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class V2ScheduledJobDefinition:
    name: str
    plan_name: str
    interval_hours: int
    description: str


_DEFAULT_JOB_DEFINITIONS: dict[str, V2ScheduledJobDefinition] = {
    "rss_fast_refresh": V2ScheduledJobDefinition(
        name="rss_fast_refresh",
        plan_name="rss_fast_refresh",
        interval_hours=1,
        description="Hourly RSS-only refresh and queue drain.",
    ),
    "incremental_refresh": V2ScheduledJobDefinition(
        name="incremental_refresh",
        plan_name="incremental_refresh",
        interval_hours=6,
        description="Six-hour incremental refresh across all source groups.",
    ),
    "daily_quality_refresh": V2ScheduledJobDefinition(
        name="daily_quality_refresh",
        plan_name="daily_quality_refresh",
        interval_hours=24,
        description="Daily incremental refresh followed by data-quality re-enrichment.",
    ),
}


class V2SchedulerService:
    """Run recurring v2 orchestration plans in a background thread."""

    def __init__(
        self,
        *,
        orchestration_service: Optional[V2OrchestrationService] = None,
        scheduler: Optional[schedule.Scheduler] = None,
        poll_interval_seconds: float = 5.0,
        job_definitions: Optional[dict[str, V2ScheduledJobDefinition]] = None,
    ) -> None:
        self.orchestration_service = orchestration_service or V2OrchestrationService()
        self.scheduler = scheduler or schedule.Scheduler()
        self.poll_interval_seconds = poll_interval_seconds
        self.job_definitions = job_definitions or dict(_DEFAULT_JOB_DEFINITIONS)

        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._run_lock = threading.Lock()
        self._active_job_name: Optional[str] = None
        self._active_plan_name: Optional[str] = None
        self._last_results: dict[str, dict[str, Any]] = {}

    def list_jobs(self) -> list[dict[str, Any]]:
        return [
            {
                "name": definition.name,
                "plan_name": definition.plan_name,
                "interval_hours": definition.interval_hours,
                "description": definition.description,
            }
            for definition in self.job_definitions.values()
        ]

    def _configure_jobs(self) -> None:
        self.scheduler.clear()
        for definition in self.job_definitions.values():
            self.scheduler.every(definition.interval_hours).hours.do(
                self.trigger_job,
                definition.name,
            )

    def start(self) -> dict[str, Any]:
        if self._running:
            return self.get_status()

        self._configure_jobs()
        self._running = True
        self._scheduler_thread = threading.Thread(
            target=self._run_loop,
            name="v2-scheduler",
            daemon=True,
        )
        self._scheduler_thread.start()
        logger.info("Started v2 scheduler with %d jobs", len(self.job_definitions))
        return self.get_status()

    def stop(self) -> dict[str, Any]:
        self._running = False
        thread = self._scheduler_thread
        if thread and thread.is_alive():
            thread.join(timeout=max(self.poll_interval_seconds * 2, 0.1))
        self._scheduler_thread = None
        logger.info("Stopped v2 scheduler")
        return self.get_status()

    def _run_loop(self) -> None:
        while self._running:
            self.scheduler.run_pending()
            time.sleep(max(self.poll_interval_seconds, 0.0))

    def get_status(self) -> dict[str, Any]:
        scheduled_jobs = []
        for job in self.scheduler.jobs:
            scheduled_jobs.append(
                {
                    "next_run": job.next_run.isoformat() if job.next_run else None,
                    "interval_seconds": getattr(job, "interval", None),
                    "unit": getattr(job, "unit", None),
                    "tags": sorted(job.tags) if job.tags else [],
                }
            )
        return {
            "running": self._running,
            "active_job_name": self._active_job_name,
            "active_plan_name": self._active_plan_name,
            "jobs": self.list_jobs(),
            "scheduled_jobs": scheduled_jobs,
            "last_results": self._last_results,
        }

    def _run_job(self, job_name: str) -> None:
        definition = self.job_definitions[job_name]
        if not self._run_lock.acquire(blocking=False):
            self._last_results[job_name] = {
                "status": "skipped",
                "reason": "busy",
                "at": datetime.now(timezone.utc).isoformat(),
            }
            logger.info("Skipping v2 scheduled job %s because another plan is running", job_name)
            return

        self._active_job_name = job_name
        self._active_plan_name = definition.plan_name
        started_at = datetime.now(timezone.utc)
        try:
            result = self.orchestration_service.run_plan(
                plan_name=definition.plan_name,
                worker_id=f"v2-scheduler:{job_name}",
            )
            self._last_results[job_name] = {
                "status": "completed",
                "at": started_at.isoformat(),
                "result": result,
            }
        except Exception as exc:
            logger.exception("v2 scheduled job %s failed", job_name)
            self._last_results[job_name] = {
                "status": "failed",
                "at": started_at.isoformat(),
                "error": str(exc),
            }
        finally:
            self._active_job_name = None
            self._active_plan_name = None
            self._run_lock.release()

    def trigger_job(self, job_name: str, *, background: bool = True) -> dict[str, Any]:
        if job_name not in self.job_definitions:
            raise ValueError(f"Unknown v2 scheduled job: {job_name}")

        definition = self.job_definitions[job_name]
        if background:
            thread = threading.Thread(
                target=self._run_job,
                args=(job_name,),
                name=f"v2-job-{job_name}",
                daemon=True,
            )
            thread.start()
            return {
                "job_name": job_name,
                "plan_name": definition.plan_name,
                "status": "started",
                "background": True,
            }

        self._run_job(job_name)
        last = self._last_results.get(job_name, {})
        return {
            "job_name": job_name,
            "plan_name": definition.plan_name,
            "background": False,
            **last,
        }
