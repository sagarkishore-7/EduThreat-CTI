"""Collection orchestration for seeding v2 from existing Phase 1 source builders."""

from __future__ import annotations

from collections import defaultdict
import os
import time
from typing import Callable, Dict, Iterable, List, Optional, Sequence
from uuid import uuid4

from sqlalchemy.orm import sessionmaker

from src.edu_cti.core.models import BaseIncident
from src.edu_cti.core.sources import (
    API_SOURCE_REGISTRY,
    CURATED_SOURCE_REGISTRY,
    NEWS_SOURCE_REGISTRY,
    PAID_RSS_SOURCE_REGISTRY,
    RSS_SOURCE_REGISTRY,
)
from src.edu_cti.pipeline.phase1.api_sources import collect_api_incidents
from src.edu_cti.pipeline.phase1.curated import collect_curated_incidents
from src.edu_cti.pipeline.phase1.news import collect_news_incidents
from src.edu_cti.pipeline.phase1.rss import collect_rss_incidents
from src.edu_cti_v2.db import create_session_factory
from src.edu_cti_v2.models import PipelineRun
from src.edu_cti_v2.phase1_dual_write import V2Phase1DualWriter, build_phase1_source_event_key
from src.edu_cti_v2.repositories import PipelineRunRepository, PipelineTaskRepository

_GROUP_ORDER = ("curated", "news", "rss", "api")
_GROUP_REGISTRIES = {
    "curated": CURATED_SOURCE_REGISTRY,
    "news": NEWS_SOURCE_REGISTRY,
    "rss": RSS_SOURCE_REGISTRY,
    "api": API_SOURCE_REGISTRY,
}
_COLLECTORS = {
    "curated": collect_curated_incidents,
    "news": collect_news_incidents,
    "rss": collect_rss_incidents,
    "api": collect_api_incidents,
}


def _env_optional_int(name: str, default: Optional[int]) -> Optional[int]:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_optional_flag(name: str) -> Optional[bool]:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _default_include_paid_rss() -> bool:
    """Use Oxylabs source discovery only when explicitly enabled by env."""
    for env_name in (
        "EDU_CTI_INCLUDE_PAID_RSS",
        "EDU_CTI_INCLUDE_OXYLABS_NEWS_SOURCE",
        "EDU_CTI_OXYLABS_NEWS_ENABLED",
    ):
        value = _env_optional_flag(env_name)
        if value is not None:
            return value
    return _env_flag("EDU_CTI_OXYLABS_ENABLED", "0")


def _normalize_groups(groups: Optional[Sequence[str]]) -> list[str]:
    if not groups:
        return list(_GROUP_ORDER)
    seen: list[str] = []
    for group in groups:
        if group not in _GROUP_REGISTRIES:
            raise ValueError(f"Unsupported group: {group}")
        if group not in seen:
            seen.append(group)
    return seen


def _group_sources(
    *,
    group: str,
    sources: Optional[Sequence[str]],
    include_paid: bool = False,
) -> list[str] | None:
    if not sources:
        return None
    registry = dict(_GROUP_REGISTRIES[group])
    if group == "rss" and include_paid:
        registry.update(PAID_RSS_SOURCE_REGISTRY)
    selected = [source for source in sources if source in registry]
    return selected or []


class V2CollectionService:
    """Run source collection and persist raw observations directly into v2."""

    def __init__(
        self,
        *,
        session_factory: Optional[sessionmaker] = None,
        dual_writer: Optional[V2Phase1DualWriter] = None,
        pipeline_run_repository: Optional[PipelineRunRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.session_factory = session_factory or create_session_factory()
        self.dual_writer = dual_writer or V2Phase1DualWriter(session_factory=self.session_factory)
        self.pipeline_run_repository = pipeline_run_repository or PipelineRunRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.sleep_fn = sleep_fn or time.sleep

    def _maybe_wait_for_task_backlog(
        self,
        *,
        task_types: Sequence[str],
        backlog_limit: Optional[int],
        backlog_resume_ratio: float,
        backlog_poll_seconds: float,
        counters: dict[str, int],
        counter_prefix: str,
    ) -> None:
        if not backlog_limit or backlog_limit <= 0:
            return

        resume_ratio = min(max(backlog_resume_ratio, 0.0), 1.0)
        resume_threshold = max(1, int(backlog_limit * resume_ratio))
        max_counter_key = f"max_{counter_prefix}_backlog_observed"
        wait_cycles_key = f"{counter_prefix}_backpressure_wait_cycles"
        wait_seconds_key = f"{counter_prefix}_backpressure_wait_seconds"

        with self.session_factory() as session:
            backlog = self.pipeline_task_repository.count_active(
                session,
                task_types=task_types,
            )
        counters[max_counter_key] = max(counters[max_counter_key], backlog)
        if backlog <= backlog_limit:
            return

        while backlog > resume_threshold:
            counters[wait_cycles_key] += 1
            counters[wait_seconds_key] += int(max(backlog_poll_seconds, 0.0))
            self.sleep_fn(max(backlog_poll_seconds, 0.0))
            with self.session_factory() as session:
                backlog = self.pipeline_task_repository.count_active(
                    session,
                    task_types=task_types,
                )
            counters[max_counter_key] = max(counters[max_counter_key], backlog)

    def _write_batch(
        self,
        incidents: Iterable[BaseIncident],
        *,
        counters: dict[str, int],
        fetch_backlog_limit: Optional[int],
        resolve_backlog_limit: Optional[int],
        fetch_backlog_resume_ratio: float,
        resolve_backlog_resume_ratio: float,
        backlog_poll_seconds: float,
    ) -> None:
        for incident in incidents:
            event_key = build_phase1_source_event_key(incident)
            source_incident_id = self.dual_writer.write_observation(incident, event_key, force=True)
            if source_incident_id is not None:
                counters["observations_processed"] += 1
        self._maybe_wait_for_task_backlog(
            task_types=("resolve_url",),
            backlog_limit=resolve_backlog_limit,
            backlog_resume_ratio=resolve_backlog_resume_ratio,
            backlog_poll_seconds=backlog_poll_seconds,
            counters=counters,
            counter_prefix="resolve",
        )
        self._maybe_wait_for_task_backlog(
            task_types=("fetch_article",),
            backlog_limit=fetch_backlog_limit,
            backlog_resume_ratio=fetch_backlog_resume_ratio,
            backlog_poll_seconds=backlog_poll_seconds,
            counters=counters,
            counter_prefix="fetch",
        )

    def collect_into_v2(
        self,
        *,
        groups: Optional[Sequence[str]] = None,
        sources: Optional[Sequence[str]] = None,
        max_pages: Optional[int] = None,
        rss_max_age_days: int = 30,
        incremental: bool = True,
        include_paid_rss: Optional[bool] = None,
        persist_run: bool = True,
        fetch_backlog_limit: Optional[int] = None,
        resolve_backlog_limit: Optional[int] = None,
        fetch_backlog_resume_ratio: float = 0.0,
        resolve_backlog_resume_ratio: float = 0.0,
        backlog_poll_seconds: float = 0.0,
    ) -> dict:
        effective_include_paid_rss = _default_include_paid_rss() if include_paid_rss is None else include_paid_rss
        fetch_backlog_limit = _env_optional_int("EDU_CTI_V2_FETCH_BACKLOG_LIMIT", fetch_backlog_limit)
        resolve_backlog_limit = _env_optional_int("EDU_CTI_V2_RESOLVE_BACKLOG_LIMIT", resolve_backlog_limit)
        fetch_backlog_resume_ratio = _env_float(
            "EDU_CTI_V2_FETCH_BACKLOG_RESUME_RATIO",
            fetch_backlog_resume_ratio if fetch_backlog_resume_ratio > 0 else 0.6,
        )
        resolve_backlog_resume_ratio = _env_float(
            "EDU_CTI_V2_RESOLVE_BACKLOG_RESUME_RATIO",
            resolve_backlog_resume_ratio if resolve_backlog_resume_ratio > 0 else 0.6,
        )
        backlog_poll_seconds = _env_float(
            "EDU_CTI_V2_BACKLOG_POLL_SECONDS",
            backlog_poll_seconds if backlog_poll_seconds > 0 else 5.0,
        )
        if fetch_backlog_limit is None:
            fetch_backlog_limit = 500
        if resolve_backlog_limit is None:
            resolve_backlog_limit = 200

        groups_to_run = _normalize_groups(groups)
        source_filter = list(dict.fromkeys(sources or []))
        run_params = {
            "groups": groups_to_run,
            "sources": source_filter,
            "max_pages": max_pages,
            "rss_max_age_days": rss_max_age_days,
            "incremental": incremental,
            "include_paid_rss": effective_include_paid_rss,
            "include_paid_rss_source": "env" if include_paid_rss is None else "override",
            "fetch_backlog_limit": fetch_backlog_limit,
            "resolve_backlog_limit": resolve_backlog_limit,
            "fetch_backlog_resume_ratio": fetch_backlog_resume_ratio,
            "resolve_backlog_resume_ratio": resolve_backlog_resume_ratio,
            "backlog_poll_seconds": backlog_poll_seconds,
        }

        run_id = None
        if persist_run:
            with self.session_factory() as session:
                run = PipelineRun(
                    run_type="collect",
                    status="pending",
                    service_name="v2-collection-service",
                    params=run_params,
                    result={},
                )
                if run.id is None:
                    run.id = uuid4()
                self.pipeline_run_repository.add(session, run)
                self.pipeline_run_repository.mark_started(session, run)
                flush = getattr(session, "flush", None)
                if callable(flush):
                    flush()
                session.commit()
                run_id = run.id

        counters: dict[str, int] = defaultdict(int)
        per_source_counts: dict[str, int] = {}
        try:
            for group in groups_to_run:
                group_sources = _group_sources(
                    group=group,
                    sources=source_filter,
                    include_paid=effective_include_paid_rss,
                )
                if source_filter and group_sources == []:
                    continue

                def _save_callback(batch: List[BaseIncident]) -> None:
                    self._write_batch(
                        batch,
                        counters=counters,
                        fetch_backlog_limit=fetch_backlog_limit,
                        resolve_backlog_limit=resolve_backlog_limit,
                        fetch_backlog_resume_ratio=fetch_backlog_resume_ratio,
                        resolve_backlog_resume_ratio=resolve_backlog_resume_ratio,
                        backlog_poll_seconds=backlog_poll_seconds,
                    )

                if group == "curated":
                    results = _COLLECTORS[group](
                        sources=group_sources or None,
                        max_pages=max_pages,
                        save_callback=_save_callback,
                        incremental=incremental,
                    )
                elif group == "news":
                    results = _COLLECTORS[group](
                        max_pages=max_pages,
                        sources=group_sources or None,
                        save_callback=_save_callback,
                        incremental=incremental,
                    )
                elif group == "rss":
                    results = _COLLECTORS[group](
                        sources=group_sources or None,
                        max_age_days=rss_max_age_days,
                        save_callback=_save_callback,
                        incremental=incremental,
                        include_paid=effective_include_paid_rss,
                    )
                else:
                    results = _COLLECTORS[group](
                        sources=group_sources or None,
                        max_pages=max_pages,
                        save_callback=_save_callback,
                        incremental=incremental,
                    )

                for source_name, incidents in results.items():
                    count = len(incidents)
                    per_source_counts[source_name] = count
                    counters["sources_run"] += 1
                    counters["incidents_collected"] += count

            result = {
                "run_id": str(run_id) if run_id else None,
                "groups": groups_to_run,
                "sources": source_filter,
                "incremental": incremental,
                "include_paid_rss": effective_include_paid_rss,
                "include_paid_rss_source": "env" if include_paid_rss is None else "override",
                "max_pages": max_pages,
                "rss_max_age_days": rss_max_age_days,
                "counts": {
                    "groups_run": len(groups_to_run),
                    "sources_run": counters["sources_run"],
                    "incidents_collected": counters["incidents_collected"],
                    "observations_processed": counters["observations_processed"],
                    "fetch_backpressure_wait_cycles": counters["fetch_backpressure_wait_cycles"],
                    "fetch_backpressure_wait_seconds": counters["fetch_backpressure_wait_seconds"],
                    "max_fetch_backlog_observed": counters["max_fetch_backlog_observed"],
                    "resolve_backpressure_wait_cycles": counters["resolve_backpressure_wait_cycles"],
                    "resolve_backpressure_wait_seconds": counters["resolve_backpressure_wait_seconds"],
                    "max_resolve_backlog_observed": counters["max_resolve_backlog_observed"],
                },
                "per_source_counts": per_source_counts,
            }
            if run_id is not None:
                with self.session_factory() as session:
                    persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
                    if persisted_run is not None:
                        self.pipeline_run_repository.mark_finished(
                            session,
                            persisted_run,
                            status="completed",
                            result=result,
                        )
                        session.commit()
            return result
        except Exception as exc:
            if run_id is not None:
                with self.session_factory() as session:
                    persisted_run = self.pipeline_run_repository.get_by_id(session, run_id)
                    if persisted_run is not None:
                        self.pipeline_run_repository.mark_finished(
                            session,
                            persisted_run,
                            status="failed",
                            result={},
                            error=str(exc),
                        )
                        session.commit()
            raise
