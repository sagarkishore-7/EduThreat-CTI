"""Collection orchestration for seeding v2 from existing Phase 1 source builders."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence
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
from src.edu_cti_v2.repositories import PipelineRunRepository

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
    ) -> None:
        self.session_factory = session_factory or create_session_factory()
        self.dual_writer = dual_writer or V2Phase1DualWriter(session_factory=self.session_factory)
        self.pipeline_run_repository = pipeline_run_repository or PipelineRunRepository()

    def _write_batch(
        self,
        incidents: Iterable[BaseIncident],
        *,
        counters: dict[str, int],
    ) -> None:
        for incident in incidents:
            event_key = build_phase1_source_event_key(incident)
            source_incident_id = self.dual_writer.write_observation(incident, event_key, force=True)
            if source_incident_id is not None:
                counters["observations_processed"] += 1

    def collect_into_v2(
        self,
        *,
        groups: Optional[Sequence[str]] = None,
        sources: Optional[Sequence[str]] = None,
        max_pages: Optional[int] = None,
        rss_max_age_days: int = 30,
        incremental: bool = True,
        include_paid_rss: bool = False,
    ) -> dict:
        groups_to_run = _normalize_groups(groups)
        source_filter = list(dict.fromkeys(sources or []))
        run_params = {
            "groups": groups_to_run,
            "sources": source_filter,
            "max_pages": max_pages,
            "rss_max_age_days": rss_max_age_days,
            "incremental": incremental,
            "include_paid_rss": include_paid_rss,
        }

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
                    include_paid=include_paid_rss,
                )
                if source_filter and group_sources == []:
                    continue

                def _save_callback(batch: List[BaseIncident]) -> None:
                    self._write_batch(batch, counters=counters)

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
                        include_paid=include_paid_rss,
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
                "run_id": str(run_id),
                "groups": groups_to_run,
                "sources": source_filter,
                "incremental": incremental,
                "include_paid_rss": include_paid_rss,
                "max_pages": max_pages,
                "rss_max_age_days": rss_max_age_days,
                "counts": {
                    "groups_run": len(groups_to_run),
                    "sources_run": counters["sources_run"],
                    "incidents_collected": counters["incidents_collected"],
                    "observations_processed": counters["observations_processed"],
                },
                "per_source_counts": per_source_counts,
            }
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
