"""Research-oriented pipeline and dataset metrics for the v2 runtime."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional, Sequence

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.edu_cti_v2.models import (
    ArticleDocument,
    ArticleFetchAttempt,
    CanonicalIncident,
    CanonicalMembership,
    PipelineRun,
    PipelineTask,
    SourceEnrichment,
    SourceIncident,
)
from src.edu_cti_v2.repositories import PipelineTaskRepository, ResearchMetricSnapshotRepository
from src.edu_cti_v2.services.read_models import V2CanonicalReadService


def _safe_pct(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator) * 100.0


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    rank = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = rank - lo
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac)


def _series_summary(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {
            "count": 0,
            "avg": 0.0,
            "median": 0.0,
            "p90": 0.0,
            "p95": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    total = float(sum(values))
    return {
        "count": len(values),
        "avg": total / len(values),
        "median": _percentile(values, 50),
        "p90": _percentile(values, 90),
        "p95": _percentile(values, 95),
        "min": float(min(values)),
        "max": float(max(values)),
    }


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


class V2ResearchMetricsService:
    """Build persistent research-grade metrics from the Postgres-backed v2 runtime."""

    def __init__(
        self,
        *,
        snapshot_repository: Optional[ResearchMetricSnapshotRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        read_service: Optional[V2CanonicalReadService] = None,
    ) -> None:
        self.snapshot_repository = snapshot_repository or ResearchMetricSnapshotRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.read_service = read_service or V2CanonicalReadService()

    def build_live_metrics(
        self,
        session: Session,
        *,
        snapshot_key: str = "global",
        snapshot_scope: str = "global",
        run_id: Optional[str] = None,
        statuses: Sequence[str] = ("open",),
        trigger: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        captured_at = datetime.now(timezone.utc).isoformat()

        source_incidents_total = int(session.execute(select(func.count(SourceIncident.id))).scalar_one() or 0)
        sources_with_selected_articles = int(
            session.execute(
                select(func.count(func.distinct(ArticleDocument.source_incident_id)))
                .where(ArticleDocument.is_selected_for_enrichment.is_(True))
            ).scalar_one()
            or 0
        )
        article_documents_total = int(session.execute(select(func.count(ArticleDocument.id))).scalar_one() or 0)
        source_enrichments_total = int(session.execute(select(func.count(SourceEnrichment.id))).scalar_one() or 0)
        canonical_incidents_total = int(
            session.execute(
                select(func.count(CanonicalIncident.id)).where(CanonicalIncident.status.in_(list(statuses)))
            ).scalar_one()
            or 0
        )
        canonicalized_sources_total = int(
            session.execute(
                select(func.count(func.distinct(CanonicalMembership.source_incident_id)))
                .join(CanonicalIncident, CanonicalIncident.id == CanonicalMembership.canonical_incident_id)
                .where(CanonicalIncident.status.in_(list(statuses)))
            ).scalar_one()
            or 0
        )
        membership_rows = session.execute(
            select(
                CanonicalMembership.canonical_incident_id,
                func.count(CanonicalMembership.id).label("membership_count"),
            )
            .join(CanonicalIncident, CanonicalIncident.id == CanonicalMembership.canonical_incident_id)
            .where(CanonicalIncident.status.in_(list(statuses)))
            .group_by(CanonicalMembership.canonical_incident_id)
        ).all()
        membership_counts = [int(row.membership_count or 0) for row in membership_rows]
        duplicate_sources_collapsed = max(canonicalized_sources_total - canonical_incidents_total, 0)

        attempts = list(session.execute(select(ArticleFetchAttempt)).scalars().all())
        successful_attempts = [attempt for attempt in attempts if attempt.success]
        selected_attempts = [
            attempt
            for attempt in successful_attempts
            if _coerce_bool((attempt.response_metadata or {}).get("selected_for_enrichment"))
        ]
        if not selected_attempts:
            selected_attempts = successful_attempts

        tier_stats: dict[str, dict[str, list[float] | int]] = defaultdict(
            lambda: {
                "attempts": 0,
                "successes": 0,
                "selected_successes": 0,
                "latency_ms": [],
                "success_chars": [],
                "selected_chars": [],
            }
        )

        for attempt in attempts:
            tier = (attempt.fetch_tier or "unknown").strip() or "unknown"
            stats = tier_stats[tier]
            stats["attempts"] += 1
            if attempt.latency_ms is not None:
                stats["latency_ms"].append(float(attempt.latency_ms))
            if attempt.success:
                stats["successes"] += 1
                if attempt.content_length is not None:
                    stats["success_chars"].append(float(attempt.content_length))
                if _coerce_bool((attempt.response_metadata or {}).get("selected_for_enrichment")):
                    stats["selected_successes"] += 1
                    if attempt.content_length is not None:
                        stats["selected_chars"].append(float(attempt.content_length))

        tier_rows: list[dict[str, Any]] = []
        for tier, stats in sorted(tier_stats.items(), key=lambda item: (-int(item[1]["attempts"]), item[0])):
            latency_summary = _series_summary(list(stats["latency_ms"]))
            success_char_summary = _series_summary(list(stats["success_chars"]))
            selected_char_summary = _series_summary(list(stats["selected_chars"]))
            attempts_count = int(stats["attempts"])
            successes_count = int(stats["successes"])
            selected_count = int(stats["selected_successes"])
            tier_rows.append(
                {
                    "fetch_tier": tier,
                    "attempts": attempts_count,
                    "attempt_share_pct": _safe_pct(attempts_count, len(attempts)),
                    "successes": successes_count,
                    "success_rate_pct": _safe_pct(successes_count, attempts_count),
                    "selected_successes": selected_count,
                    "selected_share_pct": _safe_pct(selected_count, len(selected_attempts)),
                    "latency_ms": latency_summary,
                    "success_content_length_chars": success_char_summary,
                    "selected_content_length_chars": selected_char_summary,
                }
            )

        tier_lookup = {row["fetch_tier"]: row for row in tier_rows}
        scrapling_selected_avg = float(
            (((tier_lookup.get("scrapling") or {}).get("selected_content_length_chars") or {}).get("avg") or 0.0)
        )
        oxylabs_selected_avg = float(
            (((tier_lookup.get("oxylabs") or {}).get("selected_content_length_chars") or {}).get("avg") or 0.0)
        )
        newspaper_selected_avg = float(
            (((tier_lookup.get("newspaper3k") or {}).get("selected_content_length_chars") or {}).get("avg") or 0.0)
        )
        richest_selected_tier = next(
            (
                row["fetch_tier"]
                for row in sorted(
                    tier_rows,
                    key=lambda item: (
                        -float((item.get("selected_content_length_chars") or {}).get("avg") or 0.0),
                        item["fetch_tier"],
                    ),
                )
                if int(row.get("selected_successes") or 0) > 0
            ),
            None,
        )

        terminal_tasks = [
            task
            for task in session.execute(select(PipelineTask)).scalars().all()
            if task.status in {"completed", "failed", "dead_letter", "cancelled"}
            and task.created_at
            and task.updated_at
        ]
        task_latency_rows: list[dict[str, Any]] = []
        for task_type in sorted({task.task_type for task in terminal_tasks}):
            rows = [task for task in terminal_tasks if task.task_type == task_type]
            lead_times = [
                max((task.updated_at - task.created_at).total_seconds(), 0.0)
                for task in rows
            ]
            summary = _series_summary(lead_times)
            task_latency_rows.append(
                {
                    "task_type": task_type,
                    "completed_count": sum(1 for task in rows if task.status == "completed"),
                    "failed_count": sum(1 for task in rows if task.status == "failed"),
                    "dead_letter_count": sum(1 for task in rows if task.status == "dead_letter"),
                    "cancelled_count": sum(1 for task in rows if task.status == "cancelled"),
                    "lead_time_seconds": summary,
                }
            )

        completed_runs = [
            run
            for run in session.execute(select(PipelineRun)).scalars().all()
            if run.started_at and run.finished_at
        ]
        run_duration_rows: list[dict[str, Any]] = []
        for run_type in sorted({run.run_type for run in completed_runs}):
            rows = [run for run in completed_runs if run.run_type == run_type]
            durations = [
                max((run.finished_at - run.started_at).total_seconds(), 0.0)
                for run in rows
            ]
            summary = _series_summary(durations)
            run_duration_rows.append(
                {
                    "run_type": run_type,
                    "run_count": len(rows),
                    "completed_count": sum(1 for run in rows if run.status == "completed"),
                    "failed_count": sum(1 for run in rows if run.status == "failed"),
                    "duration_seconds": summary,
                }
            )

        intelligence_summary = self.read_service.get_intelligence_summary(session, statuses=statuses)
        overview = intelligence_summary.get("overview", {})
        coverage = intelligence_summary.get("coverage", {})
        queue_backlog = self.pipeline_task_repository.get_status_summary(session)

        return {
            "snapshot_key": snapshot_key,
            "snapshot_scope": snapshot_scope,
            "captured_at": captured_at,
            "run_id": str(run_id) if run_id else None,
            "statuses": list(statuses),
            "trigger": trigger or {},
            "dataset_construction": {
                "source_incidents_total": source_incidents_total,
                "selected_article_sources_total": sources_with_selected_articles,
                "article_documents_total": article_documents_total,
                "source_enrichments_total": source_enrichments_total,
                "canonicalized_sources_total": canonicalized_sources_total,
                "canonical_incidents_total": canonical_incidents_total,
                "duplicate_sources_collapsed": duplicate_sources_collapsed,
                "source_to_selected_article_pct": _safe_pct(sources_with_selected_articles, source_incidents_total),
                "source_to_enrichment_pct": _safe_pct(source_enrichments_total, source_incidents_total),
                "source_to_canonical_pct": _safe_pct(canonicalized_sources_total, source_incidents_total),
                "deduplication_reduction_pct": _safe_pct(duplicate_sources_collapsed, canonicalized_sources_total),
                "avg_members_per_canonical": (
                    sum(membership_counts) / len(membership_counts) if membership_counts else 0.0
                ),
                "median_members_per_canonical": _percentile([float(value) for value in membership_counts], 50)
                if membership_counts
                else 0.0,
                "max_members_per_canonical": max(membership_counts) if membership_counts else 0,
            },
            "fetch_performance": {
                "overall": {
                    "attempts_total": len(attempts),
                    "successes_total": len(successful_attempts),
                    "failures_total": max(len(attempts) - len(successful_attempts), 0),
                    "success_rate_pct": _safe_pct(len(successful_attempts), len(attempts)),
                    "selected_successes_total": len(selected_attempts),
                    "fallback_selected_share_pct": _safe_pct(
                        sum(1 for attempt in selected_attempts if attempt.fetch_tier != "scrapling"),
                        len(selected_attempts),
                    ),
                },
                "tiers": tier_rows,
                "richness_comparison": {
                    "richest_selected_tier": richest_selected_tier,
                    "primary_selected_tier": "scrapling",
                    "scrapling_selected_avg_chars": scrapling_selected_avg,
                    "oxylabs_selected_avg_chars": oxylabs_selected_avg,
                    "newspaper3k_selected_avg_chars": newspaper_selected_avg,
                    "oxylabs_vs_scrapling_selected_char_delta": oxylabs_selected_avg - scrapling_selected_avg,
                    "oxylabs_vs_scrapling_selected_char_gain_pct": (
                        ((oxylabs_selected_avg - scrapling_selected_avg) / scrapling_selected_avg * 100.0)
                        if scrapling_selected_avg
                        else 0.0
                    ),
                    "oxylabs_vs_newspaper3k_selected_char_delta": oxylabs_selected_avg - newspaper_selected_avg,
                    "oxylabs_vs_newspaper3k_selected_char_gain_pct": (
                        ((oxylabs_selected_avg - newspaper_selected_avg) / newspaper_selected_avg * 100.0)
                        if newspaper_selected_avg
                        else 0.0
                    ),
                },
            },
            "pipeline_performance": {
                "task_lead_times": task_latency_rows,
                "run_durations": run_duration_rows,
                "queue_backlog_current": queue_backlog,
                "expired_leases_current": self.pipeline_task_repository.count_expired_leases(session),
            },
            "dataset_quality": {
                "actor_attributed_count": int(overview.get("actor_attributed_count") or 0),
                "actor_attributed_share": float(overview.get("actor_attributed_share") or 0.0),
                "ransomware_count": int(overview.get("ransomware_count") or 0),
                "ransomware_share": float(overview.get("ransomware_share") or 0.0),
                "breach_count": int(overview.get("breach_count") or 0),
                "breach_share": float(overview.get("breach_share") or 0.0),
                "vendor_linked_count": int(overview.get("vendor_linked_count") or 0),
                "vendor_linked_share": float(overview.get("vendor_linked_share") or 0.0),
                "known_record_events": int(overview.get("known_record_events") or 0),
                "known_record_volume": int(overview.get("known_record_volume") or 0),
                "attack_vector_known_count": int(coverage.get("attack_vector_known_count") or 0),
                "attack_vector_known_share": float(coverage.get("attack_vector_known_share") or 0.0),
                "record_loss_known_count": int(coverage.get("record_loss_known_count") or 0),
                "record_loss_known_share": float(coverage.get("record_loss_known_share") or 0.0),
            },
            "intelligence_summary": intelligence_summary,
        }

    def capture_snapshot(
        self,
        session: Session,
        *,
        snapshot_key: str = "global",
        snapshot_scope: str = "global",
        run_id: Optional[str] = None,
        statuses: Sequence[str] = ("open",),
        trigger: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        payload = self.build_live_metrics(
            session,
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
            run_id=run_id,
            statuses=statuses,
            trigger=trigger,
        )
        self.snapshot_repository.add_snapshot(
            session,
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
            payload=payload,
            run_id=run_id,
        )
        return payload

    def get_latest_or_live(
        self,
        session: Session,
        *,
        snapshot_key: str = "global",
        snapshot_scope: str = "global",
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, Any]:
        latest = self.snapshot_repository.get_latest(
            session,
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
        )
        requested_statuses = list(statuses)
        if latest and latest.payload and list(latest.payload.get("statuses") or requested_statuses) == requested_statuses:
            return latest.payload
        return self.build_live_metrics(
            session,
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
            statuses=statuses,
        )

    def list_recent_snapshots(
        self,
        session: Session,
        *,
        snapshot_key: Optional[str] = "global",
        snapshot_scope: Optional[str] = "global",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        snapshots = self.snapshot_repository.list_recent(
            session,
            snapshot_key=snapshot_key,
            snapshot_scope=snapshot_scope,
            limit=limit,
        )
        return [
            {
                "snapshot_id": str(snapshot.id),
                "snapshot_key": snapshot.snapshot_key,
                "snapshot_scope": snapshot.snapshot_scope,
                "run_id": str(snapshot.run_id) if snapshot.run_id else None,
                "captured_at": snapshot.captured_at.isoformat() if snapshot.captured_at else None,
                "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
                "payload": snapshot.payload or {},
            }
            for snapshot in snapshots
        ]

    def render_prometheus_text(self, payload: dict[str, Any]) -> str:
        lines: list[str] = []
        dataset = payload.get("dataset_construction", {})
        fetch = payload.get("fetch_performance", {})
        pipeline = payload.get("pipeline_performance", {})
        quality = payload.get("dataset_quality", {})

        def _add(metric: str, value: Any, labels: Optional[dict[str, Any]] = None) -> None:
            if value is None:
                return
            if isinstance(value, bool):
                numeric = 1.0 if value else 0.0
            elif isinstance(value, (int, float)):
                numeric = float(value)
            else:
                return
            if labels:
                label_text = ",".join(
                    f'{key}="{str(label_value)}"'
                    for key, label_value in sorted(labels.items())
                    if label_value is not None
                )
                lines.append(f"{metric}{{{label_text}}} {numeric}")
            else:
                lines.append(f"{metric} {numeric}")

        for key, value in dataset.items():
            _add(f"eduthreat_v2_dataset_{key}", value)

        overall_fetch = fetch.get("overall", {})
        for key, value in overall_fetch.items():
            _add(f"eduthreat_v2_fetch_{key}", value)

        for row in fetch.get("tiers", []):
            labels = {"tier": row.get("fetch_tier")}
            _add("eduthreat_v2_fetch_attempts_total", row.get("attempts"), labels)
            _add("eduthreat_v2_fetch_successes_total", row.get("successes"), labels)
            _add("eduthreat_v2_fetch_success_rate_pct", row.get("success_rate_pct"), labels)
            _add("eduthreat_v2_fetch_selected_successes_total", row.get("selected_successes"), labels)
            latency = row.get("latency_ms") or {}
            _add("eduthreat_v2_fetch_latency_avg_ms", latency.get("avg"), labels)
            _add("eduthreat_v2_fetch_latency_p95_ms", latency.get("p95"), labels)
            success_chars = row.get("success_content_length_chars") or {}
            _add("eduthreat_v2_fetch_success_chars_avg", success_chars.get("avg"), labels)
            _add("eduthreat_v2_fetch_success_chars_p90", success_chars.get("p90"), labels)
            selected_chars = row.get("selected_content_length_chars") or {}
            _add("eduthreat_v2_fetch_selected_chars_avg", selected_chars.get("avg"), labels)

        for row in pipeline.get("task_lead_times", []):
            labels = {"task_type": row.get("task_type")}
            _add("eduthreat_v2_task_completed_total", row.get("completed_count"), labels)
            _add("eduthreat_v2_task_failed_total", row.get("failed_count"), labels)
            _add("eduthreat_v2_task_dead_letter_total", row.get("dead_letter_count"), labels)
            lead = row.get("lead_time_seconds") or {}
            _add("eduthreat_v2_task_lead_time_avg_seconds", lead.get("avg"), labels)
            _add("eduthreat_v2_task_lead_time_p95_seconds", lead.get("p95"), labels)

        for row in pipeline.get("run_durations", []):
            labels = {"run_type": row.get("run_type")}
            _add("eduthreat_v2_runs_total", row.get("run_count"), labels)
            _add("eduthreat_v2_runs_completed_total", row.get("completed_count"), labels)
            duration = row.get("duration_seconds") or {}
            _add("eduthreat_v2_run_duration_avg_seconds", duration.get("avg"), labels)
            _add("eduthreat_v2_run_duration_p95_seconds", duration.get("p95"), labels)

        for key, value in quality.items():
            _add(f"eduthreat_v2_quality_{key}", value)

        return "\n".join(lines) + "\n"
