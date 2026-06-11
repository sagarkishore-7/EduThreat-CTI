"""Bulk LLM title-relevance classifier.

Replaces the legacy keyword pre-filter for news/RSS sources. Every collected
news/RSS title is saved as ``relevance_status='pending'``; this service leases a
batch of those pending rows, asks the LLM in a *single* prompt whether each title
describes a specific education-sector cyber-attack/data-breach incident, and only
the rows it keeps (``relevant``) are enqueued for article fetch. Confident
negatives become ``irrelevant`` (kept for audit/recall analysis, never fetched).

Design notes:
  * **High-recall pre-filter, not the final arbiter** — the full-article
    ``is_edu_cyber_incident`` gate in phase2 enrichment stays as the precision
    backstop. A title false-positive only costs one fetch; a false-negative
    permanently loses coverage, so the keep rule is deliberately recall-biased
    (only drop confident negatives).
  * **Memory-light bulk action** — holds only short strings (title + snippet) and
    makes one remote Ollama call per ~75 titles, so it runs alongside the
    memory-heavy enrichment stage without OOM risk.
  * **Title dedup** — identical normalized titles (cross-source same-incident) are
    classified once and the verdict fanned out to every duplicate row.
  * **Fail-safe** — on LLM/transport failure the whole batch is left untouched and
    the task retries (rows stay ``pending``); titles the model simply omits from a
    valid response fail *open* to ``relevant`` so coverage is never silently lost.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
from src.edu_cti_v2.env import get_float, get_int
from src.edu_cti_v2.models import SourceIncident
from src.edu_cti_v2.repositories import PipelineTaskRepository, SourceIncidentRepository
from src.edu_cti_v2.services.intake import V2IntakeService

logger = logging.getLogger(__name__)

# News/RSS rows are the only ones that go through title classification; curated
# and api feeds are high-precision and routed straight to fetch by intake.
_CLASSIFIED_SOURCE_GROUPS = ("news", "rss")

_DEFAULT_BATCH_SIZE = 75
# Only drop a title when the model is *confident* it is NOT an education-sector
# cyber incident. Uncertain negatives are kept (recall bias).
_DEFAULT_DROP_CONFIDENCE = 0.7

_SNIPPET_MAX_CHARS = 240

_SYSTEM_PROMPT = (
    "You are a Senior Cyber Threat Intelligence analyst specialising in education-"
    "sector cyber incidents. You are triaging news headlines to decide which are "
    "worth fetching in full.\n\n"
    "For each item decide: does this headline plausibly report a SPECIFIC, DISCRETE "
    "cyber-attack or data-breach incident affecting one or more identified EDUCATION "
    "institutions? Education institutions include schools, school districts, colleges, "
    "universities, seminaries/religious colleges, academies, and education-service / "
    "EdTech / student-information / learning-management providers.\n\n"
    "Set edu_cyber=false for headlines that are clearly NOT a specific education cyber "
    "incident, e.g.:\n"
    "  * physical crime, accidents, or violence that merely contains words like 'hack' "
    "or 'guard' (e.g. 'School guard hacked to death')\n"
    "  * aggregate statistics or annual threat reports ('ransomware in education up 70%')\n"
    "  * trend pieces, opinion, best-practice / how-to / advice articles\n"
    "  * cyber incidents with no education victim, or education news with no cyber angle\n\n"
    "Because a missed incident is far more costly than an extra fetch, only set "
    "edu_cyber=false with HIGH confidence; when genuinely unsure, set edu_cyber=true "
    "with low confidence so a human/LLM can review the full article.\n\n"
    "confidence is your certainty (0.0-1.0) in the edu_cyber value you chose. Return a "
    'JSON object {"results": [...]} with exactly one entry per numbered item, each '
    '{"idx": <item number>, "edu_cyber": <bool>, "confidence": <0-1>, "reason": '
    '"<short phrase>"}.'
)

_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "idx": {"type": "integer"},
                    "edu_cyber": {"type": "boolean"},
                    "confidence": {"type": "number"},
                    "reason": {"type": "string"},
                },
                "required": ["idx", "edu_cyber", "confidence"],
            },
        }
    },
    "required": ["results"],
}


def _normalize_title(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


class V2TitleClassificationService:
    """Lease a batch of pending news/RSS titles and classify them in one LLM call."""

    def __init__(
        self,
        *,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
        source_incident_repository: Optional[SourceIncidentRepository] = None,
        intake_service: Optional[V2IntakeService] = None,
        llm_client: Optional[OllamaLLMClient] = None,
        batch_size: Optional[int] = None,
        drop_confidence: Optional[float] = None,
    ) -> None:
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()
        self.source_incident_repository = (
            source_incident_repository or SourceIncidentRepository()
        )
        self.intake_service = intake_service or V2IntakeService(
            pipeline_task_repository=self.pipeline_task_repository,
        )
        self._llm_client = llm_client
        self.batch_size = (
            batch_size
            if batch_size is not None
            else (get_int("TITLE_CLASSIFY_BATCH", "EDU_CTI_TITLE_CLASSIFY_BATCH", default=_DEFAULT_BATCH_SIZE) or _DEFAULT_BATCH_SIZE)
        )
        self.drop_confidence = (
            drop_confidence
            if drop_confidence is not None
            else (
                get_float(
                    "TITLE_CLASSIFY_DROP_CONFIDENCE",
                    "EDU_CTI_TITLE_CLASSIFY_DROP_CONFIDENCE",
                    default=_DEFAULT_DROP_CONFIDENCE,
                )
                or _DEFAULT_DROP_CONFIDENCE
            )
        )

    @property
    def llm_client(self) -> OllamaLLMClient:
        if self._llm_client is None:
            self._llm_client = OllamaLLMClient()
        return self._llm_client

    # -- batch select --------------------------------------------------------
    def _lease_pending_rows(self, session: Session):
        stmt = (
            select(SourceIncident)
            .options(selectinload(SourceIncident.urls))
            .where(SourceIncident.relevance_status == "pending")
            .where(SourceIncident.source_group.in_(_CLASSIFIED_SOURCE_GROUPS))
            .where(SourceIncident.is_deleted.is_(False))
            .order_by(SourceIncident.collected_at.asc())
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(session.execute(stmt).scalars().all())

    def _count_pending(self, session: Session) -> int:
        stmt = (
            select(SourceIncident.id)
            .where(SourceIncident.relevance_status == "pending")
            .where(SourceIncident.source_group.in_(_CLASSIFIED_SOURCE_GROUPS))
            .where(SourceIncident.is_deleted.is_(False))
            .limit(1)
        )
        # We only need to know whether *any* remain to decide on continuation.
        return 1 if session.execute(stmt).first() is not None else 0

    # -- LLM ----------------------------------------------------------------
    def _classify_titles(self, items: list[tuple[int, str, str]]) -> dict[int, dict]:
        """Send one batched prompt; return {idx: {edu_cyber, confidence, reason}}.

        Raises on transport/parse failure so the task retries (rows stay pending).
        Titles the model omits from an otherwise-valid response are simply absent
        from the returned dict; the caller fails those open to ``relevant``.
        """
        lines = []
        for idx, title, snippet in items:
            snippet = (snippet or "").strip()[:_SNIPPET_MAX_CHARS]
            line = f"{idx}. {title}"
            if snippet:
                line += f" — {snippet}"
            lines.append(line)
        user_prompt = (
            "Classify each numbered news item.\n\n" + "\n".join(lines)
        )
        raw = self.llm_client.extract_json(
            system_prompt=_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            schema=_RESULT_SCHEMA,
            max_retries=2,
        )
        data = json.loads(raw)
        verdicts: dict[int, dict] = {}
        for entry in data.get("results", []):
            if not isinstance(entry, dict) or "idx" not in entry:
                continue
            try:
                idx = int(entry["idx"])
            except (TypeError, ValueError):
                continue
            conf = entry.get("confidence", 0.0)
            try:
                conf = max(0.0, min(1.0, float(conf)))
            except (TypeError, ValueError):
                conf = 0.0
            verdicts[idx] = {
                "edu_cyber": bool(entry.get("edu_cyber", False)),
                "confidence": conf,
                "reason": str(entry.get("reason", ""))[:500],
            }
        return verdicts

    def _is_relevant(self, verdict: dict) -> bool:
        # Keep on positive OR on uncertain negative; drop only confident negatives.
        if verdict["edu_cyber"]:
            return True
        return verdict["confidence"] < self.drop_confidence

    # -- entrypoint ---------------------------------------------------------
    def run_batch(self, session: Session, *, current_task_id: Optional[object] = None) -> dict:
        rows = self._lease_pending_rows(session)
        if not rows:
            return {
                "classified": 0,
                "relevant": 0,
                "irrelevant": 0,
                "failed_open": 0,
                "unique_titles": 0,
                "pending_remaining": 0,
            }

        # Group rows by normalized title so each distinct title costs one verdict.
        groups: dict[str, list[SourceIncident]] = {}
        for row in rows:
            groups.setdefault(_normalize_title(row.raw_title), []).append(row)

        # Blank-title groups can't be judged from a title — fail open to relevant.
        blank_rows = groups.pop("", [])

        keyed = list(groups.items())  # [(norm_title, [rows...]), ...]
        items: list[tuple[int, str, str]] = []
        for idx, (_norm, grp) in enumerate(keyed, start=1):
            rep = grp[0]
            items.append((idx, (rep.raw_title or "").strip(), rep.raw_subtitle or ""))

        verdicts = self._classify_titles(items) if items else {}

        now = datetime.now(timezone.utc)
        counts = {"relevant": 0, "irrelevant": 0, "failed_open": 0}

        def _apply(grp: list[SourceIncident], *, relevant: bool, score, reason: str, failed_open: bool):
            status = "relevant" if relevant else "irrelevant"
            for row in grp:
                row.relevance_status = status
                row.title_relevance_score = score
                row.title_relevance_reason = reason
                row.title_classified_at = now
                if relevant:
                    self.intake_service.ensure_initial_processing_task(session, row)
            if failed_open:
                counts["failed_open"] += len(grp)
            counts["relevant" if relevant else "irrelevant"] += len(grp)

        for idx, (_norm, grp) in enumerate(keyed, start=1):
            verdict = verdicts.get(idx)
            if verdict is None:
                # Model omitted this item from a valid response — keep it.
                _apply(grp, relevant=True, score=None, reason="classifier_no_verdict_fail_open", failed_open=True)
                continue
            relevant = self._is_relevant(verdict)
            _apply(
                grp,
                relevant=relevant,
                score=verdict["confidence"],
                reason=verdict["reason"] or ("kept" if relevant else "dropped"),
                failed_open=False,
            )

        for grp in (blank_rows,):
            if grp:
                _apply(grp, relevant=True, score=None, reason="no_title_fail_open", failed_open=True)

        pending_remaining = self._count_pending(session)
        if pending_remaining:
            # Seed the next sweep (excluding our own still-leased task from the dedup).
            self.intake_service.ensure_classify_sweep_task(
                session, exclude_task_id=current_task_id
            )

        result = {
            "classified": len(rows),
            "relevant": counts["relevant"],
            "irrelevant": counts["irrelevant"],
            "failed_open": counts["failed_open"],
            "unique_titles": len(keyed) + (1 if blank_rows else 0),
            "pending_remaining": pending_remaining,
        }
        logger.info(
            "title_classify_batch",
            extra={
                "classified": result["classified"],
                "relevant": result["relevant"],
                "irrelevant": result["irrelevant"],
                "failed_open": result["failed_open"],
                "unique_titles": result["unique_titles"],
            },
        )
        return result
