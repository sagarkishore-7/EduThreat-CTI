"""Production campaign-correlation service for v2."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.edu_cti.analysis.campaign_correlation import (
    CampaignCandidate,
    CampaignEdge,
    CampaignEvidenceItem as AnalysisEvidenceItem,
    CampaignMembership as AnalysisMembership,
    build_campaign_outputs,
    build_candidate_edges,
    build_evidence_items,
    build_profiles,
)
from src.edu_cti_v2.models import PipelineRun, PipelineTask
from src.edu_cti_v2.repositories import CampaignRepository, PipelineRunRepository, PipelineTaskRepository
from src.edu_cti_v2.repositories.campaigns import (
    serialize_campaign,
    serialize_evidence_item,
    serialize_membership,
)


CAMPAIGN_CORRELATION_VERSION = "campaign_corr_v1"
PUBLIC_CAMPAIGN_STATUSES = ("analyst_reviewed",)
ADMIN_CAMPAIGN_STATUSES = ("candidate", "analyst_reviewed", "suppressed")


class V2CampaignService:
    """Build, persist, review, and read campaign hypotheses."""

    def __init__(
        self,
        *,
        campaign_repository: Optional[CampaignRepository] = None,
        pipeline_run_repository: Optional[PipelineRunRepository] = None,
        pipeline_task_repository: Optional[PipelineTaskRepository] = None,
    ) -> None:
        self.campaign_repository = campaign_repository or CampaignRepository()
        self.pipeline_run_repository = pipeline_run_repository or PipelineRunRepository()
        self.pipeline_task_repository = pipeline_task_repository or PipelineTaskRepository()

    def fetch_campaign_rows(
        self,
        session: Session,
        *,
        include_excluded: bool = True,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        status_filter = "" if include_excluded else "and ci.status = 'open'"
        limit_clause = "limit :limit" if limit else ""
        query = text(
            f"""
            select
                ci.id::text as canonical_incident_id,
                ci.status as canonical_status,
                ci.institution_name,
                ci.institution_type,
                ci.vendor_name,
                ci.country,
                ci.country_code,
                ci.incident_date,
                ci.date_precision,
                ci.source_published_at,
                ci.attack_category,
                ci.attack_vector,
                ci.threat_actor_name,
                ci.ransomware_family,
                cm.source_incident_id::text as source_incident_id,
                cm.match_type,
                cm.is_primary_member,
                si.source_name,
                si.source_group,
                si.raw_title,
                si.raw_victim_name,
                si.raw_notes,
                si.source_published_at as source_row_published_at,
                se.manual_review_required,
                se.manual_review_reason,
                ad.id::text as article_document_id,
                ad.title as article_title,
                ad.publish_date as article_publish_date,
                left(ad.content_text, 8000) as content_text,
                ad.content_hash,
                ce.canonical_projection,
                ce.analytics_projection,
                (
                    select string_agg(distinct coalesce(nullif(siu.resolved_url, ''), siu.url), ' | ')
                    from source_incident_urls siu
                    where siu.source_incident_id = si.id
                ) as article_url
            from canonical_incidents ci
            join canonical_memberships cm on cm.canonical_incident_id = ci.id
            join source_incidents si on si.id = cm.source_incident_id
            left join source_enrichments se on se.source_incident_id = si.id
            left join article_documents ad on ad.id = se.article_document_id
            left join canonical_enrichments ce on ce.canonical_incident_id = ci.id
            where ci.status in ('open', 'excluded')
              and coalesce(ci.is_education_related, true) is true
              {status_filter}
            order by ci.incident_date nulls last, ci.id, cm.is_primary_member desc
            {limit_clause}
            """
        )
        return [dict(row) for row in session.execute(query, {"limit": limit}).mappings().all()]

    def build_candidates(
        self,
        rows: Sequence[dict[str, Any]],
    ) -> tuple[list[AnalysisEvidenceItem], list[CampaignEdge], list[CampaignCandidate], list[AnalysisMembership]]:
        evidence_items = build_evidence_items(rows)
        profiles = build_profiles(evidence_items)
        edges = build_candidate_edges(profiles)
        candidates, memberships = build_campaign_outputs(evidence_items, edges)
        return evidence_items, edges, candidates, memberships

    def run_correlation(
        self,
        session: Session,
        *,
        include_excluded: bool = True,
        limit: int | None = None,
        correlation_version: str = CAMPAIGN_CORRELATION_VERSION,
    ) -> dict[str, Any]:
        rows = self.fetch_campaign_rows(session, include_excluded=include_excluded, limit=limit)
        evidence_items, edges, candidates, memberships = self.build_candidates(rows)
        persisted = self.persist_outputs(
            session,
            evidence_items=evidence_items,
            edges=edges,
            candidates=candidates,
            memberships=memberships,
            correlation_version=correlation_version,
        )
        return {
            "correlated": True,
            "correlation_version": correlation_version,
            "rows": len(rows),
            "evidence_items": len(evidence_items),
            "edges": len(edges),
            "campaign_candidates": len(candidates),
            "campaign_memberships": len(memberships),
            **persisted,
        }

    def enqueue_correlation(
        self,
        session: Session,
        *,
        worker_id: str = "admin-v2-campaigns",
        include_excluded: bool = True,
        limit: int | None = None,
    ) -> dict[str, Any]:
        run = PipelineRun(
            run_type="campaign_correlation",
            status="pending",
            service_name="v2-campaign-correlation",
            params={
                "worker_id": worker_id,
                "include_excluded": include_excluded,
                "limit": limit,
                "correlation_version": CAMPAIGN_CORRELATION_VERSION,
            },
            result={},
        )
        if run.id is None:
            run.id = uuid4()
        self.pipeline_run_repository.add(session, run)
        task = PipelineTask(
            run_id=run.id,
            task_type="campaign_correlate",
            target_table="campaigns",
            target_id=None,
            status="queued",
            priority=150,
            payload={
                "include_excluded": include_excluded,
                "limit": limit,
                "correlation_version": CAMPAIGN_CORRELATION_VERSION,
            },
            result={},
            available_at=datetime.now(timezone.utc),
            attempt_count=0,
            max_attempts=2,
        )
        if task.id is None:
            task.id = uuid4()
        self.pipeline_task_repository.enqueue(session, task)
        return {
            "run_id": str(run.id),
            "task_id": str(task.id),
            "status": "queued",
            "task_type": "campaign_correlate",
        }

    def persist_outputs(
        self,
        session: Session,
        *,
        evidence_items: Sequence[AnalysisEvidenceItem],
        edges: Sequence[CampaignEdge],
        candidates: Sequence[CampaignCandidate],
        memberships: Sequence[AnalysisMembership],
        correlation_version: str,
    ) -> dict[str, int]:
        correlated_at = datetime.now(timezone.utc)
        candidate_by_id = {candidate.campaign_id: candidate for candidate in candidates}
        memberships_by_campaign: dict[str, list[AnalysisMembership]] = {}
        for membership in memberships:
            memberships_by_campaign.setdefault(membership.campaign_id, []).append(membership)
        evidence_by_canonical: dict[str, list[AnalysisEvidenceItem]] = {}
        for item in evidence_items:
            evidence_by_canonical.setdefault(item.canonical_incident_id, []).append(item)

        membership_reasons = self._membership_reasons(edges, memberships_by_campaign)
        for candidate in candidates:
            payload = asdict(candidate)
            payload["correlation_version"] = correlation_version
            payload["status"] = "candidate"
            payload["metadata"] = {
                "source": "deterministic_campaign_correlation",
                "llm_adjudicated": False,
            }
            self.campaign_repository.upsert_campaign(session, payload, correlated_at=correlated_at)

        persisted_memberships = 0
        for membership in memberships:
            payload = asdict(membership)
            payload["reasons"] = membership_reasons.get(
                (membership.campaign_id, membership.canonical_incident_id),
                [],
            )
            payload["metadata"] = {
                "source": "deterministic_campaign_correlation",
                "campaign_type": candidate_by_id[membership.campaign_id].campaign_type,
            }
            self.campaign_repository.upsert_membership(session, payload)
            persisted_memberships += 1

        evidence_rows = self._evidence_rows_for_memberships(memberships, evidence_by_canonical)
        persisted_evidence = self.campaign_repository.replace_evidence_items(
            session,
            [candidate.campaign_id for candidate in candidates],
            evidence_rows,
        )
        persisted_signatures = 0
        for candidate in candidates:
            if candidate.member_count < 2 or candidate.confidence < 0.65:
                continue
            self.campaign_repository.upsert_signature(
                session,
                {
                    "id": f"signature_{candidate.campaign_id}",
                    "campaign_id": candidate.campaign_id,
                    "status": "candidate",
                    "correlation_version": correlation_version,
                    "signature_payload": self._signature_payload(candidate),
                },
            )
            persisted_signatures += 1

        return {
            "persisted_campaigns": len(candidates),
            "persisted_memberships": persisted_memberships,
            "persisted_evidence_items": persisted_evidence,
            "persisted_signatures": persisted_signatures,
        }

    def list_campaigns(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = PUBLIC_CAMPAIGN_STATUSES,
        campaign_type: str | None = None,
        vendor: str | None = None,
        platform: str | None = None,
        actor: str | None = None,
        cve: str | None = None,
        min_confidence: float | None = None,
        q: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        campaigns, total = self.campaign_repository.list_campaigns(
            session,
            statuses=statuses,
            campaign_type=campaign_type,
            vendor=vendor,
            platform=platform,
            actor=actor,
            cve=cve,
            min_confidence=min_confidence,
            q=q,
            limit=limit,
            offset=offset,
        )
        return {
            "items": [serialize_campaign(campaign) for campaign in campaigns],
            "meta": {
                "limit": limit,
                "offset": offset,
                "returned": len(campaigns),
                "total": total,
                "statuses": list(statuses),
                "campaign_type": campaign_type,
                "vendor": vendor,
                "platform": platform,
                "actor": actor,
                "cve": cve,
                "min_confidence": min_confidence,
                "q": q,
            },
        }

    def get_campaign_detail(
        self,
        session: Session,
        campaign_id: str,
        *,
        statuses: Sequence[str] | None = PUBLIC_CAMPAIGN_STATUSES,
        member_limit: int = 500,
        evidence_limit: int = 1000,
    ) -> dict[str, Any] | None:
        campaign = self.campaign_repository.get_campaign(session, campaign_id, statuses=statuses)
        if campaign is None:
            return None
        memberships = self.campaign_repository.list_memberships(session, campaign_id, limit=member_limit)
        evidence = self.campaign_repository.list_evidence(session, campaign_id, limit=evidence_limit)
        return {
            "campaign": serialize_campaign(campaign),
            "memberships": [serialize_membership(membership) for membership in memberships],
            "evidence_items": [serialize_evidence_item(item) for item in evidence],
        }

    def get_campaign_graph(
        self,
        session: Session,
        campaign_id: str,
        *,
        statuses: Sequence[str] | None = PUBLIC_CAMPAIGN_STATUSES,
        member_limit: int = 250,
    ) -> dict[str, Any] | None:
        detail = self.get_campaign_detail(
            session,
            campaign_id,
            statuses=statuses,
            member_limit=member_limit,
            evidence_limit=member_limit * 4,
        )
        if detail is None:
            return None
        campaign = detail["campaign"]
        memberships = detail["memberships"]
        evidence_items = detail["evidence_items"]
        evidence_count_by_incident: dict[str, int] = {}
        for item in evidence_items:
            evidence_count_by_incident[item["canonical_incident_id"]] = (
                evidence_count_by_incident.get(item["canonical_incident_id"], 0) + 1
            )

        campaign_node_id = f"campaign:{campaign_id}"
        nodes: list[dict[str, Any]] = [
            {
                "id": campaign_node_id,
                "type": "campaign",
                "label": campaign["campaign_name"],
                "size": max(22, min(60, 18 + int(campaign["member_count"] or 0))),
                "confidence": campaign["confidence"],
                "status": campaign["status"],
                "metadata": campaign,
            }
        ]
        edges_out: list[dict[str, Any]] = []
        seen_nodes = {campaign_node_id}

        def add_anchor(node_type: str, value: str) -> None:
            node_id = f"{node_type}:{value.casefold()}"
            if node_id not in seen_nodes:
                seen_nodes.add(node_id)
                nodes.append(
                    {
                        "id": node_id,
                        "type": node_type,
                        "label": value,
                        "size": 18,
                        "metadata": {},
                    }
                )
            edges_out.append(
                {
                    "source": campaign_node_id,
                    "target": node_id,
                    "type": f"campaign_{node_type}",
                    "confidence": campaign["confidence"],
                    "reasons": [f"campaign_{node_type}"],
                    "evidence_count": 0,
                    "review_status": campaign["status"],
                }
            )

        for vendor in campaign["vendors"]:
            add_anchor("vendor", vendor)
        for platform in campaign["platforms"]:
            add_anchor("platform", platform)
        for actor in campaign["actors"]:
            add_anchor("actor", actor)
        for cve in campaign["cves"]:
            add_anchor("cve_or_product", cve)

        for membership in memberships:
            node_id = f"institution:{membership['canonical_incident_id']}"
            nodes.append(
                {
                    "id": node_id,
                    "type": "institution",
                    "label": membership["victim_name"] or membership["canonical_incident_id"],
                    "size": max(12, min(30, 10 + int((membership["confidence"] or 0) * 18))),
                    "confidence": membership["confidence"],
                    "status": membership["review_status"],
                    "metadata": membership,
                }
            )
            edges_out.append(
                {
                    "source": campaign_node_id,
                    "target": node_id,
                    "type": membership["role"],
                    "confidence": membership["confidence"],
                    "reasons": membership["reasons"],
                    "evidence_count": evidence_count_by_incident.get(membership["canonical_incident_id"], 0),
                    "review_status": membership["review_status"],
                }
            )

        return {
            "campaign": campaign,
            "nodes": nodes,
            "edges": edges_out,
            "meta": {
                "member_limit": member_limit,
                "returned_members": len(memberships),
                "returned_evidence_items": len(evidence_items),
            },
        }

    def update_campaign_review(
        self,
        session: Session,
        campaign_id: str,
        *,
        status: str | None = None,
        campaign_name: str | None = None,
        analyst_summary: str | None = None,
        analyst_notes: str | None = None,
    ) -> dict[str, Any] | None:
        campaign = self.campaign_repository.update_campaign_review(
            session,
            campaign_id,
            status=status,
            campaign_name=campaign_name,
            analyst_summary=analyst_summary,
            analyst_notes=analyst_notes,
        )
        return serialize_campaign(campaign) if campaign else None

    def update_membership_review(
        self,
        session: Session,
        campaign_id: str,
        canonical_incident_id: str,
        *,
        review_status: str,
        role: str | None = None,
    ) -> dict[str, Any] | None:
        membership = self.campaign_repository.update_membership_review(
            session,
            campaign_id,
            canonical_incident_id,
            review_status=review_status,
            role=role,
        )
        return serialize_membership(membership) if membership else None

    @staticmethod
    def _membership_reasons(
        edges: Sequence[CampaignEdge],
        memberships_by_campaign: dict[str, list[AnalysisMembership]],
    ) -> dict[tuple[str, str], list[str]]:
        reasons: dict[tuple[str, str], set[str]] = {}
        for campaign_id, campaign_memberships in memberships_by_campaign.items():
            member_ids = {membership.canonical_incident_id for membership in campaign_memberships}
            for edge in edges:
                if edge.from_canonical_incident_id not in member_ids or edge.to_canonical_incident_id not in member_ids:
                    continue
                for incident_id in (edge.from_canonical_incident_id, edge.to_canonical_incident_id):
                    reasons.setdefault((campaign_id, incident_id), set()).update(edge.reasons)
        return {key: sorted(value) for key, value in reasons.items()}

    @staticmethod
    def _evidence_rows_for_memberships(
        memberships: Sequence[AnalysisMembership],
        evidence_by_canonical: dict[str, list[AnalysisEvidenceItem]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for membership in memberships:
            for item in evidence_by_canonical.get(membership.canonical_incident_id, []):
                evidence_id = hashlib.sha1(
                    f"{membership.campaign_id}:{item.evidence_item_id}".encode("utf-8")
                ).hexdigest()[:24]
                rows.append(
                    {
                        "id": evidence_id,
                        "campaign_id": membership.campaign_id,
                        "canonical_incident_id": item.canonical_incident_id,
                        "source_incident_id": item.source_incident_id,
                        "article_document_id": item.article_document_id,
                        "source_url": item.source_url,
                        "source_title": item.source_title,
                        "article_title": item.article_title,
                        "evidence_quotes": item.evidence_quotes,
                        "vendors": item.vendors,
                        "platforms": item.platforms,
                        "actors": item.actors,
                        "cves": item.cves,
                        "evidence_payload": asdict(item),
                    }
                )
        return rows

    @staticmethod
    def _signature_payload(candidate: CampaignCandidate) -> dict[str, Any]:
        terms = []
        for values in (
            candidate.vendors,
            candidate.platforms,
            candidate.actors,
            candidate.cves,
            candidate.campaign_names,
        ):
            for value in values:
                if value not in terms:
                    terms.append(value)
        return {
            "campaign_name": candidate.campaign_name,
            "campaign_type": candidate.campaign_type,
            "confidence": candidate.confidence,
            "date_window": {
                "start_date": candidate.first_seen_date,
                "end_date": candidate.last_seen_date,
            },
            "required_any_terms": terms,
            "vendors": candidate.vendors,
            "platforms": candidate.platforms,
            "actors": candidate.actors,
            "cves": candidate.cves,
            "negative_terms": ["trend report", "best practices", "state of cybersecurity", "roundup"],
        }
