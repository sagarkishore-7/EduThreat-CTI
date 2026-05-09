"""Repository helpers for canonical incident tables."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Sequence

from sqlalchemy import Select, case, func, or_, select
from sqlalchemy.orm import Session, selectinload

from src.edu_cti_v2.models import (
    ArticleDocument,
    CanonicalEnrichment,
    CanonicalIncident,
    CanonicalMembership,
    CanonicalTimelineEvent,
    SourceEnrichment,
    SourceIncident,
    SourceIncidentUrl,
)


class CanonicalIncidentRepository:
    """Repository boundary for canonical incident and membership access."""

    @staticmethod
    def _severity_sort_rank():
        return case(
            (CanonicalIncident.severity == "critical", 4),
            (CanonicalIncident.severity == "high", 3),
            (CanonicalIncident.severity == "medium", 2),
            (CanonicalIncident.severity == "low", 1),
            else_=0,
        )

    @staticmethod
    def _apply_list_filters(
        stmt: Select,
        *,
        statuses: Sequence[str],
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> Select:
        stmt = stmt.where(CanonicalIncident.status.in_(list(statuses)))

        if search:
            pattern = f"%{search.strip()}%"
            stmt = stmt.where(
                or_(
                    CanonicalIncident.institution_name.ilike(pattern),
                    CanonicalIncident.vendor_name.ilike(pattern),
                    CanonicalIncident.canonical_summary.ilike(pattern),
                    CanonicalIncident.threat_actor_name.ilike(pattern),
                    CanonicalIncident.ransomware_family.ilike(pattern),
                    CanonicalIncident.attack_category.ilike(pattern),
                )
            )
        if country_code:
            stmt = stmt.where(CanonicalIncident.country_code == country_code.upper())
        if attack_category:
            stmt = stmt.where(CanonicalIncident.attack_category == attack_category)
        if institution_type:
            stmt = stmt.where(CanonicalIncident.institution_type == institution_type)
        if severity:
            stmt = stmt.where(CanonicalIncident.severity == severity)
        if is_education_related is not None:
            stmt = stmt.where(CanonicalIncident.is_education_related.is_(is_education_related))
        if has_vendor is True:
            stmt = stmt.where(CanonicalIncident.vendor_name.is_not(None))
        elif has_vendor is False:
            stmt = stmt.where(CanonicalIncident.vendor_name.is_(None))
        if date_from:
            stmt = stmt.where(CanonicalIncident.incident_date.is_not(None))
            stmt = stmt.where(CanonicalIncident.incident_date >= date_from)
        if date_to:
            stmt = stmt.where(CanonicalIncident.incident_date.is_not(None))
            stmt = stmt.where(CanonicalIncident.incident_date <= date_to)
        return stmt

    @staticmethod
    def _apply_list_sort(
        stmt: Select,
        *,
        sort_by: str = "last_seen_at",
        sort_order: str = "desc",
    ) -> Select:
        descending = sort_order.lower() != "asc"
        if sort_by == "incident_date":
            primary = CanonicalIncident.incident_date
            secondary = CanonicalIncident.last_seen_at
        elif sort_by == "created_at":
            primary = CanonicalIncident.created_at
            secondary = CanonicalIncident.last_seen_at
        elif sort_by == "institution_name":
            primary = CanonicalIncident.institution_name
            secondary = CanonicalIncident.last_seen_at
        elif sort_by == "country":
            primary = CanonicalIncident.country
            secondary = CanonicalIncident.last_seen_at
        elif sort_by == "severity":
            primary = CanonicalIncidentRepository._severity_sort_rank()
            secondary = CanonicalIncident.last_seen_at
        else:
            primary = CanonicalIncident.last_seen_at
            secondary = CanonicalIncident.created_at

        if descending:
            return stmt.order_by(primary.desc().nullslast(), secondary.desc().nullslast())
        return stmt.order_by(primary.asc().nullslast(), secondary.asc().nullslast())

    @staticmethod
    def build_get_by_id_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalIncident)
            .options(selectinload(CanonicalIncident.memberships))
            .where(CanonicalIncident.id == canonical_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_get_by_source_incident_stmt(source_incident_id: str) -> Select:
        return (
            select(CanonicalMembership)
            .where(CanonicalMembership.source_incident_id == source_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_list_memberships_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalMembership)
            .where(CanonicalMembership.canonical_incident_id == canonical_incident_id)
            .order_by(CanonicalMembership.is_primary_member.desc(), CanonicalMembership.matched_at.asc())
        )

    @staticmethod
    def build_get_enrichment_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalEnrichment)
            .where(CanonicalEnrichment.canonical_incident_id == canonical_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_selected_source_stmt(canonical_incident_id: str) -> Select:
        return (
            select(
                SourceEnrichment.id.label("source_enrichment_id"),
                SourceIncident.id.label("source_incident_id"),
                SourceIncident.source_name,
                SourceIncident.source_group,
                SourceIncident.raw_title,
                SourceIncident.raw_subtitle,
                ArticleDocument.id.label("article_document_id"),
                ArticleDocument.title.label("article_title"),
                ArticleDocument.author.label("article_author"),
                ArticleDocument.publish_date.label("article_publish_date"),
                SourceIncidentUrl.url.label("article_url"),
                SourceIncidentUrl.resolved_url.label("article_resolved_url"),
            )
            .select_from(CanonicalEnrichment)
            .join(SourceEnrichment, SourceEnrichment.id == CanonicalEnrichment.selected_source_enrichment_id)
            .join(SourceIncident, SourceIncident.id == SourceEnrichment.source_incident_id)
            .outerjoin(ArticleDocument, ArticleDocument.id == SourceEnrichment.article_document_id)
            .outerjoin(SourceIncidentUrl, SourceIncidentUrl.id == ArticleDocument.source_incident_url_id)
            .where(CanonicalEnrichment.canonical_incident_id == canonical_incident_id)
            .limit(1)
        )

    @staticmethod
    def build_list_timeline_stmt(canonical_incident_id: str) -> Select:
        return (
            select(CanonicalTimelineEvent)
            .where(CanonicalTimelineEvent.canonical_incident_id == canonical_incident_id)
            .order_by(CanonicalTimelineEvent.seq_order.asc(), CanonicalTimelineEvent.created_at.asc())
        )

    @staticmethod
    def build_list_recent_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        sort_by: str = "last_seen_at",
        sort_order: str = "desc",
    ) -> Select:
        membership_count = (
            select(func.count(CanonicalMembership.id))
            .where(CanonicalMembership.canonical_incident_id == CanonicalIncident.id)
            .correlate(CanonicalIncident)
            .scalar_subquery()
        )
        stmt = CanonicalIncidentRepository._apply_list_filters(
            select(
                CanonicalIncident,
                CanonicalEnrichment,
                membership_count.label("membership_count"),
            )
            .outerjoin(CanonicalEnrichment, CanonicalEnrichment.canonical_incident_id == CanonicalIncident.id)
            ,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        stmt = CanonicalIncidentRepository._apply_list_sort(
            stmt,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        return (
            stmt
            .offset(offset)
            .limit(limit)
        )

    @staticmethod
    def build_count_recent_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> Select:
        stmt = select(func.count(CanonicalIncident.id))
        return CanonicalIncidentRepository._apply_list_filters(
            stmt,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )

    @staticmethod
    def build_dashboard_rollup_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(
                func.count(CanonicalIncident.id).label("canonical_incident_count"),
                func.count(CanonicalEnrichment.id).label("enriched_canonical_count"),
                func.sum(
                    case(
                        (CanonicalIncident.is_education_related.is_(True), 1),
                        else_=0,
                    )
                ).label("education_related_count"),
            )
            .select_from(CanonicalIncident)
            .outerjoin(CanonicalEnrichment, CanonicalEnrichment.canonical_incident_id == CanonicalIncident.id)
            .where(CanonicalIncident.status.in_(list(statuses)))
        )

    @staticmethod
    def build_country_breakdown_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> Select:
        return (
            select(
                CanonicalIncident.country_code,
                CanonicalIncident.country,
                func.count(CanonicalIncident.id).label("incident_count"),
            )
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.country_code.is_not(None))
            .group_by(CanonicalIncident.country_code, CanonicalIncident.country)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.country.asc())
            .limit(limit)
        )

    @staticmethod
    def build_attack_breakdown_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> Select:
        return (
            select(
                CanonicalIncident.attack_category,
                func.count(CanonicalIncident.id).label("incident_count"),
            )
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.attack_category.is_not(None))
            .group_by(CanonicalIncident.attack_category)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.attack_category.asc())
            .limit(limit)
        )

    @staticmethod
    def build_find_by_url_candidates_stmt(normalized_urls: Sequence[str]) -> Select:
        return (
            select(CanonicalIncident)
            .join(CanonicalMembership, CanonicalMembership.canonical_incident_id == CanonicalIncident.id)
            .join(SourceIncidentUrl, SourceIncidentUrl.source_incident_id == CanonicalMembership.source_incident_id)
            .options(selectinload(CanonicalIncident.memberships))
            .where(SourceIncidentUrl.normalized_url.in_(list(normalized_urls)))
            .distinct()
        )

    @staticmethod
    def build_find_name_date_candidates_stmt(
        *,
        incident_date: Optional[date],
        country_code: Optional[str],
        window_days: int = 14,
    ) -> Select:
        stmt = select(CanonicalIncident).options(selectinload(CanonicalIncident.memberships))
        if country_code:
            stmt = stmt.where(
                or_(
                    CanonicalIncident.country_code == country_code,
                    CanonicalIncident.country_code.is_(None),
                )
            )
        if incident_date:
            start = incident_date - timedelta(days=window_days)
            end = incident_date + timedelta(days=window_days)
            stmt = stmt.where(
                or_(
                    CanonicalIncident.incident_date.between(start, end),
                    CanonicalIncident.incident_date.is_(None),
                )
            )
        return stmt

    @staticmethod
    def build_find_identity_candidates_stmt(
        identities: Sequence[str],
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        cleaned_identities = [value.strip() for value in identities if value and value.strip()]
        stmt = select(CanonicalIncident).options(selectinload(CanonicalIncident.memberships))
        if statuses:
            stmt = stmt.where(CanonicalIncident.status.in_(list(statuses)))
        if not cleaned_identities:
            return stmt.where(CanonicalIncident.id.is_(None))
        return stmt.where(
            or_(
                CanonicalIncident.institution_name.in_(cleaned_identities),
                CanonicalIncident.vendor_name.in_(cleaned_identities),
            )
        )

    @staticmethod
    def build_country_facet_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> Select:
        stmt = select(
            CanonicalIncident.country_code,
            CanonicalIncident.country,
            func.count(CanonicalIncident.id).label("incident_count"),
        )
        stmt = CanonicalIncidentRepository._apply_list_filters(
            stmt,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        return (
            stmt.where(CanonicalIncident.country_code.is_not(None))
            .group_by(CanonicalIncident.country_code, CanonicalIncident.country)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.country.asc())
            .limit(limit)
        )

    @staticmethod
    def build_attack_category_facet_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> Select:
        stmt = select(
            CanonicalIncident.attack_category,
            func.count(CanonicalIncident.id).label("incident_count"),
        )
        stmt = CanonicalIncidentRepository._apply_list_filters(
            stmt,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        return (
            stmt.where(CanonicalIncident.attack_category.is_not(None))
            .group_by(CanonicalIncident.attack_category)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.attack_category.asc())
            .limit(limit)
        )

    @staticmethod
    def build_institution_type_facet_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> Select:
        stmt = select(
            CanonicalIncident.institution_type,
            func.count(CanonicalIncident.id).label("incident_count"),
        )
        stmt = CanonicalIncidentRepository._apply_list_filters(
            stmt,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        return (
            stmt.where(CanonicalIncident.institution_type.is_not(None))
            .group_by(CanonicalIncident.institution_type)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.institution_type.asc())
            .limit(limit)
        )

    @staticmethod
    def build_severity_facet_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> Select:
        stmt = select(
            CanonicalIncident.severity,
            func.count(CanonicalIncident.id).label("incident_count"),
        )
        stmt = CanonicalIncidentRepository._apply_list_filters(
            stmt,
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        return (
            stmt.where(CanonicalIncident.severity.is_not(None))
            .group_by(CanonicalIncident.severity)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.severity.asc())
            .limit(limit)
        )

    @staticmethod
    def build_incident_trend_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        bucket: str = "month",
        limit: int = 24,
    ) -> Select:
        bucket_expr = func.date_trunc(bucket, CanonicalIncident.incident_date).label("bucket_start")
        stmt = CanonicalIncidentRepository._apply_list_filters(
            select(
                bucket_expr,
                func.count(CanonicalIncident.id).label("incident_count"),
            ),
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        return (
            stmt.where(CanonicalIncident.incident_date.is_not(None))
            .group_by(bucket_expr)
            .order_by(bucket_expr.desc())
            .limit(limit)
        )

    def get_by_id(self, session: Session, canonical_incident_id: str) -> CanonicalIncident | None:
        return session.execute(self.build_get_by_id_stmt(canonical_incident_id)).scalar_one_or_none()

    def get_membership_for_source_incident(
        self,
        session: Session,
        source_incident_id: str,
    ) -> CanonicalMembership | None:
        stmt = self.build_get_by_source_incident_stmt(source_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def list_memberships(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> list[CanonicalMembership]:
        stmt = self.build_list_memberships_stmt(canonical_incident_id)
        return list(session.execute(stmt).scalars().all())

    def find_by_url_candidates(
        self,
        session: Session,
        normalized_urls: Sequence[str],
    ) -> list[CanonicalIncident]:
        if not normalized_urls:
            return []
        stmt = self.build_find_by_url_candidates_stmt(normalized_urls)
        return list(session.execute(stmt).scalars().all())

    def find_name_date_candidates(
        self,
        session: Session,
        *,
        incident_date: Optional[date],
        country_code: Optional[str],
        window_days: int = 14,
    ) -> list[CanonicalIncident]:
        stmt = self.build_find_name_date_candidates_stmt(
            incident_date=incident_date,
            country_code=country_code,
            window_days=window_days,
        )
        return list(session.execute(stmt).scalars().all())

    def find_identity_candidates(
        self,
        session: Session,
        identities: Sequence[str],
        *,
        statuses: Sequence[str] = ("open",),
    ) -> list[CanonicalIncident]:
        stmt = self.build_find_identity_candidates_stmt(
            identities,
            statuses=statuses,
        )
        return list(session.execute(stmt).scalars().all())

    def get_enrichment(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> CanonicalEnrichment | None:
        stmt = self.build_get_enrichment_stmt(canonical_incident_id)
        return session.execute(stmt).scalar_one_or_none()

    def get_selected_source_details(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> dict[str, object] | None:
        stmt = self.build_selected_source_stmt(canonical_incident_id)
        row = session.execute(stmt).one_or_none()
        if row is None:
            return None
        data = row._mapping
        return {
            "source_enrichment_id": str(data["source_enrichment_id"]) if data["source_enrichment_id"] else None,
            "source_incident_id": str(data["source_incident_id"]) if data["source_incident_id"] else None,
            "source_name": data["source_name"],
            "source_group": data["source_group"],
            "raw_title": data["raw_title"],
            "raw_subtitle": data["raw_subtitle"],
            "article_document_id": str(data["article_document_id"]) if data["article_document_id"] else None,
            "article_title": data["article_title"],
            "article_author": data["article_author"],
            "article_publish_date": (
                data["article_publish_date"].isoformat() if data["article_publish_date"] else None
            ),
            "article_url": data["article_url"],
            "article_resolved_url": data["article_resolved_url"],
        }

    def list_timeline_events(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> list[CanonicalTimelineEvent]:
        stmt = self.build_list_timeline_stmt(canonical_incident_id)
        return list(session.execute(stmt).scalars().all())

    def list_recent_with_enrichment(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        sort_by: str = "last_seen_at",
        sort_order: str = "desc",
    ):
        stmt = self.build_list_recent_stmt(
            statuses=statuses,
            limit=limit,
            offset=offset,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            sort_by=sort_by,
            sort_order=sort_order,
        )
        return list(session.execute(stmt).all())

    def count_recent(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> int:
        stmt = self.build_count_recent_stmt(
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
        )
        return int(session.execute(stmt).scalar_one() or 0)

    def get_dashboard_rollup(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, int]:
        stmt = self.build_dashboard_rollup_stmt(statuses=statuses)
        row = session.execute(stmt).one()
        return {
            "canonical_incident_count": int(row.canonical_incident_count or 0),
            "enriched_canonical_count": int(row.enriched_canonical_count or 0),
            "education_related_count": int(row.education_related_count or 0),
        }

    def get_country_breakdown(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> list[dict[str, object]]:
        stmt = self.build_country_breakdown_stmt(statuses=statuses, limit=limit)
        return [
            {
                "country_code": row.country_code,
                "country": row.country,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_country_facets(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        stmt = self.build_country_facet_stmt(
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
        return [
            {
                "country_code": row.country_code,
                "country": row.country,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_attack_breakdown(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> list[dict[str, object]]:
        stmt = self.build_attack_breakdown_stmt(statuses=statuses, limit=limit)
        return [
            {
                "attack_category": row.attack_category,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_attack_category_facets(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        stmt = self.build_attack_category_facet_stmt(
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
        return [
            {
                "attack_category": row.attack_category,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_institution_type_facets(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        stmt = self.build_institution_type_facet_stmt(
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
        return [
            {
                "institution_type": row.institution_type,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_severity_facets(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        stmt = self.build_severity_facet_stmt(
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
        )
        return [
            {
                "severity": row.severity,
                "incident_count": int(row.incident_count or 0),
            }
            for row in session.execute(stmt).all()
        ]

    def get_incident_trend(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        search: Optional[str] = None,
        country_code: Optional[str] = None,
        attack_category: Optional[str] = None,
        institution_type: Optional[str] = None,
        severity: Optional[str] = None,
        is_education_related: Optional[bool] = None,
        has_vendor: Optional[bool] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        bucket: str = "month",
        limit: int = 24,
    ) -> list[dict[str, object]]:
        stmt = self.build_incident_trend_stmt(
            statuses=statuses,
            search=search,
            country_code=country_code,
            attack_category=attack_category,
            institution_type=institution_type,
            severity=severity,
            is_education_related=is_education_related,
            has_vendor=has_vendor,
            date_from=date_from,
            date_to=date_to,
            bucket=bucket,
            limit=limit,
        )
        rows = list(session.execute(stmt).all())
        rows.reverse()
        items: list[dict[str, object]] = []
        for row in rows:
            bucket_start = row.bucket_start
            if hasattr(bucket_start, "date"):
                bucket_value = bucket_start.date().isoformat()
            else:
                bucket_value = str(bucket_start)
            items.append(
                {
                    "bucket_start": bucket_value,
                    "incident_count": int(row.incident_count or 0),
                }
            )
        return items

    def add(self, session: Session, canonical_incident: CanonicalIncident) -> CanonicalIncident:
        session.add(canonical_incident)
        return canonical_incident

    def add_membership(self, session: Session, membership: CanonicalMembership) -> CanonicalMembership:
        session.add(membership)
        return membership
