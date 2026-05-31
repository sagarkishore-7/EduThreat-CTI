"""Repository helpers for canonical incident tables."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, Sequence

from sqlalchemy import Select, case, extract, func, or_, select
from sqlalchemy.orm import Session, selectinload

from src.edu_cti_v2.normalization import normalize_ransomware_family, normalize_threat_actor_name
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
    def build_list_membership_details_stmt(canonical_incident_id: str) -> Select:
        return (
            select(
                CanonicalMembership,
                SourceIncident.id.label("source_incident_id"),
                SourceIncident.source_name,
                SourceIncident.source_group,
                SourceIncident.collected_at,
                SourceIncident.source_published_at,
                SourceIncident.raw_title,
                SourceIncident.raw_subtitle,
                SourceIncident.raw_victim_name,
                SourceIncident.raw_institution_name,
                SourceIncident.raw_institution_type,
                SourceIncident.raw_country,
                SourceIncident.raw_region,
                SourceIncident.raw_city,
            )
            .join(SourceIncident, SourceIncident.id == CanonicalMembership.source_incident_id)
            .where(CanonicalMembership.canonical_incident_id == canonical_incident_id)
            .order_by(CanonicalMembership.is_primary_member.desc(), CanonicalMembership.matched_at.asc())
        )

    @staticmethod
    def build_list_source_urls_stmt(source_incident_ids: Sequence[str]) -> Select:
        return (
            select(SourceIncidentUrl)
            .where(SourceIncidentUrl.source_incident_id.in_(list(source_incident_ids)))
            .order_by(
                SourceIncidentUrl.source_incident_id.asc(),
                SourceIncidentUrl.is_resolved_primary.desc(),
                SourceIncidentUrl.is_primary_from_source.desc(),
                SourceIncidentUrl.created_at.asc(),
            )
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
                func.sum(
                    case(
                        (CanonicalIncident.ransomware_family.is_not(None), 1),
                        else_=0,
                    )
                ).label("incidents_with_ransomware"),
                func.sum(
                    case(
                        (CanonicalIncident.attack_category.ilike("data_breach%"), 1),
                        else_=0,
                    )
                ).label("incidents_with_data_breach"),
                func.count(func.distinct(CanonicalIncident.country_code)).label("countries_affected"),
                func.count(func.distinct(CanonicalIncident.threat_actor_name)).label("unique_threat_actors"),
                func.count(func.distinct(CanonicalIncident.ransomware_family)).label("unique_ransomware_families"),
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
    def build_ransomware_breakdown_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> Select:
        return (
            select(
                CanonicalIncident.ransomware_family,
                func.count(CanonicalIncident.id).label("incident_count"),
            )
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.ransomware_family.is_not(None))
            .group_by(CanonicalIncident.ransomware_family)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.ransomware_family.asc())
            .limit(limit)
        )

    @staticmethod
    def build_threat_actor_breakdown_stmt(
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 20,
    ) -> Select:
        return (
            select(
                CanonicalIncident.threat_actor_name.label("name"),
                func.count(CanonicalIncident.id).label("incident_count"),
                func.array_remove(
                    func.array_agg(func.distinct(CanonicalIncident.country)),
                    None,
                ).label("countries_targeted"),
                func.array_remove(
                    func.array_agg(func.distinct(CanonicalIncident.ransomware_family)),
                    None,
                ).label("ransomware_families"),
                func.min(CanonicalIncident.incident_date).label("first_seen"),
                func.max(CanonicalIncident.incident_date).label("last_seen"),
            )
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.threat_actor_name.is_not(None))
            .group_by(CanonicalIncident.threat_actor_name)
            .order_by(func.count(CanonicalIncident.id).desc(), CanonicalIncident.threat_actor_name.asc())
            .limit(limit)
        )

    @staticmethod
    def build_filter_countries_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(func.distinct(CanonicalIncident.country).label("country"))
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.country.is_not(None))
            .order_by(CanonicalIncident.country.asc())
        )

    @staticmethod
    def build_filter_attack_categories_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(func.distinct(CanonicalIncident.attack_category).label("attack_category"))
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.attack_category.is_not(None))
            .order_by(CanonicalIncident.attack_category.asc())
        )

    @staticmethod
    def build_filter_ransomware_families_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(func.distinct(CanonicalIncident.ransomware_family).label("ransomware_family"))
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.ransomware_family.is_not(None))
            .order_by(CanonicalIncident.ransomware_family.asc())
        )

    @staticmethod
    def build_filter_threat_actors_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(func.distinct(CanonicalIncident.threat_actor_name).label("threat_actor_name"))
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.threat_actor_name.is_not(None))
            .order_by(CanonicalIncident.threat_actor_name.asc())
        )

    @staticmethod
    def build_filter_institution_types_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        return (
            select(func.distinct(CanonicalIncident.institution_type).label("institution_type"))
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.institution_type.is_not(None))
            .order_by(CanonicalIncident.institution_type.asc())
        )

    @staticmethod
    def build_filter_years_stmt(
        *,
        statuses: Sequence[str] = ("open",),
    ) -> Select:
        year_expr = extract("year", CanonicalIncident.incident_date).label("incident_year")
        return (
            select(year_expr).distinct()
            .where(CanonicalIncident.status.in_(list(statuses)))
            .where(CanonicalIncident.incident_date.is_not(None))
            .order_by(year_expr.desc())
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

    def get_by_canonical_key(self, session: Session, canonical_key: str) -> CanonicalIncident | None:
        stmt = select(CanonicalIncident).where(CanonicalIncident.canonical_key == canonical_key)
        return session.execute(stmt).scalar_one_or_none()

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

    def list_membership_details(
        self,
        session: Session,
        canonical_incident_id: str,
    ) -> list[dict[str, object]]:
        stmt = self.build_list_membership_details_stmt(canonical_incident_id)
        rows = session.execute(stmt).all()
        source_incident_ids = [
            data["source_incident_id"]
            for data in (row._mapping for row in rows)
            if data["source_incident_id"] is not None
        ]
        source_url_map = self.list_source_urls_for_incidents(session, source_incident_ids)
        details: list[dict[str, object]] = []
        for row in rows:
            data = row._mapping
            membership = data[CanonicalMembership]
            source_incident_id = str(data["source_incident_id"]) if data["source_incident_id"] else None
            details.append(
                {
                    "membership": membership,
                    "source_incident_id": source_incident_id,
                    "source_name": data["source_name"],
                    "source_group": data["source_group"],
                    "collected_at": data["collected_at"].isoformat() if data["collected_at"] else None,
                    "source_published_at": (
                        data["source_published_at"].isoformat() if data["source_published_at"] else None
                    ),
                    "raw_title": data["raw_title"],
                    "raw_subtitle": data["raw_subtitle"],
                    "raw_victim_name": data["raw_victim_name"],
                    "raw_institution_name": data["raw_institution_name"],
                    "raw_institution_type": data["raw_institution_type"],
                    "raw_country": data["raw_country"],
                    "raw_region": data["raw_region"],
                    "raw_city": data["raw_city"],
                    "source_urls": source_url_map.get(source_incident_id, []),
                }
            )
        return details

    def list_source_urls_for_incidents(
        self,
        session: Session,
        source_incident_ids: Sequence[str],
    ) -> dict[str, list[dict[str, object]]]:
        if not source_incident_ids:
            return {}

        stmt = self.build_list_source_urls_stmt(source_incident_ids)
        url_rows = session.execute(stmt).scalars().all()
        grouped: dict[str, list[dict[str, object]]] = {}
        for row in url_rows:
            source_incident_id = str(row.source_incident_id)
            grouped.setdefault(source_incident_id, []).append(
                {
                    "url": row.url,
                    "resolved_url": row.resolved_url,
                    "url_kind": row.url_kind,
                    "is_wrapper": bool(row.is_wrapper),
                    "is_primary_from_source": bool(row.is_primary_from_source),
                    "is_resolved_primary": bool(row.is_resolved_primary),
                }
            )
        return grouped

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
            "incidents_with_ransomware": int(row.incidents_with_ransomware or 0),
            "incidents_with_data_breach": int(row.incidents_with_data_breach or 0),
            "countries_affected": int(row.countries_affected or 0),
            "unique_threat_actors": int(row.unique_threat_actors or 0),
            "unique_ransomware_families": int(row.unique_ransomware_families or 0),
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

    def get_ransomware_breakdown(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 10,
    ) -> list[dict[str, object]]:
        stmt = self.build_ransomware_breakdown_stmt(statuses=statuses, limit=limit)
        merged: dict[str, int] = {}
        for row in session.execute(stmt).all():
            canonical = normalize_ransomware_family(row.ransomware_family)
            if not canonical:
                continue
            merged[canonical] = merged.get(canonical, 0) + int(row.incident_count or 0)
        return [
            {
                "ransomware_family": family,
                "incident_count": count,
            }
            for family, count in sorted(merged.items(), key=lambda item: (-item[1], item[0]))
        ][:limit]

    def get_threat_actor_breakdown(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
        limit: int = 20,
    ) -> dict[str, object]:
        stmt = self.build_threat_actor_breakdown_stmt(statuses=statuses, limit=limit)
        rows = list(session.execute(stmt).all())
        merged: dict[str, dict[str, object]] = {}
        all_countries: set[str] = set()
        total_incidents = 0

        for row in rows:
            canonical_name = normalize_threat_actor_name(row.name)
            if not canonical_name:
                continue
            countries = sorted({country for country in (row.countries_targeted or []) if country})
            ransomware_families = sorted(
                {
                    family
                    for family in (
                        normalize_ransomware_family(item)
                        for item in (row.ransomware_families or [])
                    )
                    if family
                }
            )
            all_countries.update(countries)
            incident_count = int(row.incident_count or 0)
            total_incidents += incident_count
            existing = merged.get(canonical_name)
            if existing is None:
                merged[canonical_name] = {
                    "name": canonical_name,
                    "incident_count": incident_count,
                    "countries_targeted": set(countries),
                    "ransomware_families": set(ransomware_families),
                    "first_seen": row.first_seen,
                    "last_seen": row.last_seen,
                }
                continue
            existing["incident_count"] = int(existing["incident_count"]) + incident_count
            existing["countries_targeted"].update(countries)
            existing["ransomware_families"].update(ransomware_families)
            if row.first_seen and (existing["first_seen"] is None or row.first_seen < existing["first_seen"]):
                existing["first_seen"] = row.first_seen
            if row.last_seen and (existing["last_seen"] is None or row.last_seen > existing["last_seen"]):
                existing["last_seen"] = row.last_seen

        threat_actors: list[dict[str, object]] = []
        for item in sorted(
            merged.values(),
            key=lambda payload: (-int(payload["incident_count"]), str(payload["name"])),
        )[:limit]:
            threat_actors.append(
                {
                    "name": item["name"],
                    "incident_count": int(item["incident_count"]),
                    "countries_targeted": sorted(item["countries_targeted"]),
                    "ransomware_families": sorted(item["ransomware_families"]),
                    "first_seen": item["first_seen"].isoformat() if item["first_seen"] else None,
                    "last_seen": item["last_seen"].isoformat() if item["last_seen"] else None,
                }
            )

        return {
            "threat_actors": threat_actors,
            "total": len(threat_actors),
            "returned": len(threat_actors),
            "total_incidents": total_incidents,
            "countries_targeted_total": len(all_countries),
        }

    def get_filter_options(
        self,
        session: Session,
        *,
        statuses: Sequence[str] = ("open",),
    ) -> dict[str, object]:
        countries = [
            row.country
            for row in session.execute(self.build_filter_countries_stmt(statuses=statuses)).all()
            if row.country
        ]
        attack_categories = [
            row.attack_category
            for row in session.execute(self.build_filter_attack_categories_stmt(statuses=statuses)).all()
            if row.attack_category
        ]
        ransomware_families = [
            row.ransomware_family
            for row in session.execute(self.build_filter_ransomware_families_stmt(statuses=statuses)).all()
            if row.ransomware_family
        ]
        threat_actors = [
            row.threat_actor_name
            for row in session.execute(self.build_filter_threat_actors_stmt(statuses=statuses)).all()
            if row.threat_actor_name
        ]
        institution_types = [
            row.institution_type
            for row in session.execute(self.build_filter_institution_types_stmt(statuses=statuses)).all()
            if row.institution_type
        ]

        def _year_value(row: object) -> Optional[int]:
            if hasattr(row, "incident_year"):
                value = getattr(row, "incident_year")
            elif isinstance(row, (tuple, list)) and row:
                value = row[0]
            else:
                value = None
            return int(value) if value is not None else None

        years = [
            year
            for row in session.execute(self.build_filter_years_stmt(statuses=statuses)).all()
            if (year := _year_value(row)) is not None
        ]
        return {
            "countries": countries,
            "attack_categories": attack_categories,
            "ransomware_families": sorted(
                {
                    family
                    for family in (normalize_ransomware_family(value) for value in ransomware_families)
                    if family
                }
            ),
            "threat_actors": sorted(
                {
                    actor
                    for actor in (normalize_threat_actor_name(value) for value in threat_actors)
                    if actor
                }
            ),
            "institution_types": institution_types,
            "years": years,
        }

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
