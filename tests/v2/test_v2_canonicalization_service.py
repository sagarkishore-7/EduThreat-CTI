from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import CanonicalIncident, CanonicalMembership, SourceEnrichment, SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.services import V2CanonicalizationService, build_source_projection


def _source_incident(*, event_key: str = "story-1", url: str = "https://example.com/article") -> SourceIncident:
    incident = SourceIncident(
        id=uuid4(),
        source_name="therecord",
        source_group="news",
        source_event_key=event_key,
        collected_at=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        source_published_at=datetime(2026, 5, 8, 8, 0, tzinfo=timezone.utc),
        raw_title="University hit by ransomware",
        raw_institution_name="Penn State University",
        raw_victim_name="Penn State University",
        raw_institution_type="university",
        raw_country="United States",
        raw_region="Pennsylvania",
        raw_city="State College",
        raw_incident_date="2026-05-08",
        raw_date_precision="day",
        raw_status="confirmed",
        raw_attack_hint="ransomware",
        raw_threat_actor="SomeGroup",
        raw_notes="Records affected: 5000",
        source_confidence="high",
        ingest_hash=event_key,
        raw_payload={},
        is_deleted=False,
    )
    incident.urls = [
        SourceIncidentUrl(
            id=uuid4(),
            source_incident_id=incident.id,
            url=url,
            normalized_url=url,
            resolved_url=url,
            url_kind="article",
            is_wrapper=False,
            is_primary_from_source=True,
            is_resolved_primary=True,
            created_at=incident.collected_at,
        )
    ]
    return incident


def _source_enrichment(source_incident: SourceIncident) -> SourceEnrichment:
    return SourceEnrichment(
        id=uuid4(),
        source_incident_id=source_incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "Penn State University",
            "institution_type": "university",
            "country": "United States",
            "country_code": "US",
            "region": "Pennsylvania",
            "city": "State College",
            "incident_date": "2026-05-08",
            "incident_date_precision": "exact",
            "attack_category": "ransomware_encryption",
            "incident_severity": "high",
            "enriched_summary": "Penn State suffered a ransomware incident.",
            "timeline": [
                {
                    "date": "2026-05-08",
                    "date_precision": "day",
                    "event_type": "impact",
                    "event_description": "Systems were encrypted.",
                    "actor_attribution": "SomeGroup",
                }
            ],
            "attack_dynamics": {
                "attack_vector": "phishing_email",
                "ransomware_family": "LockBit",
            },
            "threat_actor_name": "SomeGroup",
        },
        raw_extraction={
            "institution_name": "Penn State University",
            "country_code": "US",
            "attack_category": "ransomware_encryption",
        },
        is_education_related=True,
        enrichment_confidence=0.92,
    )


def test_build_source_projection_maps_typed_enrichment_into_canonical_fields():
    incident = _source_incident()
    enrichment = _source_enrichment(incident)

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "Penn State University"
    assert projection["country_code"] == "US"
    assert projection["attack_vector"] == "phishing_email"
    assert projection["ransomware_family"] == "LockBit"
    assert projection["incident_date"].isoformat() == "2026-05-08"


def test_canonicalization_service_creates_seed_canonical_and_membership():
    canonical_repo = Mock()
    canonical_repo.get_membership_for_source_incident.return_value = None
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.list_memberships.return_value = []

    source_repo = Mock()
    incident = _source_incident()
    source_repo.get_by_id.return_value = incident

    enrichment_repo = Mock()
    enrichment = _source_enrichment(incident)
    enrichment_repo.get_by_source_incident.return_value = enrichment

    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    session.flush.side_effect = None
    session.execute.return_value.scalars.return_value.all.return_value = [enrichment]
    session.query.return_value.filter_by.return_value.delete.return_value = None

    added_canonicals = []
    added_memberships = []

    def _capture_canonical(_session, canonical):
        added_canonicals.append(canonical)
        if canonical.id is None:
            canonical.id = uuid4()
        canonical.memberships = []
        return canonical

    def _capture_membership(_session, membership):
        added_memberships.append(membership)
        if membership.id is None:
            membership.id = uuid4()
        return membership

    canonical_repo.add.side_effect = _capture_canonical
    canonical_repo.add_membership.side_effect = _capture_membership

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert outcome["match_type"] == "seed"
    assert added_canonicals
    assert added_memberships
    assert task_repo.enqueue.called


def test_canonicalization_service_reuses_existing_url_matched_canonical():
    canonical_repo = Mock()
    canonical_repo.get_membership_for_source_incident.return_value = None
    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="abc",
        status="open",
        institution_name="Penn State University",
        country_code="US",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    existing_primary = CanonicalMembership(
        id=uuid4(),
        canonical_incident_id=existing_canonical.id,
        source_incident_id=uuid4(),
        match_type="seed",
        match_score=100.0,
        survivor_score=10.0,
        is_primary_member=True,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    canonical_repo.find_by_url_candidates.return_value = [existing_canonical]
    canonical_repo.list_memberships.return_value = [existing_primary]
    canonical_repo.get_by_id.return_value = existing_canonical

    source_repo = Mock()
    incident = _source_incident(event_key="story-2")
    source_repo.get_by_id.return_value = incident

    enrichment_repo = Mock()
    enrichment = _source_enrichment(incident)
    enrichment_repo.get_by_source_incident.return_value = enrichment

    task_repo = Mock()
    task_repo.get_active_for_target.return_value = object()

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()
    session.execute.return_value.scalars.return_value.all.return_value = [enrichment]
    session.query.return_value.filter_by.return_value.delete.return_value = None

    added_memberships = []

    def _capture_membership(_session, membership):
        added_memberships.append(membership)
        membership.id = membership.id or uuid4()
        return membership

    canonical_repo.add_membership.side_effect = _capture_membership

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert outcome["match_type"] == "url_exact"
    assert added_memberships
    assert added_memberships[0].canonical_incident_id == existing_canonical.id
