from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from src.edu_cti_v2.models import CanonicalIncident, CanonicalMembership, SourceEnrichment, SourceIncident, SourceIncidentUrl
from src.edu_cti_v2.services import V2CanonicalizationService, build_source_projection
from src.edu_cti_v2.services.canonicalization import _build_merged_projection, _identity_match_quality


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
    assert projection["date_precision"] == "day"


def test_build_source_projection_normalizes_threat_actor_and_ransomware_aliases():
    incident = _source_incident()
    incident.raw_threat_actor = "CL0P"
    enrichment = _source_enrichment(incident)
    enrichment.typed_enrichment["threat_actor_name"] = "Clop ransomware gang"
    enrichment.typed_enrichment["attack_dynamics"]["ransomware_family"] = "cl0p_clop"

    projection = build_source_projection(incident, enrichment)

    assert projection["threat_actor_name"] == "Cl0p"
    assert projection["ransomware_family"] == "Cl0p"


def test_identity_match_quality_handles_school_subunit_names():
    assert (
        _identity_match_quality(
            "University of California San Francisco School of Medicine",
            "University of California San Francisco",
        )
        >= 88
    )


def test_build_source_projection_promotes_education_technology_provider_as_vendor_name():
    incident = _source_incident(event_key="powerschool-story")
    incident.raw_institution_name = "PowerSchool"
    incident.raw_victim_name = "PowerSchool"
    incident.raw_institution_type = "education_technology_provider"
    incident.raw_country = "United States"
    incident.raw_region = "California"
    incident.raw_city = None
    incident.raw_title = "Texas sues PowerSchool for breach exposing the data of students and teachers"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country": "United States",
            "country_code": "US",
            "incident_date": "2024-12-01",
            "incident_date_precision": "month",
            "attack_category": "data_breach_external",
            "enriched_summary": "PowerSchool suffered a data breach that exposed student and teacher data.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country_code": "US",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "PowerSchool"
    assert projection["vendor_name"] == "PowerSchool"


def test_build_source_projection_does_not_inherit_raw_location_from_mismatched_vendor_placeholder():
    incident = _source_incident(event_key="powerschool-placeholder")
    incident.raw_institution_name = "Forrest City School District"
    incident.raw_victim_name = "Forrest City School District"
    incident.raw_institution_type = "School"
    incident.raw_country = "United States"
    incident.raw_region = "Arkansas"
    incident.raw_city = "Forrest City"
    incident.raw_title = "Ransomware attack on Forrest City School District (2024)"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "incident_date": "2024-12-28",
            "incident_date_precision": "day",
            "attack_category": "third_party_compromise",
            "enriched_summary": "PowerSchool disclosed a breach affecting multiple school districts.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "attack_category": "third_party_compromise",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "PowerSchool"
    assert projection["vendor_name"] == "PowerSchool"
    assert projection["country"] is None
    assert projection["country_code"] is None
    assert projection["region"] is None
    assert projection["city"] is None


def test_build_source_projection_prefers_explicit_extracted_name_over_generic_raw_label():
    incident = _source_incident()
    incident.raw_institution_name = "a university in Australia"
    incident.raw_victim_name = "a university in Australia"
    incident.raw_title = "University of Western Australia suffers major data breach"
    incident.raw_country = "Australia"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "incident_date": "2025-08-09",
            "incident_date_precision": "approximate",
            "attack_category": "data_breach_external",
            "enriched_summary": "University of Western Australia suffered a data breach.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "University of Western Australia",
            "institution_type": "university",
            "country": "Australia",
            "country_code": "AU",
            "incident_date": "2025-08-09",
            "incident_date_precision": "approximate",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "University of Western Australia"
    assert projection["country"] == "Australia"
    assert projection["country_code"] == "AU"


def test_build_source_projection_drops_sentence_like_article_text_as_identity():
    incident = _source_incident()
    incident.raw_title = "Ransomware: Refusing to Negotiate with Attackers"
    incident.raw_subtitle = (
        "Last week, the information security community was saddened to learn of Joseph Edwards, "
        "a 17-year-old secondary school student who committed suicide after..."
    )
    incident.raw_institution_name = incident.raw_subtitle
    incident.raw_victim_name = incident.raw_subtitle
    incident.raw_country = None
    incident.raw_region = None
    incident.raw_city = None
    incident.raw_incident_date = None

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "attack_category": "ransomware_encryption",
            "enriched_summary": "An unknown institution was attacked by ransomware.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": incident.raw_subtitle,
            "attack_category": "ransomware_encryption",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] is None


def test_build_source_projection_drops_vague_plural_identity():
    incident = _source_incident()
    incident.raw_title = "Kolkata: Hackers attack several colleges websites"
    incident.raw_subtitle = "Unknown cyber-hackers hacked the official websites of few colleges in Kolkata."
    incident.raw_institution_name = "several colleges websites"
    incident.raw_victim_name = "several colleges websites"
    incident.raw_country = "India"
    incident.raw_region = None
    incident.raw_city = "Kolkata"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "attack_category": "web_defacement",
            "country": "India",
            "enriched_summary": "Unknown institution experienced a web defacement attack.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "several colleges websites",
            "attack_category": "web_defacement",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] is None


def test_build_source_projection_normalizes_country_from_institution_country():
    incident = _source_incident()
    incident.raw_institution_name = "BYU-Pathway Worldwide"
    incident.raw_victim_name = "BYU-Pathway Worldwide"
    incident.raw_country = "USA"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "incident_date": "2025-06-24",
            "incident_date_precision": "exact",
            "attack_category": "third_party_compromise",
            "enriched_summary": "BYU-Pathway Worldwide disclosed a vendor-linked data incident.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "BYU-Pathway Worldwide",
            "institution_type": "higher_education",
            "institution_country": "United States",
            "incident_date": "2025-06-24",
            "incident_date_precision": "exact",
            "attack_category": "third_party_compromise",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "BYU-Pathway Worldwide"
    assert projection["country"] == "United States"
    assert projection["country_code"] == "US"


def test_build_source_projection_keeps_raw_country_when_generic_title_differs_but_identity_matches():
    incident = _source_incident(event_key="morehead")
    incident.raw_title = "Cyber attack on a university in Kentucky, USA"
    incident.raw_subtitle = "Morehead State University (MSU) - Morehead, Kentucky, USA (Rowan County)"
    incident.raw_institution_name = "Morehead State University (MSU)"
    incident.raw_victim_name = "Morehead State University (MSU)"
    incident.raw_country = "USA"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "Morehead State University",
            "institution_type": "university",
            "incident_date": "2023-07-01",
            "incident_date_precision": "day",
            "attack_category": "unauthorized_access",
            "enriched_summary": "Morehead State University experienced a contained cyber incident.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "Morehead State University",
            "institution_type": "university",
            "incident_date": "2023-07-01",
            "incident_date_precision": "day",
            "attack_category": "unauthorized_access",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "Morehead State University"
    assert projection["country"] == "United States"
    assert projection["country_code"] == "US"


def test_build_source_projection_drops_generic_identity_labels():
    incident = _source_incident(event_key="generic-placeholder")
    incident.raw_title = "Officials disclose cyber incident affecting unnamed district"
    incident.raw_institution_name = "school district"
    incident.raw_victim_name = "school district"
    incident.raw_institution_type = "school_district"

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "research university in Southern District of Texas",
            "institution_type": "university",
            "incident_date": "2026-05-08",
            "incident_date_precision": "day",
            "attack_category": "data_breach_external",
            "enriched_summary": "An unnamed research university was affected.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "school district",
            "institution_type": "school_district",
            "incident_date": "2026-05-08",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] is None


def test_build_merged_projection_backfills_missing_deep_fields_from_supporting_sources():
    selected_incident = _source_incident(event_key="selected-story")
    selected_enrichment = _source_enrichment(selected_incident)
    selected_enrichment.typed_enrichment["system_impact"] = {
        "systems_affected": ["email"],
    }
    selected_enrichment.typed_enrichment["data_impact"] = {}
    selected_enrichment.typed_enrichment["user_impact"] = {}
    selected_enrichment.typed_enrichment["attack_dynamics"].pop("attack_vector", None)
    selected_projection = build_source_projection(selected_incident, selected_enrichment)

    supporting_incident = _source_incident(event_key="supporting-story", url="https://example.com/supporting-article")
    supporting_enrichment = _source_enrichment(supporting_incident)
    supporting_enrichment.typed_enrichment["attack_dynamics"]["attack_vector"] = "stolen_credentials"
    supporting_enrichment.typed_enrichment["system_impact"] = {
        "systems_affected": ["email", "student_portal"],
        "critical_systems_affected": True,
    }
    supporting_enrichment.typed_enrichment["data_impact"] = {
        "records_affected_exact": 5000,
        "data_categories": ["student_pii", "employee_pii"],
    }
    supporting_enrichment.typed_enrichment["user_impact"] = {
        "total_individuals_affected": 5000,
    }
    supporting_projection = build_source_projection(supporting_incident, supporting_enrichment)

    member_documents = [
        {
            "membership": SimpleNamespace(is_primary_member=True, survivor_score=120.0),
            "source_enrichment": selected_enrichment,
            "source_incident": selected_incident,
            "projection": selected_projection,
        },
        {
            "membership": SimpleNamespace(is_primary_member=False, survivor_score=100.0),
            "source_enrichment": supporting_enrichment,
            "source_incident": supporting_incident,
            "projection": supporting_projection,
        },
    ]

    merged_projection, projection_field_sources = _build_merged_projection(
        selected_projection,
        member_documents,
        selected_source_enrichment_id=str(selected_enrichment.id),
    )

    assert merged_projection["attack_vector"] == "stolen_credentials"
    assert merged_projection["typed_enrichment"]["data_impact"]["records_affected_exact"] == 5000
    assert merged_projection["typed_enrichment"]["user_impact"]["total_individuals_affected"] == 5000
    assert merged_projection["typed_enrichment"]["system_impact"]["critical_systems_affected"] is True
    assert merged_projection["typed_enrichment"]["system_impact"]["systems_affected"] == ["email", "student_portal"]
    assert projection_field_sources["data_impact.records_affected_exact"] == [str(supporting_enrichment.id)]
    assert projection_field_sources["system_impact.systems_affected"] == [
        str(selected_enrichment.id),
        str(supporting_enrichment.id),
    ]


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
    task_repo.get_active_for_target.side_effect = [None, None]
    analytics_repo = Mock()

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
        analytics_refresh_repository=analytics_repo,
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
    assert task_repo.enqueue.call_count == 2
    analytics_repo.mark_needs_refresh.assert_called_once_with(
        session,
        refresh_key="dashboard:global",
        refresh_scope="global",
        default_state_payload={},
    )


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
    task_repo.get_active_for_target.side_effect = [object(), object()]
    analytics_repo = Mock()

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
        analytics_refresh_repository=analytics_repo,
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
    analytics_repo.mark_needs_refresh.assert_called_once()


def test_canonicalization_service_does_not_merge_distinct_victims_on_shared_roundup_url():
    canonical_repo = Mock()
    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="incident:reichman",
        institution_name="Reichman University",
        vendor_name=None,
        institution_type="university",
        country="Israel",
        country_code="IL",
        region=None,
        city="Herzliya",
        incident_date=datetime(2023, 4, 4, tzinfo=timezone.utc).date(),
        date_precision="day",
        attack_category="ddos_volumetric",
        attack_vector="unknown",
        threat_actor_name="Anonymous Sudan",
        ransomware_family=None,
        is_education_related=True,
        severity=None,
        canonical_summary="Reichman University website was attacked.",
        status="open",
        first_seen_at=datetime(2023, 4, 4, tzinfo=timezone.utc),
        last_seen_at=datetime(2023, 4, 4, tzinfo=timezone.utc),
        resolution_metadata={},
    )
    canonical_repo.get_membership_for_source_incident.return_value = None
    canonical_repo.find_by_url_candidates.return_value = [existing_canonical]
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.find_identity_candidates.return_value = []
    canonical_repo.list_memberships.return_value = []

    source_repo = Mock()
    incident = _source_incident(
        event_key="roundup-article",
        url="https://www.jpost.com/breaking-news/article-736351",
    )
    incident.raw_title = "DDoS attack on the website of a university in Jerusalem, Israel"
    incident.raw_subtitle = "Hebrew University of Jerusalem - Jerusalem, Israel"
    incident.raw_institution_name = "Hebrew University of Jerusalem"
    incident.raw_victim_name = "Hebrew University of Jerusalem"
    incident.raw_country = "Israel"
    incident.raw_region = None
    incident.raw_city = "Jerusalem"
    incident.raw_incident_date = "2023-04-04"
    source_repo.get_by_id.return_value = incident

    enrichment_repo = Mock()
    enrichment = _source_enrichment(incident)
    enrichment.typed_enrichment["institution_name"] = "Hebrew University of Jerusalem"
    enrichment.typed_enrichment["country"] = "Israel"
    enrichment.typed_enrichment["country_code"] = "IL"
    enrichment.typed_enrichment["city"] = "Jerusalem"
    enrichment.typed_enrichment["incident_date"] = "2023-04-04"
    enrichment.typed_enrichment["attack_category"] = "ddos_volumetric"
    enrichment.typed_enrichment["enriched_summary"] = "Hebrew University of Jerusalem website was attacked."
    enrichment.typed_enrichment["threat_actor_name"] = "Anonymous Sudan"
    enrichment.raw_extraction["institution_name"] = "Hebrew University of Jerusalem"
    enrichment.raw_extraction["country_code"] = "IL"
    enrichment.raw_extraction["attack_category"] = "ddos_volumetric"
    enrichment_repo.get_by_source_incident.return_value = enrichment

    task_repo = Mock()
    task_repo.get_active_for_target.side_effect = [None, object(), object()]
    analytics_repo = Mock()

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
        analytics_refresh_repository=analytics_repo,
    )
    session = Mock()
    session.execute.return_value.scalars.return_value.all.return_value = [enrichment]
    session.query.return_value.filter_by.return_value.delete.return_value = None

    created_canonicals = []
    added_memberships = []

    def _capture_canonical(_session, canonical):
        created_canonicals.append(canonical)
        canonical.id = canonical.id or uuid4()
        return canonical

    def _capture_membership(_session, membership):
        added_memberships.append(membership)
        membership.id = membership.id or uuid4()
        return membership

    canonical_repo.add.side_effect = _capture_canonical
    canonical_repo.add_membership.side_effect = _capture_membership

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert outcome["match_type"] == "seed"
    assert created_canonicals
    assert created_canonicals[0].institution_name == "Hebrew University of Jerusalem"
    assert added_memberships
    assert added_memberships[0].canonical_incident_id == created_canonicals[0].id


def test_build_source_projection_recovers_identity_from_subtitle_when_llm_name_missing():
    incident = _source_incident(event_key="subtitle-fallback")
    incident.raw_title = "DDoS attack on the website of a university in Jerusalem, Israel"
    incident.raw_subtitle = (
        "Hebrew University of Jerusalem (HUJI) / "
        "הַאוּנִיבֶרְסִיטָה הַעִבְרִית בִּירוּשָׁלַיִם - Jerusalem / ירושלים, Israel"
    )
    incident.raw_institution_name = None
    incident.raw_victim_name = "Hebrew University of Jerusalem (HUJI) / הַאוּנִיבֶרְסִיטָה הַעִבְרִית בִּירוּשָׁלַיִם"

    enrichment = _source_enrichment(incident)
    enrichment.typed_enrichment["institution_name"] = None
    enrichment.raw_extraction["institution_name"] = None

    projection = build_source_projection(incident, enrichment)

    assert projection["institution_name"] == "Hebrew University of Jerusalem (HUJI)"


def test_canonicalization_service_skips_new_generic_identity_seed():
    canonical_repo = Mock()
    canonical_repo.get_membership_for_source_incident.return_value = None
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.find_identity_candidates.return_value = []
    canonical_repo.list_memberships.return_value = []

    source_repo = Mock()
    incident = _source_incident(event_key="generic-seed")
    incident.raw_title = "Officials disclose cyber incident affecting unnamed district"
    incident.raw_institution_name = "school district"
    incident.raw_victim_name = "school district"
    incident.raw_institution_type = "school_district"
    source_repo.get_by_id.return_value = incident

    enrichment_repo = Mock()
    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "research university in Southern District of Texas",
            "institution_type": "university",
            "incident_date": "2026-05-08",
            "incident_date_precision": "day",
            "attack_category": "data_breach_external",
            "enriched_summary": "An unnamed research university was affected.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "school district",
            "institution_type": "school_district",
            "incident_date": "2026-05-08",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )
    enrichment_repo.get_by_source_incident.return_value = enrichment

    task_repo = Mock()

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    session = Mock()

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome == {"canonicalized": False, "reason": "missing_identity"}
    canonical_repo.add.assert_not_called()
    canonical_repo.add_membership.assert_not_called()
    task_repo.enqueue.assert_not_called()


def test_canonicalization_service_updates_existing_canonical_with_better_projection():
    canonical_repo = Mock()
    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="abc",
        status="open",
        institution_name="a university in Australia",
        country="Japan",
        country_code="JP",
        incident_date=None,
        date_precision="unknown",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    existing_membership = CanonicalMembership(
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
    canonical_repo.get_membership_for_source_incident.return_value = existing_membership
    canonical_repo.get_by_id.return_value = existing_canonical
    canonical_repo.list_memberships.return_value = [existing_membership]
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.find_identity_candidates.return_value = []

    source_repo = Mock()
    incident = _source_incident()
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
    service._upsert_canonical_enrichment = Mock(return_value=Mock(id=uuid4()))  # type: ignore[attr-defined]
    session = Mock()
    session.flush.return_value = None

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert existing_canonical.institution_name == "Penn State University"
    assert existing_canonical.country_code == "US"
    assert existing_canonical.incident_date.isoformat() == "2026-05-08"


def test_canonicalization_service_refreshes_existing_canonical_after_generic_member_is_dropped():
    canonical_repo = Mock()
    canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="generic-cleanup",
        status="open",
        institution_name="school district",
        country="United States",
        country_code="US",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    invalid_incident = _source_incident(event_key="generic-member")
    invalid_incident.raw_title = "Officials disclose cyber incident affecting unnamed district"
    invalid_incident.raw_institution_name = "school district"
    invalid_incident.raw_victim_name = "school district"
    invalid_incident.raw_institution_type = "school_district"
    invalid_membership = CanonicalMembership(
        id=uuid4(),
        canonical_incident_id=canonical.id,
        source_incident_id=invalid_incident.id,
        match_type="seed",
        match_score=100.0,
        survivor_score=5.0,
        is_primary_member=True,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    valid_incident = _source_incident(event_key="valid-member", url="https://example.com/valid-member")
    valid_membership = CanonicalMembership(
        id=uuid4(),
        canonical_incident_id=canonical.id,
        source_incident_id=valid_incident.id,
        match_type="name_date",
        match_score=96.0,
        survivor_score=90.0,
        is_primary_member=False,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    canonical_repo.get_membership_for_source_incident.return_value = invalid_membership
    canonical_repo.get_by_id.return_value = canonical
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.find_identity_candidates.return_value = []
    canonical_repo.list_memberships.return_value = [invalid_membership, valid_membership]

    source_repo = Mock()
    source_repo.get_by_id.side_effect = lambda _session, source_incident_id: (
        invalid_incident if str(source_incident_id) == str(invalid_incident.id) else valid_incident
    )

    invalid_enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=invalid_incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "research university in Southern District of Texas",
            "institution_type": "university",
            "incident_date": "2026-05-08",
            "incident_date_precision": "day",
            "attack_category": "data_breach_external",
            "enriched_summary": "An unnamed research university was affected.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "school district",
            "institution_type": "school_district",
            "incident_date": "2026-05-08",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )
    valid_enrichment = _source_enrichment(valid_incident)
    enrichment_repo = Mock()
    enrichment_repo.get_by_source_incident.side_effect = lambda _session, source_incident_id: (
        invalid_enrichment if str(source_incident_id) == str(invalid_incident.id) else valid_enrichment
    )

    task_repo = Mock()
    task_repo.get_active_for_target.side_effect = [None, None]

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    service._upsert_canonical_enrichment = Mock(return_value=Mock(id=uuid4()))  # type: ignore[attr-defined]
    session = Mock()

    outcome = service.canonicalize_source_incident(session, invalid_incident.id)

    assert outcome["canonicalized"] is False
    assert outcome["reason"] == "missing_identity"
    assert outcome["canonical_status"] == "open"
    assert canonical.institution_name == "Penn State University"
    assert canonical.primary_source_incident_id == valid_incident.id
    assert invalid_membership.is_primary_member is False
    assert valid_membership.is_primary_member is True
    assert task_repo.enqueue.call_count == 2


def test_canonicalization_service_refreshes_canonical_fields_from_primary_member_projection():
    canonical_repo = Mock()
    incident = _source_incident(event_key="powerschool-primary-refresh", url="https://example.com/powerschool-primary-refresh")
    incident.raw_institution_name = "PowerSchool"
    incident.raw_victim_name = "PowerSchool"
    incident.raw_institution_type = "education_technology_provider"
    incident.raw_country = "United States"
    incident.raw_region = "Massachusetts"
    incident.raw_city = None
    incident.raw_incident_date = "2024-08-01"
    incident.raw_title = "Prosecutors seek prison term for PowerSchool hacker"

    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="powerschool-canonical",
        status="open",
        institution_name="Cincinnati Public Schools",
        vendor_name="PowerSchool",
        country="Canada",
        country_code="CA",
        region="Ontario",
        city="Toronto",
        incident_date=datetime(2023, 12, 1, tzinfo=timezone.utc).date(),
        date_precision="month",
        attack_category="data_breach_external",
        attack_vector="unpatched_system",
        threat_actor_name=None,
        ransomware_family=None,
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    existing_membership = CanonicalMembership(
        id=uuid4(),
        canonical_incident_id=existing_canonical.id,
        source_incident_id=incident.id,
        match_type="seed",
        match_score=100.0,
        survivor_score=10.0,
        is_primary_member=True,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    canonical_repo.get_membership_for_source_incident.return_value = existing_membership
    canonical_repo.get_by_id.return_value = existing_canonical
    canonical_repo.list_memberships.return_value = [existing_membership]
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.find_identity_candidates.return_value = []

    source_repo = Mock()
    source_repo.get_by_id.return_value = incident

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country": "United States",
            "country_code": "US",
            "region": "Massachusetts",
            "city": None,
            "incident_date": "2024-08-01",
            "incident_date_precision": "month",
            "attack_category": "ransomware_double_extortion",
            "attack_vector": "stolen_credentials",
            "threat_actor_name": "Matthew Lane",
            "ransomware_family": "unknown",
            "enriched_summary": "PowerSchool was hit by a ransomware attack.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country_code": "US",
            "incident_date": "2024-08-01",
            "attack_category": "ransomware_double_extortion",
        },
        is_education_related=True,
    )
    enrichment_repo = Mock()
    enrichment_repo.get_by_source_incident.return_value = enrichment

    task_repo = Mock()
    task_repo.get_active_for_target.return_value = None

    service = V2CanonicalizationService(
        canonical_repository=canonical_repo,
        source_incident_repository=source_repo,
        source_enrichment_repository=enrichment_repo,
        pipeline_task_repository=task_repo,
    )
    service._upsert_canonical_enrichment = Mock(return_value=Mock(id=uuid4()))  # type: ignore[attr-defined]
    session = Mock()
    session.flush.return_value = None

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert existing_canonical.institution_name == "PowerSchool"
    assert existing_canonical.country == "United States"
    assert existing_canonical.country_code == "US"
    assert existing_canonical.region == "Massachusetts"
    assert existing_canonical.city is None
    assert existing_canonical.incident_date.isoformat() == "2024-08-01"
    assert existing_canonical.attack_vector == "stolen_credentials"


def test_canonicalization_service_matches_vendor_date_candidates():
    canonical_repo = Mock()
    canonical_repo.get_membership_for_source_incident.return_value = None
    canonical_repo.find_by_url_candidates.return_value = []
    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="vendor-canonical",
        status="open",
        institution_name=None,
        vendor_name="Canvas",
        country="United States",
        country_code="US",
        incident_date=datetime(2026, 5, 8, tzinfo=timezone.utc).date(),
        date_precision="day",
        attack_category="third_party_compromise",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    canonical_repo.find_name_date_candidates.return_value = [existing_canonical]
    canonical_repo.list_memberships.return_value = []

    source_repo = Mock()
    incident = _source_incident(event_key="vendor-story", url="https://example.com/vendor-story")
    incident.raw_institution_name = None
    incident.raw_victim_name = None
    incident.raw_title = "Canvas outage affects universities"
    source_repo.get_by_id.return_value = incident

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "vendor_name": "Canvas",
            "institution_type": "edtech_platform",
            "country": "United States",
            "country_code": "US",
            "incident_date": "2026-05-08",
            "incident_date_precision": "exact",
            "attack_category": "third_party_compromise",
            "enriched_summary": "Canvas suffered an outage affecting university users.",
            "timeline": [],
        },
        raw_extraction={
            "vendor_name": "Canvas",
            "country_code": "US",
            "incident_date": "2026-05-08",
            "attack_category": "third_party_compromise",
        },
        is_education_related=True,
    )
    enrichment_repo = Mock()
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
    session.flush.return_value = None
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
    assert outcome["match_type"] == "vendor_date"
    assert added_memberships
    assert added_memberships[0].canonical_incident_id == existing_canonical.id


def test_canonicalization_service_rejects_country_conflict_even_with_name_match():
    canonical_repo = Mock()
    canonical_repo.get_membership_for_source_incident.return_value = None
    canonical_repo.find_by_url_candidates.return_value = []
    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="country-conflict",
        status="open",
        institution_name="Penn State University",
        country="Japan",
        country_code="JP",
        incident_date=datetime(2026, 5, 8, tzinfo=timezone.utc).date(),
        date_precision="day",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    canonical_repo.find_name_date_candidates.return_value = [existing_canonical]
    canonical_repo.list_memberships.return_value = []

    source_repo = Mock()
    incident = _source_incident(event_key="conflict-story")
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
    session.flush.side_effect = None
    session.execute.return_value.scalars.return_value.all.return_value = [enrichment]
    session.query.return_value.filter_by.return_value.delete.return_value = None

    added_canonicals = []

    def _capture_canonical(_session, canonical):
        added_canonicals.append(canonical)
        canonical.id = canonical.id or uuid4()
        canonical.memberships = []
        return canonical

    canonical_repo.add.side_effect = _capture_canonical
    canonical_repo.add_membership.side_effect = lambda _session, membership: membership

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert outcome["match_type"] == "seed"
    assert added_canonicals


def test_canonicalization_service_merges_vendor_followup_candidates_across_country_conflict():
    canonical_repo = Mock()
    canonical_repo.get_membership_for_source_incident.return_value = None
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    existing_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="powerschool-canonical",
        status="open",
        institution_name="PowerSchool",
        vendor_name="PowerSchool",
        country="United States",
        country_code="US",
        incident_date=datetime(2024, 12, 1, tzinfo=timezone.utc).date(),
        date_precision="month",
        attack_category="data_breach_external",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    canonical_repo.find_identity_candidates.return_value = [existing_canonical]
    canonical_repo.list_memberships.return_value = []

    source_repo = Mock()
    incident = _source_incident(event_key="powerschool-followup", url="https://example.com/powerschool-followup")
    incident.raw_institution_name = "PowerSchool"
    incident.raw_victim_name = "PowerSchool"
    incident.raw_institution_type = "education_technology_provider"
    incident.raw_country = "Canada"
    incident.raw_region = "Ontario"
    incident.raw_city = "Toronto"
    incident.raw_title = "Canadian privacy regulators say schools share blame for PowerSchool hack"
    source_repo.get_by_id.return_value = incident

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country": "Canada",
            "country_code": "CA",
            "region": "Ontario",
            "city": "Toronto",
            "incident_date": "2023-12-01",
            "incident_date_precision": "month",
            "attack_category": "data_breach_external",
            "enriched_summary": "Canadian regulators say schools share blame for the PowerSchool hack that exposed student and teacher data.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country_code": "CA",
            "incident_date": "2023-12-01",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )
    enrichment_repo = Mock()
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
    session.flush.return_value = None
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
    assert outcome["match_type"] == "vendor_followup"
    assert added_memberships
    assert added_memberships[0].canonical_incident_id == existing_canonical.id


def test_canonicalization_service_reassigns_existing_membership_to_better_vendor_followup_canonical():
    canonical_repo = Mock()
    old_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="old-powerschool",
        status="open",
        institution_name="PowerSchool",
        vendor_name=None,
        country="Canada",
        country_code="CA",
        incident_date=datetime(2023, 12, 1, tzinfo=timezone.utc).date(),
        date_precision="month",
        attack_category="data_breach_external",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    better_canonical = CanonicalIncident(
        id=uuid4(),
        canonical_key="better-powerschool",
        status="open",
        institution_name="PowerSchool",
        vendor_name="PowerSchool",
        country="United States",
        country_code="US",
        incident_date=datetime(2024, 12, 1, tzinfo=timezone.utc).date(),
        date_precision="month",
        attack_category="data_breach_external",
        first_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        last_seen_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        resolution_version="v2",
        resolution_metadata={},
    )
    existing_membership = CanonicalMembership(
        id=uuid4(),
        canonical_incident_id=old_canonical.id,
        source_incident_id=uuid4(),
        match_type="seed",
        match_score=100.0,
        survivor_score=10.0,
        is_primary_member=True,
        field_contribution={},
        matcher_version="v2",
        matched_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )

    canonical_repo.get_membership_for_source_incident.return_value = existing_membership
    canonical_repo.find_by_url_candidates.return_value = []
    canonical_repo.find_name_date_candidates.return_value = []
    canonical_repo.find_identity_candidates.return_value = [better_canonical]
    canonical_repo.get_by_id.side_effect = lambda _session, canonical_id: (
        old_canonical if str(canonical_id) == str(old_canonical.id) else better_canonical
    )
    canonical_repo.list_memberships.side_effect = [
        [],
        [],
        [existing_membership],
    ]

    source_repo = Mock()
    incident = _source_incident(event_key="powerschool-followup-reassign", url="https://example.com/powerschool-followup-reassign")
    incident.raw_institution_name = "PowerSchool"
    incident.raw_victim_name = "PowerSchool"
    incident.raw_institution_type = "education_technology_provider"
    incident.raw_country = "Canada"
    incident.raw_region = "Ontario"
    incident.raw_city = "Toronto"
    incident.raw_title = "Canadian privacy regulators say schools share blame for PowerSchool hack"
    source_repo.get_by_id.return_value = incident

    enrichment = SourceEnrichment(
        id=uuid4(),
        source_incident_id=incident.id,
        article_document_id=uuid4(),
        llm_provider="ollama",
        llm_model="deepseek-v3.1:671b-cloud",
        typed_enrichment={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country": "Canada",
            "country_code": "CA",
            "region": "Ontario",
            "city": "Toronto",
            "incident_date": "2023-12-01",
            "incident_date_precision": "month",
            "attack_category": "data_breach_external",
            "enriched_summary": "Canadian regulators say schools share blame for the PowerSchool hack.",
            "timeline": [],
        },
        raw_extraction={
            "institution_name": "PowerSchool",
            "institution_type": "education_technology_provider",
            "country_code": "CA",
            "incident_date": "2023-12-01",
            "attack_category": "data_breach_external",
        },
        is_education_related=True,
    )
    enrichment_repo = Mock()
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
    session.flush.return_value = None
    session.execute.return_value.scalars.return_value.all.return_value = [enrichment]
    session.query.return_value.filter_by.return_value.delete.return_value = None

    outcome = service.canonicalize_source_incident(session, incident.id)

    assert outcome["canonicalized"] is True
    assert outcome["match_type"] == "vendor_followup"
    assert str(existing_membership.canonical_incident_id) == str(better_canonical.id)
    assert old_canonical.status == "excluded"
    assert old_canonical.primary_source_incident_id is None
