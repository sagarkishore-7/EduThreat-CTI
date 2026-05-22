from src.edu_cti_v2.source_identity import identity_matches_source_anchor, recover_source_identity


def test_recover_source_identity_uses_education_title_when_needed():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle=None,
        raw_title="Leiden University website down in cyberattack",
    )

    assert identity == "Leiden University"


def test_recover_source_identity_ignores_non_education_title():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle=None,
        raw_title="Ransomware: Refusing to Negotiate with Attackers",
    )

    assert identity is None


def test_recover_source_identity_ignores_geography_only_subtitle():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle="Ukraine",
        raw_title="Massive attacks on Wordpress sites of Ukrainian universities",
    )

    assert identity is None


def test_recover_source_identity_ignores_rss_headline_subtitle():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle="Canvas Breach Disrupts Schools & Colleges Nationwide Krebs on Security",
        raw_title="Canvas Breach Disrupts Schools & Colleges Nationwide - Krebs on Security",
    )

    assert identity is None


def test_recover_source_identity_ignores_rss_publisher_subtitle():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle="BusinessLine",
        raw_title="Colleges around the world report web outages after vendor hack - BusinessLine",
    )

    assert identity is None


def test_recover_source_identity_ignores_incident_fragment_subtitle():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle="may have impacted 9,000 schools Security Affairs",
        raw_title="Canvas data breach may have impacted 9,000 schools - Security Affairs",
    )

    assert identity is None


def test_recover_source_identity_keeps_long_specific_subtitle_anchor():
    identity = recover_source_identity(
        raw_institution_name=None,
        raw_victim_name=None,
        raw_subtitle="Universität Bremen, Institut für Didaktik der Naturwissenschaften - Bremen, Germany",
        raw_title="Cyber attack on a university institute in Germany",
    )

    assert identity == "Universität Bremen, Institut für Didaktik der Naturwissenschaften"


def test_identity_matches_source_anchor_for_translated_name():
    assert identity_matches_source_anchor(
        "Sorbonne University",
        "Sorbonne Université",
        extracted_aliases=["Sorbonne Université"],
    )


def test_identity_matches_source_anchor_for_acronym_variant():
    assert identity_matches_source_anchor(
        "Kansas State University",
        "Kansas State University (K-State)",
        extracted_aliases=["K-State"],
    )


def test_identity_matches_source_anchor_for_campus_variant():
    assert identity_matches_source_anchor(
        "South East Technological University",
        "South East Technological University Waterford Campus",
        extracted_aliases=["South East Technological University Waterford Campus"],
    )


def test_identity_matches_source_anchor_for_setu_acronym():
    assert identity_matches_source_anchor(
        "South East Technological University",
        "SETU Waterford campuses",
    )


def test_identity_matches_source_anchor_for_nc_abbreviation():
    assert identity_matches_source_anchor(
        "North Carolina Central University",
        "NC Central University",
    )
