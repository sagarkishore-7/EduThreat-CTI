from src.edu_cti_v2.source_identity import recover_source_identity


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
