"""Unit tests for write-time categorical normalization (controlled_vocab)."""

import pytest

from src.edu_cti_v2 import controlled_vocab as cv


class TestSlugify:
    def test_lowercases_and_collapses_separators(self):
        assert cv.slugify("Research Institute") == "research_institute"
        assert cv.slugify("K-12 School") == "k_12_school"
        assert cv.slugify("BlackCat/ALPHV") == "blackcat_alphv"

    def test_strips_and_collapses_repeated_separators(self):
        assert cv.slugify("  University   Hospital  ") == "university_hospital"
        assert cv.slugify("a--b__c") == "a_b_c"

    def test_none_and_empty(self):
        assert cv.slugify(None) is None
        assert cv.slugify("   ") is None
        assert cv.slugify("") is None


class TestInstitutionType:
    def test_casing_variants_collapse_to_one_slug(self):
        # The core bug this fixes: "University" and "university" were stored as
        # two distinct values; both must normalize to the same slug.
        assert cv.normalize_institution_type("University") == "university"
        assert cv.normalize_institution_type("university") == "university"
        assert cv.normalize_institution_type("UNIVERSITY") == "university"

    def test_known_aliases_map_to_canonical(self):
        assert cv.normalize_institution_type("School") == "k12_school"
        assert cv.normalize_institution_type("K-12 School") == "k12_school"
        assert cv.normalize_institution_type("high_school") == "k12_school"
        assert cv.normalize_institution_type("College") == "community_college"
        assert cv.normalize_institution_type("Research Institution") == "research_institute"

    def test_in_vocabulary_slug_passes_through(self):
        assert cv.normalize_institution_type("school_district") == "school_district"
        assert cv.normalize_institution_type("edtech_platform") == "edtech_platform"

    def test_unknown_value_keeps_normalized_slug(self):
        # Out-of-vocab values are not dropped; they are still casing-normalized.
        assert cv.normalize_institution_type("Flight School") == "flight_school"

    def test_none(self):
        assert cv.normalize_institution_type(None) is None


class TestAttackCategoryAndVector:
    def test_attack_category_passthrough(self):
        assert cv.normalize_attack_category("ransomware_encryption") == "ransomware_encryption"
        assert cv.normalize_attack_category("Data_Breach_External") == "data_breach_external"

    def test_attack_vector_passthrough(self):
        assert cv.normalize_attack_vector("Phishing Email") == "phishing_email"
        assert cv.normalize_attack_vector("vulnerability_exploit_known") == "vulnerability_exploit_known"


class TestSeverity:
    def test_canonical_and_aliases(self):
        assert cv.normalize_severity("Critical") == "critical"
        assert cv.normalize_severity("info") == "informational"
        assert cv.normalize_severity("moderate") == "medium"
        assert cv.normalize_severity("severe") == "high"
        assert cv.normalize_severity("catastrophic") == "critical"


class TestMitreTactic:
    def test_tactic_normalization(self):
        assert cv.normalize_mitre_tactic("Initial Access") == "initial_access"
        assert cv.normalize_mitre_tactic("command_and_control") == "command_and_control"


class TestVocabularyMembership:
    @pytest.mark.parametrize(
        "kind,slug,expected",
        [
            ("institution_type", "university", True),
            ("institution_type", "flight_school", False),
            ("attack_category", "ransomware_encryption", True),
            ("attack_vector", "phishing_email", True),
            ("severity", "critical", True),
            ("severity", "bogus", False),
            ("mitre_tactic", "impact", True),
            ("institution_type", None, False),
            ("unknown_kind", "x", False),
        ],
    )
    def test_is_in_vocabulary(self, kind, slug, expected):
        assert cv.is_in_vocabulary(kind, slug) is expected

    def test_vocab_sets_are_nonempty_and_frozen(self):
        for vocab in (cv.INSTITUTION_TYPES, cv.ATTACK_CATEGORIES, cv.ATTACK_VECTORS,
                      cv.SEVERITIES, cv.MITRE_TACTICS):
            assert isinstance(vocab, frozenset)
            assert len(vocab) > 0
