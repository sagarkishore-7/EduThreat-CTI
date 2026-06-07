"""Unit tests for star-schema projection helpers (pure, no DB)."""

from datetime import date

import pytest

from src.edu_cti_v2.services import star_projection as sp


class TestClassifyIoc:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("8.8.8.8", "ipv4"),
            ("192.168.1.1", "ipv4"),
            ("evil.example.com", "domain"),
            ("sub.domain.co.uk", "domain"),
            ("https://malware.test/path", "url"),
            ("attacker@example.com", "email"),
            ("d41d8cd98f00b204e9800998ecf8427e", "md5"),
            ("da39a3ee5e6b4b0d3255bfef95601890afd80709", "sha1"),
            ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "sha256"),
            ("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "ipv6"),
        ],
    )
    def test_classifies_known_indicator_types(self, value, expected):
        assert sp._classify_ioc(value) == expected

    @pytest.mark.parametrize("value", ["", "   ", "not an ioc", "just text", "a" * 300])
    def test_rejects_non_indicators(self, value):
        assert sp._classify_ioc(value) is None


class TestNumberParsing:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (1000, 1000.0),
            (12.5, 12.5),
            ("$1,000,000", 1000000.0),
            ("275 million", 275.0),  # digits extracted; trailing words dropped
            ("1234", 1234.0),
            (None, None),
            ("", None),
            ("n/a", None),
        ],
    )
    def test_to_number(self, value, expected):
        assert sp._to_number(value) == expected

    def test_to_int_truncates(self):
        assert sp._to_int("1,234") == 1234
        assert sp._to_int(None) is None


class TestDateParsing:
    def test_iso_date_variants(self):
        assert sp._to_date("2025-08-09") == date(2025, 8, 9)
        assert sp._to_date("2025-08") == date(2025, 8, 1)
        assert sp._to_date("2025") == date(2025, 1, 1)

    def test_passthrough_and_none(self):
        d = date(2024, 1, 2)
        assert sp._to_date(d) == d
        assert sp._to_date(None) is None
        assert sp._to_date("not a date") is None


class TestBoolCoercion:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("yes", True),
            ("false", False),
            ("no", False),
            ("maybe", None),
            (None, None),
        ],
    )
    def test_as_bool(self, value, expected):
        assert sp._as_bool(value) is expected


class TestAttackFamily:
    @pytest.mark.parametrize(
        "slug,family",
        [
            ("ransomware_encryption", "ransomware"),
            ("ransomware_double_extortion", "ransomware"),
            ("phishing_credential_harvest", "phishing"),
            ("spear_phishing", "phishing"),
            ("data_breach_external", "data_breach"),
            ("ddos_volumetric", "ddos"),
            ("malware_trojan", "malware"),
            ("supply_chain_software", "supply_chain"),
            ("insider_malicious", "insider"),
            ("espionage", "other"),
        ],
    )
    def test_attack_family_mapping(self, slug, family):
        assert sp._attack_family(slug) == family


class TestDataCategoryCollection:
    def test_collects_list_and_boolean_flags(self):
        di = {
            "data_types_affected": ["Student PII", "financial_data"],
            "student_data": True,
            "faculty_data": True,
            "alumni_data": None,
            "research_data": False,
        }
        cats = set(sp._collect_data_categories(di))
        assert "student_pii" in cats
        assert "financial_data" in cats
        assert "student_data" in cats
        assert "faculty_data" in cats
        # None / False flags are not promoted
        assert "alumni_data" not in cats
        assert "research_data" not in cats

    def test_empty(self):
        assert sp._collect_data_categories({}) == []


class TestIocCollection:
    def test_collects_from_timeline_indicators_and_top_level(self):
        proj = {
            "timeline": [
                {"indicators": ["8.8.8.8", "evil.test.com"]},
                {"indicators": "1.2.3.4"},
                {"indicators": None},
            ],
            "iocs": ["attacker@bad.com"],
        }
        found = dict(sp._collect_iocs(proj))
        # mapping of (type, value) -> None; check the type classification
        types = {t for (t, _v) in sp._collect_iocs(proj)}
        values = {v for (_t, v) in sp._collect_iocs(proj)}
        assert "8.8.8.8" in values
        assert "ipv4" in types
        assert "domain" in types
        assert "email" in types

    def test_no_indicators(self):
        assert sp._collect_iocs({"timeline": [{"indicators": None}]}) == []


class TestNestedGet:
    def test_safe_nested_get(self):
        proj = {"attack_dynamics": {"attack_vector": "phishing_email"}}
        assert sp._d(proj, "attack_dynamics", "attack_vector") == "phishing_email"
        assert sp._d(proj, "attack_dynamics", "missing") is None
        assert sp._d(proj, "missing", "x") is None
        assert sp._d(None, "x") is None
