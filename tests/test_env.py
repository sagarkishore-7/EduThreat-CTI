import os

from src.edu_cti_v2.env import get_env, get_int, get_flag, get_optional_int


def _clear(*names):
    for n in names:
        os.environ.pop(n, None)


def test_new_name_takes_precedence_over_legacy():
    _clear("NEW", "LEGACY")
    os.environ["NEW"] = "new_value"
    os.environ["LEGACY"] = "legacy_value"
    assert get_env("NEW", "LEGACY") == "new_value"
    _clear("NEW", "LEGACY")


def test_falls_back_to_legacy_alias():
    _clear("NEW", "LEGACY")
    os.environ["LEGACY"] = "legacy_value"
    assert get_env("NEW", "LEGACY") == "legacy_value"
    _clear("LEGACY")


def test_default_when_all_unset():
    _clear("NEW", "LEGACY")
    assert get_env("NEW", "LEGACY", default="fallback") == "fallback"


def test_blank_value_is_skipped():
    # A blank new var must not mask a real legacy value.
    _clear("NEW", "LEGACY")
    os.environ["NEW"] = "   "
    os.environ["LEGACY"] = "real"
    assert get_env("NEW", "LEGACY") == "real"
    _clear("NEW", "LEGACY")


def test_get_int_and_invalid_falls_back_to_default():
    _clear("N")
    os.environ["N"] = "42"
    assert get_int("N", default=7) == 42
    os.environ["N"] = "notanint"
    assert get_int("N", default=7) == 7
    _clear("N")
    assert get_optional_int("N") is None


def test_get_flag_truthy_values():
    _clear("F")
    for v, expected in [("1", True), ("true", True), ("YES", True), ("on", True),
                        ("0", False), ("false", False), ("", False)]:
        if v == "":
            os.environ.pop("F", None)
        else:
            os.environ["F"] = v
        assert get_flag("F", default=False) is expected
    _clear("F")
