"""Tests for the shared threat-actor identity module (P0-1)."""

from src.edu_cti.core.actor_identity import canonical_actor_name, is_generic_actor


def test_spelling_and_spacing_variants_collapse_to_one_canonical():
    # The live variant pairs that were splitting campaign families.
    assert canonical_actor_name("ShinyHunters") == canonical_actor_name("Shiny Hunters") == "ShinyHunters"
    assert canonical_actor_name("Cl0p") == canonical_actor_name("Clop") == "Cl0p"
    assert canonical_actor_name("Clop ransomware syndicate") == "Cl0p"
    assert canonical_actor_name("Skira") == canonical_actor_name("Skira Team") == "Skira"
    assert canonical_actor_name("LockBit") == canonical_actor_name("LockBit 3.0") == "LockBit"
    assert canonical_actor_name("alphv") == "BlackCat/ALPHV"


def test_person_name_middle_initial_normalization():
    assert canonical_actor_name("Matthew D. Lane") == canonical_actor_name("Matthew Lane") == "Matthew Lane"
    # a single name and a two-token name are untouched
    assert canonical_actor_name("Qilin") == "Qilin"
    assert canonical_actor_name("Vice Society") == "Vice Society"


def test_generic_labels_drop_but_real_names_survive_descriptors():
    for junk in ["criminal", "Russian cyber-extortion", "threat actors", "Chinese", "unknown gang"]:
        assert is_generic_actor(junk) is True, junk
        assert canonical_actor_name(junk) is None, junk
    # a real name buried in a descriptive phrase is kept (token-core, not substring)
    assert canonical_actor_name("Clop cybercriminal") == "Cl0p"
    assert canonical_actor_name("Fog hacking") == "Fog"


def test_unknown_actor_passes_through_cleaned():
    # not in any registry -> kept as its cleaned surface form, deterministically
    assert canonical_actor_name("Brand New Crew") == "Brand New"  # "crew" is a descriptor suffix
    assert canonical_actor_name(None) is None
    assert canonical_actor_name("   ") is None


def test_deterministic_no_runtime_mutation():
    # calling in either order yields the same canonical (static tight-key map)
    a = canonical_actor_name("shiny hunters")
    b = canonical_actor_name("ShinyHunters")
    assert a == b == "ShinyHunters"
