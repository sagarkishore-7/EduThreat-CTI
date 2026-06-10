from src.edu_cti_v2.normalization import (
    is_generic_actor,
    normalize_ransomware_family,
    normalize_threat_actor_name,
)


def test_is_generic_actor_drops_descriptive_labels_keeps_real_groups():
    for junk in [
        "criminal",
        "criminals",
        "Russian cyber-extortion",
        "cyber extortionists",
        "unknown actor",
        "threat actors",
        "Chinese",
        "unidentified hackers",
    ]:
        assert is_generic_actor(junk) is True, junk
        assert normalize_threat_actor_name(junk) is None, junk
    for real in ["Cl0p", "Qilin", "ShinyHunters", "Scattered Spider", "LockBit 3.0", "Hunters International"]:
        assert is_generic_actor(real) is False, real
        assert normalize_threat_actor_name(real) is not None, real


def test_threat_actor_suffix_variants_normalize_cleanly():
    assert normalize_threat_actor_name("Clop ransomware gang") == "Cl0p"
    assert normalize_threat_actor_name("Clop extortion group") == "Cl0p"
    assert normalize_threat_actor_name("BlackSuit ransomware gang") == "BlackSuit"
    assert normalize_threat_actor_name("INC ransomware gang") == "INC"
    assert normalize_threat_actor_name("INC ransom") == "INC"
    assert normalize_threat_actor_name("PEAR ransomware group") == "PEAR"
    assert normalize_threat_actor_name("Monday group") == "Monday"


def test_ransomware_family_variants_normalize_cleanly():
    assert normalize_ransomware_family("Clop ransomware gang") == "Cl0p"
    assert normalize_ransomware_family("Clop extortion group") == "Cl0p"
    assert normalize_ransomware_family("BlackSuit ransomware gang") == "BlackSuit"
    assert normalize_ransomware_family("INC ransomware gang") == "INC Ransom"
    assert normalize_ransomware_family("INC ransom") == "INC Ransom"
    assert normalize_ransomware_family("PEAR ransomware group") == "PEAR"


def test_lockbit_actor_and_family_are_normalized_differently():
    assert normalize_threat_actor_name("LockBit 2.0 ransomware gang") == "LockBit"
    assert normalize_threat_actor_name("LockBit 3.0") == "LockBit"
    assert normalize_ransomware_family("LockBit 2.0") == "LockBit 2.0"
    assert normalize_ransomware_family("LockBit 3.0") == "LockBit 3.0"
    assert normalize_ransomware_family("LockBit Black") == "LockBit 3.0"


def test_generic_threat_actor_placeholders_are_suppressed():
    assert normalize_threat_actor_name("unknown gang") is None
    assert normalize_threat_actor_name("cybercriminal group") is None
    assert normalize_threat_actor_name("pro-Russian hackers") is None
    assert normalize_threat_actor_name("Chinese hackers") is None
    assert normalize_threat_actor_name("China") is None
