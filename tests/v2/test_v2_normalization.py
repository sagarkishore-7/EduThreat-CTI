from src.edu_cti_v2.normalization import (
    normalize_ransomware_family,
    normalize_threat_actor_name,
)


def test_threat_actor_suffix_variants_normalize_cleanly():
    assert normalize_threat_actor_name("Clop ransomware gang") == "Cl0p"
    assert normalize_threat_actor_name("Clop extortion group") == "Cl0p"
    assert normalize_threat_actor_name("BlackSuit ransomware gang") == "BlackSuit"
    assert normalize_threat_actor_name("INC ransomware gang") == "INC"
    assert normalize_threat_actor_name("INC ransom") == "INC"


def test_ransomware_family_variants_normalize_cleanly():
    assert normalize_ransomware_family("Clop ransomware gang") == "Cl0p"
    assert normalize_ransomware_family("Clop extortion group") == "Cl0p"
    assert normalize_ransomware_family("BlackSuit ransomware gang") == "BlackSuit"
    assert normalize_ransomware_family("INC ransomware gang") == "INC Ransom"
    assert normalize_ransomware_family("INC ransom") == "INC Ransom"


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
