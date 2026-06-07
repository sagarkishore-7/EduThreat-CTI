"""Tests for the LLM repetition-degeneration guard in Phase 2 enrichment.

Greedy decoding (temperature 0) can lock into a token loop on some long
articles, emitting tens of thousands of repeated characters until the output
limit, producing unparseable JSON. The guard detects this and fails the
enrichment cleanly (no garbage stored), rather than burning the JSON-repair
cascade on it.
"""

import json

from src.edu_cti.pipeline.phase2.enrichment import _is_degenerate_repetition


def test_flags_real_repetition_loop():
    # The pattern observed in production: a short fragment repeated thousands of times.
    degen = '{"a": null, ' + "compliance_" * 12000 + 'suspension": null}'
    assert _is_degenerate_repetition(degen) is True


def test_does_not_flag_short_output():
    assert _is_degenerate_repetition('{"a": null, "attack": "ransomware"}') is False


def test_does_not_flag_long_valid_sparse_json():
    # 400 fields, most null, plus a varied multi-thousand-word summary: large but
    # legitimate, and must never be flagged.
    import random
    words = ("university ransomware attack data breach student records exfiltration "
             "disclosure incident vendor compromise network encryption recovery "
             "notification affected exposed credentials phishing actor Cl0p LockBit").split()
    fields = {f"field_{i}": (None if i % 3 else "ransomware") for i in range(400)}
    fields["enriched_summary"] = " ".join(random.choice(words) for _ in range(5000))
    payload = json.dumps(fields)
    assert len(payload) > 30000  # genuinely large
    assert _is_degenerate_repetition(payload) is False


def test_does_not_flag_below_min_length():
    # A short repetitive string is not large enough to be a decoding loop.
    assert _is_degenerate_repetition("ab" * 100) is False
