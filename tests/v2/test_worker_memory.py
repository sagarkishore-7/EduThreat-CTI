"""Tests for the worker memory backstop and reclamation.

These lock in the fix for the worker OOM that persisted even at a safe worker
count: process RSS ratcheted up across ML-inference tasks because freed heap was
never returned to the OS. Reclamation (gc + malloc_trim) now runs after each task
and when the guard trips.
"""

from src.edu_cti_v2 import worker


def test_reclaim_memory_is_safe_to_call():
    # Must never raise, on glibc or non-glibc platforms.
    worker._reclaim_memory()
    worker._reclaim_memory()


def test_memory_guard_pauses_above_high_water(monkeypatch):
    monkeypatch.setattr(worker, "current_rss_mb", lambda: 7000.0)
    assert worker._memory_guard_should_pause(6255.0) == 7000.0


def test_memory_guard_allows_below_high_water(monkeypatch):
    monkeypatch.setattr(worker, "current_rss_mb", lambda: 5000.0)
    assert worker._memory_guard_should_pause(6255.0) is None


def test_memory_guard_noop_without_limit():
    assert worker._memory_guard_should_pause(None) is None


def test_memory_guard_handles_unavailable_rss(monkeypatch):
    monkeypatch.setattr(worker, "current_rss_mb", lambda: None)
    assert worker._memory_guard_should_pause(6255.0) is None
