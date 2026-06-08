"""Tests for the wall-clock source-timeout guard and the unified listing-fetch
toggle. These guard against a single hung source freezing an entire collection
run (the historical-run hang) and against the timeout wrapper breaking the
signature introspection that the collectors rely on."""

from __future__ import annotations

import inspect
import time

import pytest

from src.edu_cti.core.timeouts import (
    Heartbeat,
    OperationTimeout,
    call_with_idle_timeout,
    call_with_timeout,
    guard_source_timeout,
    source_timeout_seconds,
)


def test_call_with_timeout_returns_value_for_fast_fn():
    assert call_with_timeout(lambda: 42, timeout_seconds=5, label="fast") == 42


def test_call_with_timeout_raises_on_slow_fn():
    start = time.time()
    with pytest.raises(OperationTimeout):
        call_with_timeout(lambda: time.sleep(5), timeout_seconds=0.2, label="slow")
    # Should give up promptly, not wait for the full sleep.
    assert time.time() - start < 2.0


def test_call_with_timeout_reraises_inner_exception():
    def boom():
        raise ValueError("kaboom")

    with pytest.raises(ValueError, match="kaboom"):
        call_with_timeout(boom, timeout_seconds=5, label="boom")


def test_guard_preserves_signature_for_introspection():
    """Collectors call inspect.signature(builder) to decide which kwargs to pass
    (save_callback, incremental, max_pages). The guard must not hide them."""

    def builder(*, save_callback=None, incremental=True, max_pages=None):
        return ["ok"]

    guarded = guard_source_timeout(builder, label="curated:demo", timeout_seconds=5)
    params = inspect.signature(guarded).parameters
    assert "save_callback" in params
    assert "incremental" in params
    assert "max_pages" in params
    assert guarded(save_callback=None, incremental=False) == ["ok"]


def test_guard_times_out_hung_builder():
    def hung_builder(**_kwargs):
        time.sleep(5)
        return ["never"]

    guarded = guard_source_timeout(hung_builder, label="curated:hung", timeout_seconds=0.2)
    with pytest.raises(OperationTimeout):
        guarded(save_callback=None)


def test_guard_does_not_kill_slow_but_progressing_source():
    """A source that keeps saving batches is making progress and must run to
    completion even though its total time far exceeds the idle budget. This is
    the Comparitech regression: a legitimately slow multi-page scrape was being
    killed by a flat total-time budget."""

    saved = []

    def slow_progressing_builder(*, save_callback=None, **_kwargs):
        # Total runtime (~0.9s) exceeds the 0.3s idle budget, but each step
        # saves within the budget, so progress keeps resetting the clock.
        for i in range(6):
            time.sleep(0.15)
            if save_callback is not None:
                save_callback([f"incident-{i}"])
        return [f"incident-{i}" for i in range(6)]

    def collector_save(batch):
        saved.extend(batch)

    guarded = guard_source_timeout(
        slow_progressing_builder, label="curated:comparitech", timeout_seconds=0.3
    )
    result = guarded(save_callback=collector_save)
    assert len(result) == 6
    assert len(saved) == 6  # all batches persisted, nothing lost to a timeout


def test_guard_kills_source_that_stops_progressing():
    """A source that saves a few batches then stalls is abandoned once the idle
    stretch exceeds the budget — distinguishing a hang from slow progress."""

    def stalling_builder(*, save_callback=None, **_kwargs):
        if save_callback is not None:
            save_callback(["one"])  # early progress
        time.sleep(5)  # then hang with no further progress
        return ["unreachable"]

    guarded = guard_source_timeout(
        stalling_builder, label="curated:stall", timeout_seconds=0.3
    )
    with pytest.raises(OperationTimeout):
        guarded(save_callback=lambda batch: None)


def test_idle_timeout_resets_on_heartbeat():
    hb = Heartbeat()

    def worker():
        for _ in range(5):
            time.sleep(0.1)
            hb.beat()
        return "done"

    # Idle budget 0.25s < total 0.5s, but beats every 0.1s keep it alive.
    assert call_with_idle_timeout(
        worker, heartbeat=hb, idle_timeout_seconds=0.25, label="hb"
    ) == "done"


def test_source_timeout_env_override(monkeypatch):
    monkeypatch.setenv("EDU_CTI_SOURCE_TIMEOUT_SECONDS", "123")
    assert source_timeout_seconds() == 123
    monkeypatch.setenv("EDU_CTI_SOURCE_TIMEOUT_SECONDS", "not-a-number")
    assert source_timeout_seconds() == 600  # falls back to default
    monkeypatch.delenv("EDU_CTI_SOURCE_TIMEOUT_SECONDS", raising=False)
    assert source_timeout_seconds() == 600


def test_unified_listing_fetch_toggle(monkeypatch):
    from src.edu_cti.core import http

    monkeypatch.delenv("EDU_CTI_UNIFY_LISTING_FETCH", raising=False)
    assert http._unified_listing_fetch_enabled() is True  # on by default
    monkeypatch.setenv("EDU_CTI_UNIFY_LISTING_FETCH", "0")
    assert http._unified_listing_fetch_enabled() is False
    monkeypatch.setenv("EDU_CTI_UNIFY_LISTING_FETCH", "off")
    assert http._unified_listing_fetch_enabled() is False
    monkeypatch.setenv("EDU_CTI_UNIFY_LISTING_FETCH", "1")
    assert http._unified_listing_fetch_enabled() is True


def test_fetch_listing_html_returns_none_when_tiers_disabled(monkeypatch):
    """With Scrapling forced off and Oxylabs not enabled, the unified listing
    fetch yields None so callers fall back to the legacy path."""
    from src.edu_cti.pipeline.phase2.storage import article_fetcher

    monkeypatch.setattr(article_fetcher, "_fetch_scrapling_enabled", lambda: False)
    monkeypatch.setattr(article_fetcher, "_fetch_oxylabs_enabled", lambda: False)
    assert article_fetcher.fetch_listing_html("https://example.com/listing") is None
