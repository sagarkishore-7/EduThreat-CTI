"""Tests for the wall-clock source-timeout guard and the unified listing-fetch
toggle. These guard against a single hung source freezing an entire collection
run (the historical-run hang) and against the timeout wrapper breaking the
signature introspection that the collectors rely on."""

from __future__ import annotations

import inspect
import time

import pytest

from src.edu_cti.core.timeouts import (
    OperationTimeout,
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
