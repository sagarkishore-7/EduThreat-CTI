"""Tests for the public-API security config: CORS allowlist, rate-limit env,
and that the app builds with the limiter + health exemptions."""

from __future__ import annotations

import importlib


def _reload_app_module():
    import src.edu_cti_v2.api_app as api_app

    return importlib.reload(api_app)


def test_cors_origins_default_and_env(monkeypatch):
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    mod = _reload_app_module()
    default = mod._cors_origins()
    assert any("localhost:3000" in o for o in default)
    assert "*" not in default  # default is NOT wide open

    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "https://a.example, https://b.example")
    assert mod._cors_origins() == ["https://a.example", "https://b.example"]

    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "*")
    assert mod._cors_origins() == ["*"]


def test_cors_origin_regex_matches_vercel_previews(monkeypatch):
    import re

    monkeypatch.delenv("CORS_ALLOW_ORIGIN_REGEX", raising=False)
    mod = _reload_app_module()
    pattern = re.compile(mod._cors_origin_regex())
    # Production + rotating preview + branch deployments all match.
    assert pattern.fullmatch("https://edu-threat-cti-dashboard.vercel.app")
    assert pattern.fullmatch(
        "https://edu-threat-cti-dashboard-9v8sg0sk1-sagarkishore-7s-projects.vercel.app"
    )
    assert pattern.fullmatch(
        "https://edu-threat-cti-dashboard-git-main-sagarkishore-7s-projects.vercel.app"
    )
    # Look-alikes are rejected.
    assert not pattern.fullmatch("https://evil.example.com")
    assert not pattern.fullmatch("https://edu-threat-cti-dashboard.vercel.app.evil.com")

    # Overridable; empty string disables preview matching.
    monkeypatch.setenv("CORS_ALLOW_ORIGIN_REGEX", "")
    assert mod._cors_origin_regex() is None
    monkeypatch.setenv("CORS_ALLOW_ORIGIN_REGEX", r"https://foo\.example\.com")
    assert mod._cors_origin_regex() == r"https://foo\.example\.com"


def test_rate_limit_default_and_env(monkeypatch):
    monkeypatch.delenv("API_RATE_LIMIT", raising=False)
    mod = _reload_app_module()
    assert mod._rate_limit() == "60/minute"
    monkeypatch.setenv("API_RATE_LIMIT", "120/minute")
    assert mod._rate_limit() == "120/minute"


def test_app_builds_with_limiter(monkeypatch):
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    mod = _reload_app_module()
    app = mod.create_app()
    # Limiter attached to app state, health routes present.
    assert getattr(app.state, "limiter", None) is not None
    paths = {r.path for r in app.routes}
    assert "/health" in paths and "/api/health" in paths


def test_health_open_and_app_starts(monkeypatch):
    from fastapi.testclient import TestClient

    mod = _reload_app_module()
    app = mod.create_app()
    with TestClient(app) as client:
        r = client.get("/api/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"
