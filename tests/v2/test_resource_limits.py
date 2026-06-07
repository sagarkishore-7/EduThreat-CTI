"""Regression tests for worker auto-sizing (the OOM crash-loop fix).

The worker OOM crash-looped because the auto-sizer modelled each concurrent
enrichment as 200 MB, which is only valid when the local ML pre-pass is off.
With ML on, the real peak is ~1.8 GB, so 6 workers blew past a 7.6 GB container.
These tests pin the corrected, ML-aware sizing so the regression cannot return.
"""

import pytest

from src.edu_cti_v2 import resource_limits as rl


@pytest.fixture
def railway_container(monkeypatch):
    """Simulate the Railway worker container memory limit."""
    monkeypatch.setattr(rl, "cgroup_memory_limit_mb", lambda: 7629)
    # Clear overrides that would short-circuit auto-sizing.
    for var in ("EDU_CTI_V2_WORKER_COUNT", "EDU_CTI_V2_PER_WORKER_MB",
                "EDU_CTI_V2_MAX_WORKERS", "EDU_CTI_V2_MODEL_FLOOR_MB"):
        monkeypatch.delenv(var, raising=False)
    yield monkeypatch


def test_local_ml_on_derives_safe_worker_count(railway_container):
    railway_container.setenv("EDU_CTI_V2_ENABLE_LOCAL_ML", "true")
    railway_container.delenv("DISABLE_ML_FEATURES", raising=False)
    workers = rl.resolve_enrichment_worker_count("auto")
    # 2 workers: peak ~= 1.6 GB floor + 2 x 1.8 GB = 5.2 GB, under 7.6 GB.
    assert workers == 2
    # Peak estimate stays under the container limit.
    assert 1600 + workers * rl.DEFAULT_PER_WORKER_MB_WITH_ML < 7629


def test_local_ml_off_uses_rate_cap(railway_container):
    railway_container.setenv("EDU_CTI_V2_ENABLE_LOCAL_ML", "false")
    railway_container.setenv("DISABLE_ML_FEATURES", "true")
    # With ML off the per-worker cost is small, so the rate cap (6) binds.
    assert rl.resolve_enrichment_worker_count("auto") == rl.DEFAULT_MAX_ENRICH_WORKERS


def test_explicit_worker_count_overrides_autosizer(railway_container):
    railway_container.setenv("EDU_CTI_V2_WORKER_COUNT", "4")
    assert rl.resolve_enrichment_worker_count("auto") == 4


def test_per_worker_env_override(railway_container):
    railway_container.setenv("EDU_CTI_V2_ENABLE_LOCAL_ML", "true")
    railway_container.setenv("EDU_CTI_V2_PER_WORKER_MB", "1000")
    # usable = (7629 - 1600) * 0.85 = 5124; 5124 // 1000 = 5, under rate cap 6.
    assert rl.resolve_enrichment_worker_count("auto") == 5


def test_ml_aware_per_worker_default():
    assert rl.DEFAULT_PER_WORKER_MB_WITH_ML > rl.DEFAULT_PER_WORKER_MB_NO_ML
    assert rl.DEFAULT_PER_WORKER_MB_WITH_ML >= 1500  # realistic for the ML pre-pass


def test_memory_guard_tightened_to_82_percent(railway_container):
    # Guard must fire below the limit with headroom for a large-article spike.
    guard = rl.memory_high_water_mb()
    assert guard == pytest.approx(7629 * 0.82, rel=1e-6)
    assert guard < 7629 * 0.90  # tighter than the old 90% threshold


def test_memory_guard_explicit_override(railway_container):
    railway_container.setenv("EDU_CTI_V2_MAX_RSS_MB", "5000")
    assert rl.memory_high_water_mb() == 5000.0


def test_unbounded_container_falls_back_to_cpu(monkeypatch):
    monkeypatch.setattr(rl, "cgroup_memory_limit_mb", lambda: None)
    monkeypatch.setattr(rl, "cgroup_cpu_count", lambda: 2)
    for var in ("EDU_CTI_V2_WORKER_COUNT", "EDU_CTI_V2_MAX_WORKERS"):
        monkeypatch.delenv(var, raising=False)
    # CPU-based fallback (2*cpu+1=5) capped at the rate cap (6) -> 5.
    assert rl.resolve_enrichment_worker_count("auto") == 5
