import importlib
from types import SimpleNamespace
from unittest.mock import patch


def test_resolve_memory_policy_prefers_cgroup_limit(monkeypatch):
    phase2_main = importlib.import_module("src.edu_cti.pipeline.phase2.__main__")

    monkeypatch.setattr(phase2_main, "PHASE2_MEMORY_MONITOR_ENABLED", True)
    monkeypatch.setattr(phase2_main, "PHASE2_MEMORY_SOFT_LIMIT_MB", 0)
    monkeypatch.setattr(phase2_main, "PHASE2_MEMORY_HARD_LIMIT_MB", 0)
    monkeypatch.setattr(phase2_main, "PHASE2_MEMORY_SOFT_LIMIT_PCT", 0.75)
    monkeypatch.setattr(phase2_main, "PHASE2_MEMORY_HARD_LIMIT_PCT", 0.85)
    monkeypatch.setattr(
        phase2_main,
        "_detect_container_memory_limit_bytes",
        lambda: 4 * 1024 * 1024 * 1024,
    )

    policy = phase2_main._resolve_memory_policy()

    assert policy is not None
    assert policy["container_limit_mb"] == 4096
    assert policy["soft_limit_mb"] == 3072
    assert policy["hard_limit_mb"] == 3481


def test_request_memory_pause_sets_cancel_and_progress(monkeypatch):
    phase2_main = importlib.import_module("src.edu_cti.pipeline.phase2.__main__")

    phase2_main._cancel_event.clear()
    phase2_main._reset_memory_guard_state()
    phase2_main._progress["step"] = ""
    phase2_main._progress["detail"] = ""

    with patch.object(phase2_main._metrics, "increment") as mock_increment:
        requested = phase2_main._request_memory_pause(
            rss_mb=2900.0,
            hard_limit_mb=2600,
            container_limit_mb=3072,
        )

    state = phase2_main._get_memory_guard_state()

    assert requested is True
    assert state["pause_requested"] is True
    assert phase2_main._cancel_event.is_set() is True
    assert phase2_main._progress["step"] == "Pausing for memory pressure"
    assert "2900 MB" in phase2_main._progress["detail"]
    mock_increment.assert_called_once_with("pipeline_memory_pause_total")

    phase2_main._cancel_event.clear()
    phase2_main._reset_memory_guard_state()


def test_reset_phase2_run_state_clears_cancel_and_progress():
    phase2_main = importlib.import_module("src.edu_cti.pipeline.phase2.__main__")

    phase2_main._cancel_event.set()
    with phase2_main._in_progress_lock:
        phase2_main._in_progress.add("incident-123")
    phase2_main._progress["step"] = "Pausing for memory pressure"
    phase2_main._progress["detail"] = "RSS 2900 MB exceeded hard limit 2600 MB"
    phase2_main._progress["percent"] = 88
    phase2_main._reset_memory_guard_state()

    phase2_main._reset_phase2_run_state()

    assert phase2_main._cancel_event.is_set() is False
    assert phase2_main._progress == {"step": "", "detail": "", "percent": 0}
    with phase2_main._in_progress_lock:
        assert phase2_main._in_progress == set()
    assert phase2_main._get_memory_guard_state()["pause_requested"] is False


def test_check_memory_pressure_requests_pause_at_hard_limit(monkeypatch):
    phase2_main = importlib.import_module("src.edu_cti.pipeline.phase2.__main__")

    phase2_main._reset_memory_guard_state()
    phase2_main._cancel_event.clear()
    monkeypatch.setattr(
        phase2_main,
        "_get_memory_policy",
        lambda: {
            "soft_limit_mb": 2000,
            "hard_limit_mb": 2500,
            "container_limit_mb": 3072,
            "check_interval": 10,
            "gc_interval": 1000,
        },
    )

    fake_psutil = SimpleNamespace(
        Process=lambda _pid: SimpleNamespace(
            memory_info=lambda: SimpleNamespace(rss=2700 * 1024 * 1024)
        )
    )

    with patch.dict("sys.modules", {"psutil": fake_psutil}), patch.object(
        phase2_main._metrics, "observe"
    ), patch.object(phase2_main._metrics, "increment"):
        should_pause = phase2_main._check_memory_pressure(10)

    assert should_pause is True
    assert phase2_main._get_memory_guard_state()["pause_requested"] is True

    phase2_main._cancel_event.clear()
    phase2_main._reset_memory_guard_state()
