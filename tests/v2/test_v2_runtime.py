import time

from src.edu_cti_v2.runtime import V2RuntimeService
from src.edu_cti_v2.worker import V2WorkerRunSummary


def test_v2_runtime_service_starts_and_stops_workers_and_scheduler(monkeypatch):
    seen = []
    prewarmed = []

    class _FakeScheduler:
        def __init__(self):
            self.started = 0
            self.stopped = 0

        def start(self):
            self.started += 1
            return {"running": True}

        def stop(self):
            self.stopped += 1
            return {"running": False}

        def get_status(self):
            return {"running": self.started > self.stopped}

    def _fake_run_worker_loop(*, worker_id, stop_event, task_type=None, exclude_task_types=None, **_kwargs):
        seen.append((worker_id, task_type, tuple(exclude_task_types or ())))
        stop_event.wait(0.05)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=None,
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: prewarmed.append(True))

    scheduler = _FakeScheduler()
    runtime = V2RuntimeService(
        worker_count=2,
        scheduler_service=scheduler,
    )

    started = runtime.start()
    assert started["running"] is True
    assert scheduler.started == 1

    time.sleep(0.02)
    stopped = runtime.stop()
    assert stopped["running"] is False
    assert scheduler.stopped == 1
    assert prewarmed == [True]
    assert ("v2-runtime:orchestrator", "orchestrate_plan", ()) in seen
    assert ("v2-runtime:analytics", "refresh_analytics", ()) in seen
    assert ("v2-runtime:1", None, ("orchestrate_plan", "refresh_analytics", "fetch_article", "resolve_url", "canonicalize")) in seen
    assert ("v2-runtime:2", None, ("orchestrate_plan", "refresh_analytics", "fetch_article", "resolve_url", "canonicalize")) in seen
    assert ("v2-runtime:fetch:1", "fetch_article", ()) in seen
    assert ("v2-runtime:fetch:2", "fetch_article", ()) in seen
    assert ("v2-runtime:resolve:1", "resolve_url", ()) in seen
    assert ("v2-runtime:canonicalize:1", "canonicalize", ()) in seen
    assert started["fetch_worker_count"] == 2
    assert started["resolve_worker_count"] == 1
    assert started["canonicalize_worker_count"] == 1
    assert all(worker["summary"]["stop_reason"] == "stopped" for worker in stopped["workers"])


def test_v2_runtime_service_can_disable_scheduler(monkeypatch):
    def _fake_run_worker_loop(*, worker_id, stop_event, **_kwargs):
        stop_event.wait(0.01)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=None,
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: None)

    runtime = V2RuntimeService(worker_count=1, enable_scheduler=False)
    started = runtime.start()
    assert started["scheduler_enabled"] is False
    assert started["scheduler"] is None
    assert started["fetch_worker_count"] == 1
    assert started["resolve_worker_count"] == 1
    assert started["canonicalize_worker_count"] == 1
    stopped = runtime.stop()
    assert stopped["scheduler"] is None


def test_v2_runtime_service_allows_explicit_fetch_and_resolve_worker_overrides(monkeypatch):
    seen = []

    def _fake_run_worker_loop(*, worker_id, stop_event, task_type=None, exclude_task_types=None, **_kwargs):
        seen.append((worker_id, task_type, tuple(exclude_task_types or ())))
        stop_event.wait(0.01)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=task_type,
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: None)

    runtime = V2RuntimeService(
        worker_count=2,
        fetch_worker_count=3,
        resolve_worker_count=2,
        enable_scheduler=False,
    )
    started = runtime.start()
    stopped = runtime.stop()

    assert started["fetch_worker_count"] == 3
    assert started["resolve_worker_count"] == 2
    assert ("v2-runtime:fetch:3", "fetch_article", ()) in seen
    assert ("v2-runtime:resolve:2", "resolve_url", ()) in seen


def test_v2_runtime_service_allows_explicit_canonicalize_worker_override(monkeypatch):
    seen = []

    def _fake_run_worker_loop(*, worker_id, stop_event, task_type=None, exclude_task_types=None, **_kwargs):
        seen.append((worker_id, task_type, tuple(exclude_task_types or ())))
        stop_event.wait(0.01)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=task_type,
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: None)

    runtime = V2RuntimeService(
        worker_count=1,
        canonicalize_worker_count=2,
        enable_scheduler=False,
    )
    started = runtime.start()
    stopped = runtime.stop()

    assert started["canonicalize_worker_count"] == 2
    assert ("v2-runtime:canonicalize:1", "canonicalize", ()) in seen
    assert ("v2-runtime:canonicalize:2", "canonicalize", ()) in seen
    assert stopped["canonicalize_worker_count"] == 2
    assert all(worker["summary"]["stop_reason"] == "stopped" for worker in stopped["workers"])


def test_v2_runtime_service_restarts_dead_worker_threads(monkeypatch):
    call_counts = {}

    def _fake_run_worker_loop(*, worker_id, stop_event, **_kwargs):
        count = call_counts.get(worker_id, 0) + 1
        call_counts[worker_id] = count
        if count == 1:
            raise RuntimeError("boom")
        stop_event.wait(0.01)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=None,
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: None)

    runtime = V2RuntimeService(worker_count=1, task_type="fetch_article", enable_scheduler=False)
    runtime.start()
    time.sleep(0.02)
    runtime.tick()
    time.sleep(0.02)
    stopped = runtime.stop()

    assert call_counts["v2-runtime:1"] == 2
    assert stopped["workers"][0]["summary"]["stop_reason"] == "stopped"


def test_v2_runtime_service_tick_recovers_expired_leases(monkeypatch):
    def _fake_run_worker_loop(*, worker_id, stop_event, **_kwargs):
        stop_event.wait(0.01)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=None,
        )

    recovered = []

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: None)

    runtime = V2RuntimeService(
        worker_count=1,
        enable_scheduler=False,
        lease_recovery_interval_seconds=0.01,
    )
    runtime.start()
    monkeypatch.setattr(runtime, "_recover_expired_leases", lambda: recovered.append(True) or 1)
    runtime._last_lease_recovery_monotonic = 0.0
    runtime.tick()
    runtime.stop()

    assert recovered == [True]


def test_v2_runtime_service_skips_prewarm_for_fetch_only_workers(monkeypatch):
    prewarmed = []

    def _fake_run_worker_loop(*, worker_id, stop_event, **_kwargs):
        stop_event.wait(0.01)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type="fetch_article",
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)
    monkeypatch.setattr("src.edu_cti_v2.runtime._prewarm_ml_models", lambda: prewarmed.append(True))

    runtime = V2RuntimeService(worker_count=1, task_type="fetch_article", enable_scheduler=False)
    runtime.start()
    runtime.stop()

    assert prewarmed == []
