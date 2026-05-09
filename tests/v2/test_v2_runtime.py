import time

from src.edu_cti_v2.runtime import V2RuntimeService
from src.edu_cti_v2.worker import V2WorkerRunSummary


def test_v2_runtime_service_starts_and_stops_workers_and_scheduler(monkeypatch):
    seen = []

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

    def _fake_run_worker_loop(*, worker_id, stop_event, **_kwargs):
        seen.append(worker_id)
        stop_event.wait(0.05)
        return V2WorkerRunSummary(
            processed_tasks=0,
            idle_polls=0,
            stop_reason="stopped",
            worker_id=worker_id,
            task_type=None,
        )

    monkeypatch.setattr("src.edu_cti_v2.runtime.run_worker_loop", _fake_run_worker_loop)

    scheduler = _FakeScheduler()
    runtime = V2RuntimeService(worker_count=2, scheduler_service=scheduler)

    started = runtime.start()
    assert started["running"] is True
    assert scheduler.started == 1

    time.sleep(0.02)
    stopped = runtime.stop()
    assert stopped["running"] is False
    assert scheduler.stopped == 1
    assert seen == ["v2-runtime:1", "v2-runtime:2"]
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

    runtime = V2RuntimeService(worker_count=1, enable_scheduler=False)
    started = runtime.start()
    assert started["scheduler_enabled"] is False
    assert started["scheduler"] is None
    stopped = runtime.stop()
    assert stopped["scheduler"] is None


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

    runtime = V2RuntimeService(worker_count=1, enable_scheduler=False)
    runtime.start()
    time.sleep(0.02)
    runtime.tick()
    time.sleep(0.02)
    stopped = runtime.stop()

    assert call_counts["v2-runtime:1"] == 2
    assert stopped["workers"][0]["summary"]["stop_reason"] == "stopped"
