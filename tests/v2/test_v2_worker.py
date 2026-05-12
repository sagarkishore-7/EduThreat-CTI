from threading import Event
from unittest.mock import Mock

from src.edu_cti_v2.worker import run_worker_loop


class _FakeSession:
    def __init__(self):
        self.committed = 0
        self.rolled_back = 0

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1


class _FakeSessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def test_worker_loop_drains_until_idle():
    session = _FakeSession()

    def _session_factory():
        return _FakeSessionContext(session)

    runtime = Mock()
    runtime.lease_next_task.side_effect = ["task-1", "task-2", None]
    runtime.process_leased_task.side_effect = [object(), object()]

    summary = run_worker_loop(
        session_factory=_session_factory,
        runtime=runtime,
        worker_id="worker-1",
        stop_when_idle=True,
        poll_interval=0,
        lease_seconds=60,
    )

    assert summary.stop_reason == "idle"
    assert summary.processed_tasks == 2
    assert summary.idle_polls == 1
    assert session.committed == 5


def test_worker_loop_stops_after_max_tasks():
    session = _FakeSession()

    def _session_factory():
        return _FakeSessionContext(session)

    runtime = Mock()
    runtime.lease_next_task.side_effect = ["task-1", "task-2"]
    runtime.process_leased_task.side_effect = [object(), object()]

    summary = run_worker_loop(
        session_factory=_session_factory,
        runtime=runtime,
        worker_id="worker-1",
        max_tasks=1,
        stop_when_idle=False,
        poll_interval=0,
        lease_seconds=60,
    )

    assert summary.stop_reason == "max_tasks"
    assert summary.processed_tasks == 1
    assert summary.idle_polls == 0
    assert session.committed == 2


def test_worker_loop_stops_when_stop_event_is_set():
    session = _FakeSession()
    stop_event = Event()

    def _session_factory():
        return _FakeSessionContext(session)

    runtime = Mock()
    runtime.lease_next_task.side_effect = [None]

    stop_event.set()
    summary = run_worker_loop(
        session_factory=_session_factory,
        runtime=runtime,
        worker_id="worker-1",
        stop_when_idle=False,
        poll_interval=0,
        lease_seconds=60,
        stop_event=stop_event,
    )

    assert summary.stop_reason == "stopped"
    assert summary.processed_tasks == 0
    assert summary.idle_polls == 0
    runtime.lease_next_task.assert_not_called()


def test_worker_loop_reuses_one_lease_heartbeat_for_multiple_tasks(monkeypatch):
    session = _FakeSession()

    def _session_factory():
        return _FakeSessionContext(session)

    runtime = Mock()
    runtime.lease_next_task.side_effect = ["task-1", "task-2", None]
    runtime.process_leased_task.side_effect = [object(), object()]

    seen = {"instances": 0}

    class _FakeHeartbeat:
        def __init__(self, *, session_factory, worker_id, lease_seconds):
            seen["instances"] += 1
            seen["worker_id"] = worker_id
            seen["lease_seconds"] = lease_seconds
            seen["activated"] = []
            seen["cleared"] = []
            seen["stopped"] = 0

        def activate(self, task_id):
            seen["activated"].append(task_id)

        def clear(self, task_id=None):
            seen["cleared"].append(task_id)

        def stop(self):
            seen["stopped"] += 1

    monkeypatch.setattr("src.edu_cti_v2.worker._ReusableLeaseHeartbeat", _FakeHeartbeat)

    summary = run_worker_loop(
        session_factory=_session_factory,
        runtime=runtime,
        worker_id="worker-1",
        stop_when_idle=True,
        poll_interval=0,
        lease_seconds=60,
    )

    assert summary.stop_reason == "idle"
    assert seen["instances"] == 1
    assert seen["worker_id"] == "worker-1"
    assert seen["lease_seconds"] == 60
    assert seen["activated"] == ["task-1", "task-2"]
    assert seen["cleared"] == ["task-1", "task-2"]
    assert seen["stopped"] == 1
