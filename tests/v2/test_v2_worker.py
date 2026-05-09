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


def test_worker_loop_starts_and_stops_lease_heartbeat(monkeypatch):
    session = _FakeSession()

    def _session_factory():
        return _FakeSessionContext(session)

    runtime = Mock()
    runtime.lease_next_task.side_effect = ["task-1", None]
    runtime.process_leased_task.side_effect = [object()]

    seen = {}

    class _FakeThread:
        def __init__(self):
            self.join_calls = []

        def join(self, timeout=None):
            self.join_calls.append(timeout)

    def _fake_start_lease_heartbeat(*, session_factory, task_id, worker_id, lease_seconds):
        seen["task_id"] = task_id
        seen["worker_id"] = worker_id
        seen["lease_seconds"] = lease_seconds
        stop_event = Event()
        thread = _FakeThread()
        seen["stop_event"] = stop_event
        seen["thread"] = thread
        return stop_event, thread

    monkeypatch.setattr("src.edu_cti_v2.worker._start_lease_heartbeat", _fake_start_lease_heartbeat)

    summary = run_worker_loop(
        session_factory=_session_factory,
        runtime=runtime,
        worker_id="worker-1",
        stop_when_idle=True,
        poll_interval=0,
        lease_seconds=60,
    )

    assert summary.stop_reason == "idle"
    assert seen["task_id"] == "task-1"
    assert seen["worker_id"] == "worker-1"
    assert seen["lease_seconds"] == 60
    assert seen["stop_event"].is_set() is True
    assert seen["thread"].join_calls
