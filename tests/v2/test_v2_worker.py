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
    runtime.process_next_task.side_effect = [object(), object(), None]

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
    assert session.committed == 3


def test_worker_loop_stops_after_max_tasks():
    session = _FakeSession()

    def _session_factory():
        return _FakeSessionContext(session)

    runtime = Mock()
    runtime.process_next_task.side_effect = [object(), object()]

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
    assert session.committed == 1
