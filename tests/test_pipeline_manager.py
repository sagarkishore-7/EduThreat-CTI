"""Tests for pipeline manager execution and regression behavior."""

import io
import sys
import types
from unittest.mock import call, patch

import pytest

# Local shells may not have APScheduler installed; CI does via requirements.txt.
if "apscheduler.schedulers.background" not in sys.modules:
    apscheduler = types.ModuleType("apscheduler")
    schedulers = types.ModuleType("apscheduler.schedulers")
    background = types.ModuleType("apscheduler.schedulers.background")

    class BackgroundScheduler:  # pragma: no cover - tiny compatibility stub
        def __init__(self, *args, **kwargs):
            pass

        def add_job(self, *args, **kwargs):
            return None

        def get_jobs(self):
            return []

        def remove_all_jobs(self):
            return None

        def start(self):
            return None

        def shutdown(self, *args, **kwargs):
            return None

    background.BackgroundScheduler = BackgroundScheduler
    sys.modules["apscheduler"] = apscheduler
    sys.modules["apscheduler.schedulers"] = schedulers
    sys.modules["apscheduler.schedulers.background"] = background

from src.edu_cti.pipeline.manager import PipelineManager, PipelineRun, RunStatus


class _ImmediateThread:
    """Thread stub that runs work synchronously for deterministic tests."""

    def __init__(self, target=None, args=None, kwargs=None, **_ignored):
        self._target = target
        self._args = args or ()
        self._kwargs = kwargs or {}

    def start(self):
        if self._target:
            self._target(*self._args, **self._kwargs)

    def join(self, timeout=None):
        return None

    def is_alive(self):
        return False


@pytest.fixture(autouse=True)
def reset_pipeline_manager_singleton():
    """Ensure singleton state does not leak between tests."""
    PipelineManager._instance = None
    yield
    PipelineManager._instance = None


def test_start_phase_invalidates_cache_before_and_after_run():
    manager = PipelineManager()

    with patch("src.edu_cti.pipeline.manager.threading.Thread", _ImmediateThread), patch(
        "src.edu_cti.api.cache.cache_invalidate"
    ) as mock_cache_invalidate, patch.object(
        manager, "_dispatch_phase", return_value={"ok": True}
    ):
        run = manager.start_phase("enrich", {})

    assert run.status == RunStatus.COMPLETED
    assert mock_cache_invalidate.call_count == 2


def test_run_historical_stops_when_only_unactionable_incidents_remain():
    manager = PipelineManager()
    run = PipelineRun("historical-run", "historical", {})

    with patch.object(
        manager, "_run_ingest", return_value={"new_incidents": 0}
    ), patch.object(
        manager, "_run_enrich", side_effect=AssertionError("enrichment should not run")
    ), patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        return_value={"unenriched_incidents": 8, "ready_for_enrichment": 0},
    ), patch(
        "src.edu_cti.pipeline.phase2.csv_export.export_enriched_dataset"
    ) as mock_export:
        result = manager._run_historical(run, {})

    assert result["enrich_rounds"] == 0
    assert result["cancelled"] is False
    assert run.progress["percent"] == 100
    mock_export.assert_not_called()


def test_run_historical_enables_paid_rss_collection_by_default():
    manager = PipelineManager()
    run = PipelineRun("historical-run", "historical", {})
    captured_params = {}

    def _fake_run_ingest(_run, params, **_kwargs):
        captured_params.update(params)
        return {"new_incidents": 0}

    with patch("src.edu_cti.pipeline.manager.threading.Thread", _ImmediateThread), patch.object(
        manager, "_run_ingest", side_effect=_fake_run_ingest
    ), patch.object(
        manager, "_run_enrich", side_effect=AssertionError("enrichment should not run")
    ), patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        return_value={"unenriched_incidents": 0, "ready_for_enrichment": 0},
    ), patch(
        "src.edu_cti.pipeline.phase2.csv_export.export_enriched_dataset"
    ):
        result = manager._run_historical(run, {})

    assert result["enrich_rounds"] == 0
    assert captured_params["include_paid_rss"] is True


def test_run_daily_stops_when_only_unactionable_incidents_remain():
    manager = PipelineManager()
    run = PipelineRun("daily-run", "daily", {})

    with patch.object(
        manager, "_run_ingest", return_value={"new_incidents": 0}
    ), patch.object(
        manager, "_run_enrich", side_effect=AssertionError("enrichment should not run")
    ), patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        return_value={"unenriched_incidents": 4, "ready_for_enrichment": 0},
    ), patch(
        "src.edu_cti.pipeline.phase2.csv_export.export_enriched_dataset"
    ) as mock_export:
        result = manager._run_daily(run, {})

    assert result["enrich_rounds"] == 0
    assert run.progress["percent"] == 100
    mock_export.assert_not_called()


def test_run_historical_exports_only_when_explicitly_requested():
    manager = PipelineManager()
    run = PipelineRun("historical-run", "historical", {})

    with patch.object(
        manager, "_run_ingest", return_value={"new_incidents": 0}
    ), patch.object(
        manager, "_run_enrich", side_effect=AssertionError("enrichment should not run")
    ), patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        return_value={"unenriched_incidents": 0, "ready_for_enrichment": 0},
    ), patch(
        "src.edu_cti.pipeline.phase2.csv_export.export_enriched_dataset"
    ) as mock_export:
        manager._run_historical(run, {"export_csv": True})

    mock_export.assert_called_once()


def test_run_daily_exports_only_when_explicitly_requested():
    manager = PipelineManager()
    run = PipelineRun("daily-run", "daily", {})

    with patch.object(
        manager, "_run_ingest", return_value={"new_incidents": 0}
    ), patch.object(
        manager, "_run_enrich", side_effect=AssertionError("enrichment should not run")
    ), patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        return_value={"unenriched_incidents": 0, "ready_for_enrichment": 0},
    ), patch(
        "src.edu_cti.pipeline.phase2.csv_export.export_enriched_dataset"
    ) as mock_export:
        manager._run_daily(run, {"export_csv": True})

    mock_export.assert_called_once()


def test_scheduler_enrichment_does_not_request_csv_export():
    manager = PipelineManager()
    manager._scheduler_running = True

    with patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        return_value={"unenriched_incidents": 2, "ready_for_enrichment": 1},
    ), patch.object(manager, "_scheduler_run_job") as mock_run_job:
        manager._scheduler_run_enrich_if_needed()

    mock_run_job.assert_called_once_with(
        "enrich",
        {"rate_limit_delay": 2.0, "export_csv": False},
    )


def test_scheduler_catchup_uses_lightweight_jobs():
    manager = PipelineManager()
    manager._scheduler_running = True

    with patch.object(manager, "_scheduler_run_job") as mock_run_job, patch.object(
        manager, "_scheduler_run_enrich_if_needed"
    ) as mock_enrich:
        manager._scheduler_catchup()

    assert mock_run_job.call_args_list == [
        call("rss", {"max_age_days": 7}),
        call("ingest_source", {"group": "api"}),
    ]
    mock_enrich.assert_called_once_with()


def test_scheduler_daily_refresh_runs_sweep_after_daily_jobs():
    manager = PipelineManager()
    manager._scheduler_running = True

    with patch.object(manager, "_scheduler_run_job") as mock_run_job, patch.object(
        manager, "_scheduler_run_enrich_if_needed"
    ) as mock_enrich, patch.object(
        manager, "_scheduler_run_data_quality_sweep"
    ) as mock_sweep:
        manager._scheduler_run_daily_refresh()

    mock_run_job.assert_called_once_with("weekly", {"max_pages": 20})
    assert mock_enrich.call_count == 2
    mock_sweep.assert_called_once_with()
    assert manager._scheduler_last_runs["daily"] is not None


def test_auto_resume_interrupted_historical_run():
    with patch(
        "src.edu_cti.pipeline.manager._recover_interrupted_runs",
        return_value=[
            {
                "run_id": "old-run",
                "phase": "historical",
                "params": {"max_pages": 25},
                "started_at": "2026-05-01T10:00:00",
            }
        ],
    ), patch(
        "src.edu_cti.core.config.AUTO_RESUME_INTERRUPTED_PIPELINES",
        True,
        create=True,
    ), patch.object(
        PipelineManager,
        "start_phase",
        return_value=PipelineRun("new-run", "historical", {}),
    ) as mock_start:
        PipelineManager()

    mock_start.assert_called_once_with(
        "historical",
        {
            "max_pages": 25,
            "auto_resumed": True,
            "resumed_from_run_id": "old-run",
        },
    )


def test_run_historical_reduces_workers_after_memory_pause():
    manager = PipelineManager()
    run = PipelineRun("historical-run", "historical", {"workers": 3})
    worker_counts = []

    def _fake_run_enrich(_run, params, **_kwargs):
        worker_counts.append(params["workers"])
        if len(worker_counts) == 1:
            return {
                "memory_pause_requested": True,
                "memory_pause_reason": "RSS 2900 MB exceeded hard limit 2600 MB",
                "run_stats": {"enriched": 2},
                "enrichment_stats": {"ready_for_enrichment": 5, "unenriched_incidents": 5},
            }
        return {
            "memory_pause_requested": False,
            "run_stats": {"enriched": 1},
            "enrichment_stats": {"ready_for_enrichment": 0, "unenriched_incidents": 0},
        }

    stats_sequence = [
        {"unenriched_incidents": 5, "ready_for_enrichment": 5},
        {"unenriched_incidents": 5, "ready_for_enrichment": 5},
        {"unenriched_incidents": 0, "ready_for_enrichment": 0},
    ]

    with patch("src.edu_cti.pipeline.manager.threading.Thread", _ImmediateThread), patch.object(
        manager, "_run_ingest", return_value={"new_incidents": 0}
    ), patch.object(
        manager, "_run_one_enrich_batch", side_effect=_fake_run_enrich
    ), patch(
        "src.edu_cti.pipeline.manager._load_enrichment_stats",
        side_effect=stats_sequence,
    ), patch(
        "src.edu_cti.pipeline.manager.time.sleep"
    ):
        result = manager._run_historical(run, {"workers": 3})

    assert worker_counts == [3, 2]
    assert result["memory_pause_requested"] is False
    assert result["enrich_rounds"] == 2


def test_run_one_enrich_batch_launches_unbuffered_subprocess():
    manager = PipelineManager()
    run = PipelineRun("enrich-run", "enrich", {})
    captured = {}
    fake_stdout = io.StringIO()

    class _FakeConn:
        def close(self):
            return None

    class _FakeProc:
        def __init__(self):
            self.stdout = io.StringIO("child log line\n")
            self.returncode = 0
            self.pid = 4321
            self._poll_calls = 0

        def poll(self):
            self._poll_calls += 1
            if self._poll_calls == 1:
                return None
            self.returncode = 0
            return 0

        def terminate(self):
            self.returncode = -15

        def wait(self, timeout=None):
            return self.returncode

        def kill(self):
            self.returncode = -9

    def _fake_popen(cmd, stdout=None, stderr=None, text=None, bufsize=None, env=None):
        captured["cmd"] = cmd
        captured["stdout"] = stdout
        captured["stderr"] = stderr
        captured["text"] = text
        captured["bufsize"] = bufsize
        captured["env"] = env
        return _FakeProc()

    with patch("src.edu_cti.pipeline.manager.threading.Thread", _ImmediateThread), patch.object(
        sys, "stdout", fake_stdout
    ), patch(
        "src.edu_cti.core.db.get_connection", side_effect=[_FakeConn(), _FakeConn()]
    ), patch(
        "src.edu_cti.core.db.init_db"
    ), patch(
        "src.edu_cti.pipeline.phase2.storage.db.get_enrichment_stats",
        side_effect=[
            {"enriched_incidents": 10},
            {"enriched_incidents": 11},
        ],
    ), patch(
        "subprocess.Popen", side_effect=_fake_popen
    ), patch(
        "src.edu_cti.pipeline.manager.time.sleep"
    ):
        result = manager._run_one_enrich_batch(run, {"workers": 3})

    assert captured["cmd"][:3] == [sys.executable, "-u", "-m"]
    assert captured["cmd"][3] == "src.edu_cti.pipeline.phase2"
    assert captured["env"]["PYTHONUNBUFFERED"] == "1"
    assert result["run_stats"]["enriched"] == 1
    assert "[phase2] child log line" in fake_stdout.getvalue()
    assert any("[phase2] child log line" in line for line in run.logs)
