"""Tests for pipeline manager execution and regression behavior."""

import sys
import types
from unittest.mock import patch

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
