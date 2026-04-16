"""Regression tests for paid RSS/search ingestion wiring."""

import sys
import types
from unittest.mock import patch

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

from src.edu_cti.pipeline.manager import PipelineManager, PipelineRun


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
