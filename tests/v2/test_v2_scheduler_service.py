from unittest.mock import Mock, patch

import schedule

from src.edu_cti_v2.services.scheduler import V2SchedulerService


def test_scheduler_service_lists_default_jobs():
    service = V2SchedulerService(
        orchestration_service=Mock(),
        scheduler=schedule.Scheduler(),
        poll_interval_seconds=0,
    )

    jobs = service.list_jobs()

    names = {job["name"] for job in jobs}
    assert "rss_fast_refresh" in names
    assert "incremental_refresh" in names


def test_scheduler_service_trigger_job_runs_plan_synchronously():
    orchestration = Mock()
    orchestration.run_plan.return_value = {"run_id": "plan-1"}
    service = V2SchedulerService(
        orchestration_service=orchestration,
        scheduler=schedule.Scheduler(),
        poll_interval_seconds=0,
    )

    result = service.trigger_job("rss_fast_refresh", background=False)

    assert result["job_name"] == "rss_fast_refresh"
    assert result["status"] == "completed"
    orchestration.run_plan.assert_called_once_with(
        plan_name="rss_fast_refresh",
        worker_id="v2-scheduler:rss_fast_refresh",
    )


def test_scheduler_service_start_configures_jobs_and_stop_resets_running_flag():
    orchestration = Mock()
    service = V2SchedulerService(
        orchestration_service=orchestration,
        scheduler=schedule.Scheduler(),
        poll_interval_seconds=0,
    )

    with patch("threading.Thread") as thread_cls:
        thread_instance = Mock()
        thread_cls.return_value = thread_instance
        started = service.start()

    assert started["running"] is True
    assert len(service.scheduler.jobs) == 2
    thread_instance.start.assert_called_once()

    thread_instance.is_alive.return_value = False
    stopped = service.stop()
    assert stopped["running"] is False

