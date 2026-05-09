import json

import pytest

from src.edu_cti_v2.orchestrator_cli import main


def test_orchestrator_cli_runs_plan_and_prints_json(capsys, monkeypatch):
    captured = {}

    class _Service:
        def run_plan(self, **kwargs):
            captured.update(kwargs)
            return {"ok": True, "plan_name": kwargs["plan_name"]}

    monkeypatch.setattr("src.edu_cti_v2.orchestrator_cli.V2OrchestrationService", lambda: _Service())
    monkeypatch.setattr(
        "sys.argv",
        [
            "eduthreat-v2-run-plan",
            "historical_max_coverage",
            "--worker-id",
            "cli-test",
            "--worker-max-tasks",
            "42",
            "--no-drain",
            "--include-paid-rss",
        ],
    )

    main()

    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert captured == {
        "plan_name": "historical_max_coverage",
        "worker_id": "cli-test",
        "worker_max_tasks": 42,
        "drain_tasks": False,
        "collect_overrides": {"include_paid_rss": True},
    }


def test_orchestrator_cli_rejects_conflicting_paid_flags(monkeypatch):
    monkeypatch.setattr(
        "sys.argv",
        [
            "eduthreat-v2-run-plan",
            "historical_max_coverage",
            "--include-paid-rss",
            "--exclude-paid-rss",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 2
