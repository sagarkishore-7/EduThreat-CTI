"""Regression tests for the CLI orchestrator."""

from types import SimpleNamespace
from unittest.mock import patch

from src.edu_cti.pipeline import orchestrator


def test_historical_command_does_not_force_csv_export():
    args = SimpleNamespace(
        skip_enrich=False,
        enrich_limit=None,
        rate_limit_delay=2.0,
        export_csv=False,
        log_level="INFO",
    )

    with patch.object(orchestrator, "cmd_ingest", return_value=0), patch.object(
        orchestrator, "cmd_enrich"
    ) as mock_enrich:
        orchestrator.cmd_historical(args)

    forwarded_args = mock_enrich.call_args.args[0]
    assert forwarded_args.export_csv is False


def test_daily_command_does_not_force_csv_export():
    args = SimpleNamespace(
        skip_enrich=False,
        enrich_limit=None,
        rate_limit_delay=2.0,
        export_csv=False,
        log_level="INFO",
    )

    with patch.object(orchestrator, "cmd_ingest", return_value=0), patch.object(
        orchestrator, "cmd_enrich"
    ) as mock_enrich:
        orchestrator.cmd_daily(args)

    forwarded_args = mock_enrich.call_args.args[0]
    assert forwarded_args.export_csv is False


def test_historical_command_preserves_explicit_csv_export_request():
    args = SimpleNamespace(
        skip_enrich=False,
        enrich_limit=None,
        rate_limit_delay=2.0,
        export_csv=True,
        log_level="INFO",
    )

    with patch.object(orchestrator, "cmd_ingest", return_value=0), patch.object(
        orchestrator, "cmd_enrich"
    ) as mock_enrich:
        orchestrator.cmd_historical(args)

    forwarded_args = mock_enrich.call_args.args[0]
    assert forwarded_args.export_csv is True


def test_daily_command_preserves_explicit_csv_export_request():
    args = SimpleNamespace(
        skip_enrich=False,
        enrich_limit=None,
        rate_limit_delay=2.0,
        export_csv=True,
        log_level="INFO",
    )

    with patch.object(orchestrator, "cmd_ingest", return_value=0), patch.object(
        orchestrator, "cmd_enrich"
    ) as mock_enrich:
        orchestrator.cmd_daily(args)

    forwarded_args = mock_enrich.call_args.args[0]
    assert forwarded_args.export_csv is True


def test_parser_accepts_optional_csv_export_for_historical_and_daily():
    parser = orchestrator.build_parser()

    historical_args = parser.parse_args(["historical", "--export-csv"])
    daily_args = parser.parse_args(["daily", "--export-csv"])

    assert historical_args.export_csv is True
    assert daily_args.export_csv is True
