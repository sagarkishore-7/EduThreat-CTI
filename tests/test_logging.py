"""Tests for the structlog-based central logging configuration.

These guard the production-grade behaviour: JSON output in prod, context
propagation so a single task/incident can be traced across stages, third-party
noise suppression (including the scrapling double-print), and message truncation.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from src.edu_cti.core import logging_utils
from src.edu_cti.core.logging_utils import (
    bind_log_context,
    clear_log_context,
    setup_logging,
    unbind_log_context,
)


@pytest.fixture(autouse=True)
def _reset_context():
    clear_log_context()
    yield
    clear_log_context()


def _capture(level="INFO", log_format="json"):
    """Configure logging and return a StringIO bound to the root stream handler."""
    setup_logging(level=level, log_format=log_format)
    buf = io.StringIO()
    logging.getLogger().handlers[0].stream = buf
    return buf


def test_json_mode_emits_parseable_lines():
    buf = _capture(log_format="json")
    logging.getLogger("edu_cti.test").info("incident_saved")
    line = buf.getvalue().strip().splitlines()[-1]
    obj = json.loads(line)
    assert obj["event"] == "incident_saved"
    assert obj["level"] == "info"
    assert "timestamp" in obj


def test_bound_context_appears_on_stdlib_logger():
    buf = _capture(log_format="json")
    bind_log_context(task_id="T1", run_id="R1", source="konbriefing")
    logging.getLogger("edu_cti.test").info("task_started")
    obj = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert obj["task_id"] == "T1"
    assert obj["run_id"] == "R1"
    assert obj["source"] == "konbriefing"


def test_extra_fields_render():
    buf = _capture(log_format="json")
    logging.getLogger("edu_cti.test").info("task_completed", extra={"elapsed_ms": 412})
    obj = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert obj["elapsed_ms"] == 412


def test_unbind_removes_only_named_keys():
    buf = _capture(log_format="json")
    bind_log_context(task_id="T1", source="konbriefing")
    unbind_log_context("source")
    logging.getLogger("edu_cti.test").info("after_unbind")
    obj = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert obj["task_id"] == "T1"
    assert "source" not in obj


def test_none_context_values_dropped():
    buf = _capture(log_format="json")
    bind_log_context(task_id="T1", run_id=None)
    logging.getLogger("edu_cti.test").info("evt")
    obj = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert obj["task_id"] == "T1"
    assert "run_id" not in obj


def test_third_party_noise_suppressed():
    setup_logging(log_format="json")
    # WARNING or quieter. transformers is driven to ERROR by set_verbosity_error()
    # when the library is installed, so assert the level is at least WARNING.
    for name in ("httpx", "httpcore", "urllib3", "gliner", "transformers"):
        assert logging.getLogger(name).level >= logging.WARNING


def test_scrapling_double_print_disabled():
    setup_logging(log_format="json")
    scrapling = logging.getLogger("scrapling")
    assert scrapling.propagate is False
    assert scrapling.handlers == []


def test_oversized_message_truncated():
    buf = _capture(log_format="json")
    logging.getLogger("edu_cti.test").info("X" * 5000)
    obj = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert len(obj["event"]) < 5000
    assert obj["event"].endswith("[truncated]")


def test_console_mode_renders_without_error():
    buf = _capture(log_format="console")
    logging.getLogger("edu_cti.test").warning("retrying_fetch")
    out = buf.getvalue()
    assert "retrying_fetch" in out


def test_per_logger_level_override(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL_some_chatty_lib", "ERROR")
    setup_logging(log_format="json")
    assert logging.getLogger("some_chatty_lib").level == logging.ERROR


def test_resolve_format_explicit_env_wins(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    assert logging_utils._resolve_log_format() == "json"
    monkeypatch.setenv("LOG_FORMAT", "console")
    assert logging_utils._resolve_log_format() == "console"


def test_resolve_format_auto_by_tty(monkeypatch):
    """Without an explicit LOG_FORMAT, auto-detect by whether stderr is a TTY:
    interactive terminal -> console, piped/deployed -> json."""
    monkeypatch.delenv("LOG_FORMAT", raising=False)

    class _FakeStderr:
        def __init__(self, tty):
            self._tty = tty

        def isatty(self):
            return self._tty

    monkeypatch.setattr(logging_utils.sys, "stderr", _FakeStderr(True))
    assert logging_utils._resolve_log_format() == "console"
    monkeypatch.setattr(logging_utils.sys, "stderr", _FakeStderr(False))
    assert logging_utils._resolve_log_format() == "json"
