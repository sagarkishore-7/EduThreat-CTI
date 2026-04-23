import sqlite3
from unittest.mock import Mock, patch

import pytest

from src.edu_cti.core.db import is_sqlite_lock_error, run_with_sqlite_lock_retry


def test_is_sqlite_lock_error_matches_locked_operational_errors():
    assert is_sqlite_lock_error(sqlite3.OperationalError("database is locked")) is True
    assert is_sqlite_lock_error(sqlite3.OperationalError("database table is locked")) is True
    assert is_sqlite_lock_error(sqlite3.OperationalError("database schema is locked")) is True
    assert is_sqlite_lock_error(sqlite3.OperationalError("no such table")) is False


def test_run_with_sqlite_lock_retry_retries_transient_lock():
    conn = Mock()
    calls = {"count": 0}

    def _work():
        calls["count"] += 1
        if calls["count"] < 3:
            raise sqlite3.OperationalError("database is locked")
        return "ok"

    with patch("src.edu_cti.core.db.time.sleep") as sleep:
        result = run_with_sqlite_lock_retry(
            conn,
            _work,
            operation="test write",
            max_attempts=4,
            base_delay=0.0,
            max_delay=0.0,
        )

    assert result == "ok"
    assert calls["count"] == 3
    assert conn.rollback.call_count == 2
    assert sleep.call_count == 2


def test_run_with_sqlite_lock_retry_does_not_retry_non_lock_errors():
    conn = Mock()

    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        run_with_sqlite_lock_retry(
            conn,
            lambda: (_ for _ in ()).throw(sqlite3.OperationalError("no such table: incidents")),
            operation="bad query",
            max_attempts=3,
        )

    conn.rollback.assert_not_called()
