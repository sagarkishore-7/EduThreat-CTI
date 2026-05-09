from datetime import datetime, timezone

from src.edu_cti_v2.services.preflight import V2PreflightService


class _Result:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value

    def scalar_one_or_none(self):
        return self.value


class _Session:
    def __init__(self, *, now_value, revision_value, fail_now=False):
        self.now_value = now_value
        self.revision_value = revision_value
        self.fail_now = fail_now

    def execute(self, statement):
        sql = str(statement)
        if "SELECT NOW()" in sql:
            if self.fail_now:
                raise RuntimeError("db down")
            return _Result(self.now_value)
        if "FROM alembic_version" in sql:
            return _Result(self.revision_value)
        raise AssertionError(f"unexpected SQL: {sql}")


def test_preflight_service_reports_ready_when_db_revision_and_llm_are_configured(monkeypatch):
    monkeypatch.setenv("OLLAMA_API_KEY", "key")
    monkeypatch.setenv("EDUTHREAT_ADMIN_API_KEY", "admin-key")
    session = _Session(
        now_value=datetime(2026, 5, 9, tzinfo=timezone.utc),
        revision_value="20250509_0001",
    )

    service = V2PreflightService()
    status = service.get_status(session)

    assert status["ready"] is True
    assert status["database"]["connected"] is True
    assert status["database"]["current_revision"] == "20250509_0001"
    assert status["integrations"]["ollama"]["configured"] is True


def test_preflight_service_reports_warnings_when_db_or_integrations_are_missing(monkeypatch):
    monkeypatch.delenv("OLLAMA_API_KEY", raising=False)
    monkeypatch.delenv("OXYLABS_USERNAME", raising=False)
    monkeypatch.delenv("OXYLABS_PASSWORD", raising=False)
    monkeypatch.delenv("EDUTHREAT_ADMIN_API_KEY", raising=False)
    monkeypatch.delenv("EDUTHREAT_ADMIN_PASSWORD_HASH", raising=False)
    session = _Session(now_value=None, revision_value=None, fail_now=True)

    service = V2PreflightService()
    status = service.get_status(session)

    assert status["ready"] is False
    assert status["database"]["connected"] is False
    assert any("Database connectivity failed." == warning for warning in status["warnings"])
    assert any("OLLAMA_API_KEY is not configured." == warning for warning in status["warnings"])
