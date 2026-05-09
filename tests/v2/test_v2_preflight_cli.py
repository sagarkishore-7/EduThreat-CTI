import json

import pytest

from src.edu_cti_v2.preflight_cli import main, run_preflight


class _SessionContext:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def test_run_preflight_uses_session_factory_and_service():
    class _Service:
        def __init__(self):
            self.called_with = None

        def get_status(self, session):
            self.called_with = session
            return {"ready": True}

    session = object()
    service = _Service()

    result = run_preflight(
        session_factory=lambda: _SessionContext(session),
        service=service,
    )

    assert result == {"ready": True}
    assert service.called_with is session


def test_preflight_cli_prints_json_and_exits_zero(capsys, monkeypatch):
    monkeypatch.setattr(
        "src.edu_cti_v2.preflight_cli.run_preflight",
        lambda: {"ready": True, "warnings": []},
    )
    monkeypatch.setattr("sys.argv", ["eduthreat-v2-preflight"])

    main()

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["ready"] is True


def test_preflight_cli_exits_nonzero_when_require_ready_fails(monkeypatch):
    monkeypatch.setattr(
        "src.edu_cti_v2.preflight_cli.run_preflight",
        lambda: {"ready": False, "warnings": ["db"]},
    )
    monkeypatch.setattr("sys.argv", ["eduthreat-v2-preflight", "--require-ready"])

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 1

