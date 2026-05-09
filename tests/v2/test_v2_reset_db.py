from unittest.mock import Mock

from src.edu_cti_v2 import reset_db


def test_reset_database_drops_known_tables_and_rebuilds(monkeypatch):
    executed = []

    class _ConnectionContext:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def exec_driver_sql(self, statement):
            executed.append(statement)

    engine = Mock()
    engine.begin.return_value = _ConnectionContext()

    monkeypatch.setattr(reset_db, "create_engine_from_settings", lambda _settings=None: engine)
    monkeypatch.setattr(reset_db, "build_alembic_config", lambda: "cfg")
    upgrade = Mock()
    monkeypatch.setattr(
        reset_db.importlib,
        "import_module",
        lambda _name: type("_AlembicCommand", (), {"upgrade": upgrade})(),
    )

    reset_db.reset_database()

    assert executed[0] == "DROP TABLE IF EXISTS analytics_refresh_state CASCADE"
    assert executed[-1] == "DROP TABLE IF EXISTS alembic_version CASCADE"
    upgrade.assert_called_once_with("cfg", "head")
    engine.dispose.assert_called_once()
