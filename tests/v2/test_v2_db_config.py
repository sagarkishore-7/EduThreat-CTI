import os

from src.edu_cti_v2.db.config import V2DatabaseSettings, build_database_url


def test_build_database_url_uses_psycopg_driver():
    url = build_database_url(
        user="postgres",
        password="secret",
        host="db.internal",
        port=5432,
        database="eduthreat_cti_v2",
    )

    assert url == "postgresql+psycopg://postgres:secret@db.internal:5432/eduthreat_cti_v2"


def test_v2_database_settings_from_env(monkeypatch):
    monkeypatch.setenv("EDU_CTI_V2_DATABASE_URL", "postgresql+psycopg://u:p@localhost:5432/db")
    monkeypatch.setenv("EDU_CTI_V2_DB_ECHO", "1")
    monkeypatch.setenv("EDU_CTI_V2_DB_POOL_SIZE", "12")
    monkeypatch.setenv("EDU_CTI_V2_TASK_LEASE_SECONDS", "480")

    settings = V2DatabaseSettings.from_env()

    assert settings.database_url == "postgresql+psycopg://u:p@localhost:5432/db"
    assert settings.echo_sql is True
    assert settings.pool_size == 12
    assert settings.task_lease_seconds == 480
    assert settings.is_postgres is True
    assert settings.masked_database_url.endswith("@localhost:5432/db")


def test_v2_database_settings_default_url_is_postgres(monkeypatch):
    for key in list(os.environ):
        if key.startswith("EDU_CTI_V2_DB_") or key == "EDU_CTI_V2_DATABASE_URL":
            monkeypatch.delenv(key, raising=False)

    settings = V2DatabaseSettings.from_env()

    assert settings.database_url.startswith("postgresql+psycopg://")
