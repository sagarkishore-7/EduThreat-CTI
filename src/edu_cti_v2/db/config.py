"""Configuration helpers for the Postgres-backed v2 runtime."""

from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote_plus


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def build_database_url(
    *,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
    driver: str = "psycopg",
) -> str:
    """Build a SQLAlchemy Postgres URL from discrete settings."""
    return (
        f"postgresql+{driver}://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{database}"
    )


def normalize_database_url(database_url: str, *, default_driver: str = "psycopg") -> str:
    """Normalize plain Postgres URLs to the SQLAlchemy driver form we install."""
    prefix = "postgresql://"
    driver_prefix = "postgresql+"
    if database_url.startswith(prefix) and not database_url.startswith(driver_prefix):
        return database_url.replace(prefix, f"postgresql+{default_driver}://", 1)
    return database_url


@dataclass(frozen=True)
class V2DatabaseSettings:
    """Runtime settings for the v2 Postgres layer."""

    database_url: str
    alembic_database_url: str
    echo_sql: bool = False
    # Conservative per-worker pool so auto-scaled API workers stay well under
    # Postgres max_connections: workers × (pool_size + max_overflow) is the cap.
    pool_size: int = 5
    max_overflow: int = 5
    pool_timeout: int = 10
    pool_recycle: int = 1800
    statement_timeout_ms: int = 30000
    app_name: str = "eduthreat-cti-v2"
    schema_name: str = "public"
    task_lease_seconds: int = 300

    @classmethod
    def from_env(cls) -> "V2DatabaseSettings":
        database_url = os.environ.get("EDU_CTI_V2_DATABASE_URL")
        if not database_url:
            database_url = build_database_url(
                user=os.environ.get("EDU_CTI_V2_DB_USER", "postgres"),
                password=os.environ.get("EDU_CTI_V2_DB_PASSWORD", "postgres"),
                host=os.environ.get("EDU_CTI_V2_DB_HOST", "localhost"),
                port=int(os.environ.get("EDU_CTI_V2_DB_PORT", "5432")),
                database=os.environ.get("EDU_CTI_V2_DB_NAME", "eduthreat_cti_v2"),
                driver=os.environ.get("EDU_CTI_V2_DB_DRIVER", "psycopg"),
            )
        database_url = normalize_database_url(
            database_url,
            default_driver=os.environ.get("EDU_CTI_V2_DB_DRIVER", "psycopg"),
        )
        alembic_database_url = normalize_database_url(
            os.environ.get("ALEMBIC_DATABASE_URL", database_url),
            default_driver=os.environ.get("EDU_CTI_V2_DB_DRIVER", "psycopg"),
        )

        return cls(
            database_url=database_url,
            alembic_database_url=alembic_database_url,
            echo_sql=_env_flag("EDU_CTI_V2_DB_ECHO", "0"),
            pool_size=int(os.environ.get("EDU_CTI_V2_DB_POOL_SIZE", "5")),
            max_overflow=int(os.environ.get("EDU_CTI_V2_DB_MAX_OVERFLOW", "5")),
            pool_timeout=int(os.environ.get("EDU_CTI_V2_DB_POOL_TIMEOUT", "10")),
            pool_recycle=int(os.environ.get("EDU_CTI_V2_DB_POOL_RECYCLE", "1800")),
            statement_timeout_ms=int(os.environ.get("EDU_CTI_V2_DB_STATEMENT_TIMEOUT_MS", "30000")),
            app_name=os.environ.get("EDU_CTI_V2_DB_APP_NAME", "eduthreat-cti-v2"),
            schema_name=os.environ.get("EDU_CTI_V2_DB_SCHEMA", "public"),
            task_lease_seconds=int(os.environ.get("EDU_CTI_V2_TASK_LEASE_SECONDS", "300")),
        )

    @property
    def masked_database_url(self) -> str:
        if "://" not in self.database_url or "@" not in self.database_url:
            return self.database_url
        prefix, suffix = self.database_url.split("://", 1)
        creds, rest = suffix.split("@", 1)
        if ":" not in creds:
            return f"{prefix}://***@{rest}"
        user, _password = creds.split(":", 1)
        return f"{prefix}://{user}:***@{rest}"

    @property
    def is_postgres(self) -> bool:
        return self.database_url.startswith("postgresql")
