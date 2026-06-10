"""Configuration helpers for the Postgres-backed v2 runtime."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote_plus

from src.edu_cti_v2.env import get_env, get_flag, get_int


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
        # New unprefixed names with legacy EDU_CTI_V2_* fallbacks; DB_URL also
        # falls back to the platform-standard DATABASE_URL.
        driver = get_env("DB_DRIVER", "EDU_CTI_V2_DB_DRIVER", default="psycopg")
        database_url = get_env("DB_URL", "EDU_CTI_V2_DATABASE_URL", "DATABASE_URL")
        if not database_url:
            database_url = build_database_url(
                user=get_env("DB_USER", "EDU_CTI_V2_DB_USER", default="postgres"),
                password=get_env("DB_PASSWORD", "EDU_CTI_V2_DB_PASSWORD", default="postgres"),
                host=get_env("DB_HOST", "EDU_CTI_V2_DB_HOST", default="localhost"),
                port=get_int("DB_PORT", "EDU_CTI_V2_DB_PORT", default=5432),
                database=get_env("DB_NAME", "EDU_CTI_V2_DB_NAME", default="eduthreat_cti_v2"),
                driver=driver,
            )
        database_url = normalize_database_url(database_url, default_driver=driver)
        alembic_database_url = normalize_database_url(
            get_env("ALEMBIC_DATABASE_URL", default=database_url),
            default_driver=driver,
        )

        return cls(
            database_url=database_url,
            alembic_database_url=alembic_database_url,
            echo_sql=get_flag("DB_ECHO", "EDU_CTI_V2_DB_ECHO", default=False),
            pool_size=get_int("DB_POOL_SIZE", "EDU_CTI_V2_DB_POOL_SIZE", default=5),
            max_overflow=get_int("DB_MAX_OVERFLOW", "EDU_CTI_V2_DB_MAX_OVERFLOW", default=5),
            pool_timeout=get_int("DB_POOL_TIMEOUT", "EDU_CTI_V2_DB_POOL_TIMEOUT", default=10),
            pool_recycle=get_int("DB_POOL_RECYCLE", "EDU_CTI_V2_DB_POOL_RECYCLE", default=1800),
            statement_timeout_ms=get_int("DB_STATEMENT_TIMEOUT_MS", "EDU_CTI_V2_DB_STATEMENT_TIMEOUT_MS", default=30000),
            app_name=get_env("DB_APP_NAME", "EDU_CTI_V2_DB_APP_NAME", default="eduthreat-cti-v2"),
            schema_name=get_env("DB_SCHEMA", "EDU_CTI_V2_DB_SCHEMA", default="public"),
            task_lease_seconds=get_int("TASK_LEASE_SECONDS", "EDU_CTI_V2_TASK_LEASE_SECONDS", default=300),
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
