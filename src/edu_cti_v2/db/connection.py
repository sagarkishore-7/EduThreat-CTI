"""SQLAlchemy engine and session helpers for v2."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from .config import V2DatabaseSettings


def create_engine_from_settings(settings: Optional[V2DatabaseSettings] = None) -> Engine:
    """Create a SQLAlchemy engine from v2 settings."""
    settings = settings or V2DatabaseSettings.from_env()
    engine = create_engine(
        settings.database_url,
        echo=settings.echo_sql,
        future=True,
        pool_pre_ping=True,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_timeout=settings.pool_timeout,
        pool_recycle=settings.pool_recycle,
        connect_args={
            "options": (
                f"-c statement_timeout={settings.statement_timeout_ms} "
                f"-c application_name={settings.app_name}"
            )
        } if settings.is_postgres else {},
    )

    if settings.is_postgres:
        @event.listens_for(engine, "connect")
        def _configure_postgres_connection(dbapi_connection, _connection_record) -> None:
            with dbapi_connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")

    return engine


def create_session_factory(
    settings: Optional[V2DatabaseSettings] = None,
) -> sessionmaker[Session]:
    """Create a reusable session factory."""
    engine = create_engine_from_settings(settings)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
