"""Preflight checks for bringing up the Postgres-backed v2 runtime."""

from __future__ import annotations

import os
from typing import Any, Callable, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.edu_cti_v2.db import V2DatabaseSettings


def _env_flag(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


class V2PreflightService:
    """Inspect DB connectivity, migration state, and runtime credentials."""

    def __init__(self, *, settings_factory: Optional[Callable[[], V2DatabaseSettings]] = None) -> None:
        self.settings_factory = settings_factory or V2DatabaseSettings.from_env

    def get_status(self, session: Session) -> dict[str, Any]:
        settings = self.settings_factory()

        db_ok = False
        current_revision = None
        db_error = None
        server_now = None

        try:
            server_now = session.execute(text("SELECT NOW()")).scalar_one()
            db_ok = True
        except Exception as exc:
            db_error = str(exc)

        if db_ok:
            try:
                current_revision = session.execute(
                    text("SELECT version_num FROM alembic_version LIMIT 1")
                ).scalar_one_or_none()
            except Exception:
                current_revision = None

        integrations = {
            "ollama": {
                "configured": bool(os.environ.get("OLLAMA_API_KEY")),
                "host": os.environ.get("OLLAMA_HOST", "https://ollama.com"),
                "model": os.environ.get("OLLAMA_MODEL", "qwen3-coder:480b-cloud"),
            },
            "oxylabs": {
                "configured": bool(
                    os.environ.get("OXYLABS_USERNAME") and os.environ.get("OXYLABS_PASSWORD")
                ),
                "historical_enabled": _env_flag("ENABLE_OXYLABS_NEWS_HISTORICAL", "1"),
                "daily_enabled": _env_flag("ENABLE_OXYLABS_NEWS_DAILY", "0"),
            },
            "admin_auth": {
                "api_key_configured": bool(os.environ.get("EDUTHREAT_ADMIN_API_KEY")),
                "password_hash_configured": bool(os.environ.get("EDUTHREAT_ADMIN_PASSWORD_HASH")),
            },
        }

        warnings: list[str] = []
        if not db_ok:
            warnings.append("Database connectivity failed.")
        if current_revision is None:
            warnings.append("Alembic revision not detected; run eduthreat-v2-migrate upgrade head.")
        if not integrations["ollama"]["configured"]:
            warnings.append("OLLAMA_API_KEY is not configured.")
        if not integrations["oxylabs"]["configured"]:
            warnings.append("OXYLABS_USERNAME/OXYLABS_PASSWORD are not configured.")
        if not (
            integrations["admin_auth"]["api_key_configured"]
            or integrations["admin_auth"]["password_hash_configured"]
        ):
            warnings.append("Admin auth is using development-default credentials.")

        return {
            "ready": db_ok and current_revision is not None and integrations["ollama"]["configured"],
            "database": {
                "connected": db_ok,
                "error": db_error,
                "current_revision": current_revision,
                "server_time": server_now.isoformat() if server_now is not None else None,
                "masked_database_url": settings.masked_database_url,
                "pool_size": settings.pool_size,
                "max_overflow": settings.max_overflow,
                "statement_timeout_ms": settings.statement_timeout_ms,
            },
            "integrations": integrations,
            "warnings": warnings,
        }
