"""Dedicated FastAPI application for the Postgres-backed v2 runtime."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.edu_cti.core.logging_utils import setup_logging

# Configure structured logging at import time so every uvicorn worker process
# (the app is imported per worker) shares the same format and suppression.
setup_logging()

from src.edu_cti.api.v2 import router as v2_router
from src.edu_cti.api.v2_admin import router as v2_admin_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("Starting EduThreat-CTI v2 API...")
    # The v2 read endpoints are sync `def` handlers, so FastAPI runs them in
    # anyio's shared threadpool. Cap that threadpool so we never run more
    # concurrent blocking DB calls than the connection pool can serve (plus a
    # small buffer for cache hits / no-DB routes) — this avoids threads piling
    # up waiting on connections under load.
    try:
        import anyio
        from src.edu_cti_v2.db.config import V2DatabaseSettings

        settings = V2DatabaseSettings.from_env()
        tokens = settings.pool_size + settings.max_overflow + 8
        limiter = anyio.to_thread.current_default_thread_limiter()
        limiter.total_tokens = tokens
        logger.info("Set request threadpool limit to %d tokens", tokens)
    except Exception:  # pragma: no cover - never block startup on tuning
        logger.warning("Could not tune request threadpool size", exc_info=True)
    yield
    logger.info("Stopping EduThreat-CTI v2 API...")


def _cors_origins() -> list[str]:
    """Allowed browser origins. Default to the public dashboard; override with a
    comma-separated ``CORS_ALLOW_ORIGINS`` env (use ``*`` to allow any, e.g. for
    open API consumers in a dev environment)."""
    raw = os.environ.get("CORS_ALLOW_ORIGINS", "").strip()
    if raw:
        return [o.strip() for o in raw.split(",") if o.strip()]
    return [
        # Production dashboard (Vercel). Both spellings are included because the
        # deployed project URL uses the hyphenated "edu-threat" form.
        "https://edu-threat-cti-dashboard.vercel.app",
        "https://eduthreat-cti-dashboard.vercel.app",
        "http://localhost:3000",
    ]


def _rate_limit() -> str:
    """Default per-IP rate limit for public read endpoints (override via
    ``API_RATE_LIMIT``, e.g. ``120/minute``)."""
    return os.environ.get("API_RATE_LIMIT", "60/minute").strip() or "60/minute"


def create_app() -> FastAPI:
    app = FastAPI(
        title="EduThreat-CTI v2 API",
        description="Postgres-backed canonical incident API for EduThreat-CTI",
        version="2.0.3-v2",
        lifespan=lifespan,
    )

    # ── Rate limiting (public read abuse / cost protection) ───────────────────
    # slowapi keys by client IP and applies a global default limit; /health is
    # exempt so uptime checks are never throttled.
    try:
        from slowapi import Limiter, _rate_limit_exceeded_handler
        from slowapi.errors import RateLimitExceeded
        from slowapi.middleware import SlowAPIMiddleware
        from slowapi.util import get_remote_address

        def _limit_key(request: Request) -> str:
            # Honour the platform's forwarded client IP (Railway/Cloudflare) so
            # the limiter doesn't see every request as the same proxy IP.
            fwd = request.headers.get("x-forwarded-for")
            if fwd:
                return fwd.split(",")[0].strip()
            return get_remote_address(request)

        limiter = Limiter(key_func=_limit_key, default_limits=[_rate_limit()])
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        app.add_middleware(SlowAPIMiddleware)
        logger.info("Rate limiting enabled: %s per IP", _rate_limit())
    except Exception:  # pragma: no cover - never block startup on the limiter
        limiter = None
        logger.warning("Could not enable rate limiting", exc_info=True)

    cors_origins = _cors_origins()
    allow_any = cors_origins == ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        # credentials cannot be combined with a wildcard origin per the CORS spec.
        allow_credentials=not allow_any,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    app.include_router(v2_admin_router, prefix="/api")
    app.include_router(v2_router)

    def _exempt(fn):
        # Exempt uptime checks from the rate limiter so they're never throttled.
        return limiter.exempt(fn) if limiter is not None else fn

    @app.get("/health", tags=["Health"])
    @_exempt
    async def health() -> dict[str, str]:
        return {"status": "healthy", "service": "v2-api", "layer": "v2"}

    @app.get("/api/health", tags=["Health"])
    @_exempt
    async def api_health() -> dict[str, str]:
        return {"status": "healthy", "service": "v2-api", "layer": "v2"}

    return app


app = create_app()

