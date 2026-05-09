"""Dedicated FastAPI application for the Postgres-backed v2 runtime."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.edu_cti.api.v2 import router as v2_router
from src.edu_cti.api.v2_admin import router as v2_admin_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("Starting EduThreat-CTI v2 API...")
    yield
    logger.info("Stopping EduThreat-CTI v2 API...")


def create_app() -> FastAPI:
    app = FastAPI(
        title="EduThreat-CTI v2 API",
        description="Postgres-backed canonical incident API for EduThreat-CTI",
        version="2.0.0-v2",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(v2_admin_router, prefix="/api")
    app.include_router(v2_router)

    @app.get("/health", tags=["Health"])
    async def health() -> dict[str, str]:
        return {"status": "healthy", "service": "v2-api", "layer": "v2"}

    @app.get("/api/health", tags=["Health"])
    async def api_health() -> dict[str, str]:
        return {"status": "healthy", "service": "v2-api", "layer": "v2"}

    return app


app = create_app()

