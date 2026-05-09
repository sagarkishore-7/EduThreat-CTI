"""CLI entry point for the dedicated Postgres-backed v2 API."""

from __future__ import annotations

import argparse
import logging
import os

import uvicorn


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start the EduThreat-CTI v2 API server")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=None, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument("--workers", type=int, default=1, help="Number of API worker processes")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    port = args.port or int(os.environ.get("PORT", 8000))

    print(f"Starting EduThreat-CTI v2 API server on {args.host}:{port}")
    print(f"v2 API documentation available at: http://localhost:{port}/docs")

    class _QuietPollFilter(logging.Filter):
        _quiet_paths = (
            "/api/admin/v2/status",
            "/api/admin/v2/tasks",
            "/api/admin/v2/runs",
            "/api/admin/v2/scheduler/status",
            "/health",
            "/api/health",
        )

        def filter(self, record: logging.LogRecord) -> bool:
            msg = record.getMessage()
            return not any(path in msg for path in self._quiet_paths)

    logging.getLogger("uvicorn.access").addFilter(_QuietPollFilter())

    uvicorn.run(
        "src.edu_cti_v2.api_app:app",
        host=args.host,
        port=port,
        reload=args.reload,
        workers=args.workers if not args.reload else 1,
    )


if __name__ == "__main__":
    main()

