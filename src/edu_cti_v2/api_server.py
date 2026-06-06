"""CLI entry point for the dedicated Postgres-backed v2 API.

Worker count is resolved automatically from the *container's* CPU and memory
limits (cgroup v2/v1), not the physical host — this keeps the service from
over-forking and OOM-killing itself on memory-constrained platforms like
Railway. Override with ``API_WORKERS`` / ``WEB_CONCURRENCY`` (an integer, or
``auto``).
"""

from __future__ import annotations

import argparse
import logging
import os

import uvicorn

from src.edu_cti_v2.resource_limits import cgroup_cpu_count, cgroup_memory_limit_mb


def resolve_worker_count(cli_workers: int | None) -> int:
    """Resolve the number of API worker processes.

    Precedence: explicit ``API_WORKERS``/``WEB_CONCURRENCY`` env → explicit
    ``--workers N`` (N>0) → automatic, derived from container CPU + memory.
    """
    env = os.environ.get("API_WORKERS") or os.environ.get("WEB_CONCURRENCY")
    if env and env.strip().lower() not in {"auto", "0", ""}:
        try:
            return max(1, int(env))
        except ValueError:
            pass
    if cli_workers and cli_workers > 0:
        return cli_workers

    cpu = cgroup_cpu_count()
    cpu_workers = 2 * cpu + 1

    per_worker_mb = int(os.environ.get("API_WORKER_MEM_MB", "400"))
    mem_mb = cgroup_memory_limit_mb()
    if mem_mb:
        # Reserve ~15% headroom for the OS, shared caches, and DB connections.
        usable = int(mem_mb * 0.85)
        mem_workers = max(1, usable // per_worker_mb)
    else:
        mem_workers = cpu_workers

    hard_cap = int(os.environ.get("API_MAX_WORKERS", "8"))
    workers = max(1, min(cpu_workers, mem_workers, hard_cap))
    print(
        f"[api] auto worker count = {workers} "
        f"(cpu={cpu} → {cpu_workers}, mem={mem_mb or 'unbounded'}MB → {mem_workers}, cap={hard_cap})"
    )
    return workers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start the EduThreat-CTI v2 API server")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=None, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")
    parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Number of API worker processes (0 = auto, derived from container CPU/memory)",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    port = args.port or int(os.environ.get("PORT", 8000))
    workers = 1 if args.reload else resolve_worker_count(args.workers)

    print(f"Starting EduThreat-CTI v2 API server on {args.host}:{port} (workers={workers})")
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
        workers=workers,
    )


if __name__ == "__main__":
    main()
