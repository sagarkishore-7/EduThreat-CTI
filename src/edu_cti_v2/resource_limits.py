"""Container-aware resource limits and worker auto-sizing.

Shared by the API (`api_server`) and the pipeline worker (`runtime`/`worker`).
Worker counts are derived from the *container's* cgroup CPU and memory limits
(v2 and v1), not the physical host, so the services don't over-fork and
OOM-kill themselves on memory-constrained platforms like Railway (7 GB).

The pipeline worker is special: its heavy ML extraction stack (GLiNER +
sentence-transformers + MITRE embeddings + torch) loads **once per process and
is shared across worker threads** (measured ~1.6 GB resident). So the memory
floor is that one-time model footprint, *not* a per-worker copy — and because
enrichment is network-bound on the LLM API, throughput scales with thread
concurrency at low marginal memory cost. The binding constraint is therefore
usually the LLM provider's rate limit (an explicit cap), not container memory.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def read_int_file(path: str) -> int | None:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            value = handle.read().strip()
        if not value or value == "max":
            return None
        return int(value)
    except (OSError, ValueError):
        return None


def cgroup_memory_limit_mb() -> int | None:
    """Container memory limit in MB, or None if unbounded/unknown."""
    limit = read_int_file("/sys/fs/cgroup/memory.max")  # cgroup v2
    if limit is None:
        limit = read_int_file("/sys/fs/cgroup/memory/memory.limit_in_bytes")  # cgroup v1
    if limit is None or limit >= 1 << 62:  # near-INT64 ⇒ unbounded
        return None
    return max(1, limit // (1024 * 1024))


def cgroup_cpu_count() -> int:
    """Effective CPU count for the container (honours cgroup CPU quota)."""
    try:
        with open("/sys/fs/cgroup/cpu.max", "r", encoding="utf-8") as handle:  # cgroup v2
            quota_s, period_s = (handle.read().strip().split() + ["100000"])[:2]
        if quota_s != "max":
            quota, period = int(quota_s), int(period_s)
            if quota > 0 and period > 0:
                return max(1, round(quota / period))
    except (OSError, ValueError):
        pass
    quota = read_int_file("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")  # cgroup v1
    period = read_int_file("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    if quota and period and quota > 0:
        return max(1, round(quota / period))
    return os.cpu_count() or 1


def current_rss_mb() -> float | None:
    """Resident set size of the current process in MB (via psutil), or None."""
    try:
        import psutil

        return psutil.Process().memory_info().rss / (1024 * 1024)
    except Exception:
        return None


# ── Pipeline worker auto-sizing ──────────────────────────────────────────────

# One-time resident footprint of the shared ML extraction stack (GLiNER +
# sentence-transformers + MITRE embeddings + torch), measured ~1.56 GB. Shared
# across all worker threads in the process.
DEFAULT_MODEL_FLOOR_MB = 1600
# Transient peak per concurrent enrichment. This depends heavily on whether the
# local ML pre-pass runs: when it does (the intended config,
# EDU_CTI_V2_ENABLE_LOCAL_ML=true), every concurrent enrichment runs a GLiNER +
# sentence-transformer forward pass and buffers a large article (the split
# schema fires at >25k chars), so the real transient peak is ~1.8 GB, NOT the
# few hundred MB an I/O-bound LLM call would use. Sizing on the old 200 MB
# estimate let the auto-sizer pick 6 workers on a 7.6 GB container, which
# OOM-killed the worker in a ~5-minute restart loop. When local ML is disabled
# the pre-pass is skipped and the per-worker cost really is small.
DEFAULT_PER_WORKER_MB_WITH_ML = 1800
DEFAULT_PER_WORKER_MB_NO_ML = 250
# Cap that binds when memory is plentiful: matched to the LLM provider's
# concurrency / rate limit. Override via EDU_CTI_V2_MAX_WORKERS.
DEFAULT_MAX_ENRICH_WORKERS = 6


def _local_ml_enabled() -> bool:
    """Whether the local ML extraction pre-pass runs (drives per-worker memory)."""
    enable = os.environ.get("EDU_CTI_V2_ENABLE_LOCAL_ML", "").strip().lower()
    disable = os.environ.get("DISABLE_ML_FEATURES", "").strip().lower()
    if enable in {"1", "true", "yes"}:
        return True
    if disable in {"1", "true", "yes"}:
        return False
    # Default matches the production worker: local ML on.
    return enable not in {"0", "false", "no"}


def _default_per_worker_mb() -> int:
    return DEFAULT_PER_WORKER_MB_WITH_ML if _local_ml_enabled() else DEFAULT_PER_WORKER_MB_NO_ML


def _env_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; ignoring", name, raw)
        return None


def resolve_enrichment_worker_count(requested: int | str | None = None) -> int:
    """Resolve the number of enrichment worker threads.

    Precedence: explicit positive int (env ``EDU_CTI_V2_WORKER_COUNT`` or the
    ``requested`` arg) → automatic, derived from container memory minus the
    shared model floor, divided by per-worker overhead, capped at the
    provider rate-limit cap (``EDU_CTI_V2_MAX_WORKERS``).

    ``auto`` / ``0`` / unset request ⇒ derive automatically.
    """
    # Explicit override (env wins over arg).
    env_raw = os.environ.get("EDU_CTI_V2_WORKER_COUNT")
    candidates: list[int | str | None] = [env_raw, requested]
    for cand in candidates:
        if cand is None:
            continue
        text = str(cand).strip().lower()
        if text in {"", "auto", "0"}:
            continue
        try:
            explicit = int(text)
        except ValueError:
            continue
        if explicit > 0:
            return explicit

    model_floor = _env_int("EDU_CTI_V2_MODEL_FLOOR_MB") or DEFAULT_MODEL_FLOOR_MB
    per_worker = _env_int("EDU_CTI_V2_PER_WORKER_MB") or _default_per_worker_mb()
    cap = _env_int("EDU_CTI_V2_MAX_WORKERS") or DEFAULT_MAX_ENRICH_WORKERS

    mem_mb = cgroup_memory_limit_mb()
    if mem_mb:
        usable = (mem_mb - model_floor) * 0.85  # headroom for OS + DB conns + caches
        mem_workers = max(1, int(usable // per_worker)) if usable > 0 else 1
    else:
        # Unbounded/unknown container ⇒ fall back to CPU-based sizing.
        mem_workers = 2 * cgroup_cpu_count() + 1

    workers = max(1, min(cap, mem_workers))
    logger.info(
        "[worker] auto enrichment workers = %d "
        "(mem=%sMB, floor=%dMB, per_worker=%dMB → mem_cap=%d, rate_cap=%d)",
        workers,
        mem_mb or "unbounded",
        model_floor,
        per_worker,
        mem_workers,
        cap,
    )
    return workers


def memory_high_water_mb() -> float | None:
    """RSS threshold above which the worker pauses leasing new tasks.

    Defaults to 82% of the container memory limit; override with
    ``EDU_CTI_V2_MAX_RSS_MB``. Returns None when no limit can be determined
    (the guard is then a no-op).

    82% (not 90%) leaves headroom for a single large-article enrichment to spike
    RSS between guard checks without crossing the container limit. At 90% on a
    7.6 GB container the guard fires at ~6.9 GB, too late to absorb a burst, so
    the container was OOM-killed before leasing paused.
    """
    explicit = _env_int("EDU_CTI_V2_MAX_RSS_MB")
    if explicit and explicit > 0:
        return float(explicit)
    mem_mb = cgroup_memory_limit_mb()
    if mem_mb:
        return mem_mb * 0.82
    return None
