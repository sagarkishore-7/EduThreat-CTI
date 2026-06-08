"""Wall-clock timeout helpers.

These exist so that a single hung operation — most importantly a stuck source
collector during a historical run — cannot freeze the entire pipeline. The
collection loop processes sources sequentially in one thread, so one source
that blocks indefinitely (e.g. an unbounded network fetch) stalls every other
source and the whole orchestration plan.

``call_with_timeout`` runs the target in a daemon worker thread and abandons it
if it exceeds the budget. The abandoned call keeps running daemonized (Python
cannot forcibly kill a thread), but the caller proceeds, the source is recorded
as failed/empty, and the run continues. Because source builders save
incrementally, any work completed before the timeout is already persisted.
"""

from __future__ import annotations

import functools
import logging
import os
import threading
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Generous default: legitimate single-source collection (incl. multi-page RSS
# over a long window) finishes well under this; a genuine hang vastly exceeds it.
DEFAULT_SOURCE_TIMEOUT_SECONDS = 600


class OperationTimeout(TimeoutError):
    """Raised when a guarded operation exceeds its wall-clock budget."""


def source_timeout_seconds() -> int:
    """Per-source collection budget, overridable via ``EDU_CTI_SOURCE_TIMEOUT_SECONDS``."""
    raw = os.environ.get("EDU_CTI_SOURCE_TIMEOUT_SECONDS", "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            logger.debug("Invalid EDU_CTI_SOURCE_TIMEOUT_SECONDS=%r; using default", raw)
    return DEFAULT_SOURCE_TIMEOUT_SECONDS


def call_with_timeout(fn: Callable[[], T], *, timeout_seconds: float, label: str) -> T:
    """Run ``fn()`` with a wall-clock timeout.

    Returns ``fn()``'s value on success. Re-raises any exception ``fn`` raised on
    the calling thread. Raises :class:`OperationTimeout` if ``fn`` does not finish
    within ``timeout_seconds``.
    """
    box: dict = {}

    def _run() -> None:
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - propagated to the caller thread
            box["error"] = exc

    thread = threading.Thread(target=_run, name=f"timeout:{label}", daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        raise OperationTimeout(
            f"Operation '{label}' exceeded {timeout_seconds:.0f}s wall-clock budget"
        )
    if "error" in box:
        raise box["error"]
    return box.get("value")  # type: ignore[return-value]


def guard_source_timeout(
    fn: Callable,
    *,
    label: str,
    timeout_seconds: Optional[float] = None,
) -> Callable:
    """Wrap a source builder so each invocation is wall-clock bounded.

    Uses :func:`functools.wraps` so ``inspect.signature`` still resolves to the
    original builder's signature (callers introspect it to decide which kwargs,
    e.g. ``save_callback``/``incremental``, to pass).
    """
    budget = timeout_seconds if (timeout_seconds and timeout_seconds > 0) else source_timeout_seconds()

    @functools.wraps(fn)
    def _wrapped(*args, **kwargs):
        return call_with_timeout(
            lambda: fn(*args, **kwargs),
            timeout_seconds=budget,
            label=label,
        )

    return _wrapped
