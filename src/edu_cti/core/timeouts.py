"""Wall-clock timeout helpers.

These exist so that a single hung operation — most importantly a stuck source
collector during a historical run — cannot freeze the entire pipeline. The
collection loop processes sources sequentially in one thread, so one source
that blocks indefinitely (e.g. an unbounded network fetch) stalls every other
source and the whole orchestration plan.

The guard is **progress-aware**: it bounds *idle* time (time with no observed
progress), not total elapsed time. A source builder that keeps saving batches
via ``save_callback`` is making progress and is never interrupted, no matter how
long the full scrape takes; only a source that produces no progress for the
whole budget window — a genuine hang — is abandoned. This is the difference
between a legitimately slow multi-page scrape (e.g. Comparitech) and a stuck
HTTP fetch (the original KonBriefing hang).

Abandoned calls keep running daemonized (Python cannot forcibly kill a thread),
but the caller proceeds, the source is recorded as failed/empty, and the run
continues. Because source builders save incrementally, any work completed before
the timeout is already persisted.
"""

from __future__ import annotations

import functools
import logging
import os
import threading
import time
from typing import Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Generous default: a source is only abandoned after this many seconds with
# *no* progress (no saved batch). Legitimate slow scrapes save far more often
# than this; a genuine hang produces nothing for the whole window.
DEFAULT_SOURCE_IDLE_TIMEOUT_SECONDS = 600


class OperationTimeout(TimeoutError):
    """Raised when a guarded operation makes no progress within its budget."""


class Heartbeat:
    """Thread-safe progress signal. ``beat()`` records that progress happened;
    ``idle_seconds()`` reports how long since the last beat."""

    def __init__(self) -> None:
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def beat(self) -> None:
        with self._lock:
            self._last = time.monotonic()

    def idle_seconds(self) -> float:
        with self._lock:
            return time.monotonic() - self._last


def source_timeout_seconds() -> int:
    """Per-source idle (no-progress) budget, overridable via
    ``EDU_CTI_SOURCE_TIMEOUT_SECONDS``."""
    raw = os.environ.get("EDU_CTI_SOURCE_TIMEOUT_SECONDS", "").strip()
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            logger.debug("Invalid EDU_CTI_SOURCE_TIMEOUT_SECONDS=%r; using default", raw)
    return DEFAULT_SOURCE_IDLE_TIMEOUT_SECONDS


def call_with_timeout(fn: Callable[[], T], *, timeout_seconds: float, label: str) -> T:
    """Run ``fn()`` with a fixed total wall-clock timeout.

    Returns ``fn()``'s value on success. Re-raises any exception ``fn`` raised.
    Raises :class:`OperationTimeout` if ``fn`` does not finish within
    ``timeout_seconds``. (For source collection prefer
    :func:`call_with_idle_timeout`, which does not penalise slow-but-progressing
    work.)
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


def call_with_idle_timeout(
    fn: Callable[[], T],
    *,
    heartbeat: Heartbeat,
    idle_timeout_seconds: float,
    label: str,
) -> T:
    """Run ``fn()``, abandoning it only if it makes no progress for
    ``idle_timeout_seconds``.

    Progress is signalled by calls to ``heartbeat.beat()`` (typically from a
    wrapped ``save_callback``). Returns ``fn()``'s value on success, re-raises
    its exception, or raises :class:`OperationTimeout` after an idle stretch that
    exceeds the budget.
    """
    box: dict = {}

    def _run() -> None:
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - propagated to the caller thread
            box["error"] = exc

    thread = threading.Thread(target=_run, name=f"idle-timeout:{label}", daemon=True)
    heartbeat.beat()  # reset the clock at start so setup counts as progress
    thread.start()
    # Poll frequently enough to detect a stall promptly without busy-waiting.
    poll = max(0.05, min(5.0, idle_timeout_seconds / 4.0))
    while True:
        thread.join(poll)
        if not thread.is_alive():
            break
        idle = heartbeat.idle_seconds()
        if idle >= idle_timeout_seconds:
            raise OperationTimeout(
                f"Operation '{label}' made no progress for {idle:.0f}s "
                f"(idle budget {idle_timeout_seconds:.0f}s)"
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
    """Wrap a source builder so each invocation is bounded by *idle* time.

    If the builder is called with a ``save_callback`` keyword, that callback is
    wrapped so every saved batch counts as progress and resets the idle clock —
    a slow-but-progressing source therefore runs to completion, while a source
    that produces nothing for ``timeout_seconds`` is abandoned. Builders that do
    not take a ``save_callback`` are still bounded from start to return.

    Uses :func:`functools.wraps` so ``inspect.signature`` still resolves to the
    original builder's signature (callers introspect it to decide which kwargs,
    e.g. ``save_callback``/``incremental``, to pass).
    """
    budget = timeout_seconds if (timeout_seconds and timeout_seconds > 0) else source_timeout_seconds()

    @functools.wraps(fn)
    def _wrapped(*args, **kwargs):
        heartbeat = Heartbeat()

        callback = kwargs.get("save_callback")
        if callable(callback):
            original_callback = callback

            def _beating_callback(batch):
                heartbeat.beat()
                try:
                    return original_callback(batch)
                finally:
                    heartbeat.beat()

            kwargs = {**kwargs, "save_callback": _beating_callback}

        return call_with_idle_timeout(
            lambda: fn(*args, **kwargs),
            heartbeat=heartbeat,
            idle_timeout_seconds=budget,
            label=label,
        )

    return _wrapped
