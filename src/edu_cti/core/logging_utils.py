"""Production-grade structured logging for EduThreat-CTI.

A single :func:`setup_logging` configures the whole process. It is built on
``structlog`` wrapping the standard library, so every existing
``logging.getLogger(__name__)`` call site renders through the same pipeline —
no per-module changes required — and so do foreign stdlib loggers (httpx,
uvicorn, scrapling, etc.).

Highlights:
- **Two render modes, one env switch.** ``LOG_FORMAT=json`` emits one JSON event
  per line (filterable / aggregator-ready); ``LOG_FORMAT=console`` emits a pretty
  coloured line for local development. Default: ``json`` on Railway, else
  ``console``.
- **Context propagation.** :func:`bind_log_context` / :func:`clear_log_context`
  attach ``task_id`` / ``run_id`` / ``source`` / ``canonical_id`` etc. to every
  downstream log line via ``contextvars`` — so a single task or incident can be
  traced across the resolve -> fetch -> enrich -> canonicalize stages.
- **Centralised third-party noise suppression** (httpx, scrapling double-print,
  gliner/transformers progress + warnings, ...).
- **Truncation** of oversized messages (raw LLM output) so logs cannot flood.

Backward compatibility: :func:`configure_logging` and :func:`get_logger` remain
as thin wrappers so existing CLI entry points keep working unchanged.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import warnings
from pathlib import Path
from typing import Any, Optional

import structlog

# ── Constants ────────────────────────────────────────────────────────────────

LOG_DIR = Path("logs")
_MAX_MESSAGE_LENGTH = 2000

# Third-party loggers that are chatty at INFO and add no diagnostic value.
_NOISY_LOGGERS_WARNING = (
    "httpx",
    "httpcore",
    "urllib3",
    "playwright",
    "curl_cffi",
    "asyncio",
    "transformers",
    "sentence_transformers",
    "safetensors",
    "gliner",
    "filelock",
    "huggingface_hub",
)

_configured = False


# ── Processors ───────────────────────────────────────────────────────────────

def _truncate_event(_logger, _method_name, event_dict):
    """Cap the rendered message so a pathological line (e.g. a raw degenerate
    LLM response) cannot flood the logs."""
    event = event_dict.get("event")
    if isinstance(event, str) and len(event) > _MAX_MESSAGE_LENGTH:
        event_dict["event"] = event[:_MAX_MESSAGE_LENGTH] + " …[truncated]"
    return event_dict


def _resolve_log_format() -> str:
    fmt = os.environ.get("LOG_FORMAT", "").strip().lower()
    if fmt in {"json", "console"}:
        return fmt
    # Default: JSON in any deployed (Railway) environment, pretty console locally.
    return "json" if os.environ.get("RAILWAY_ENVIRONMENT") else "console"


def _shared_processors() -> list:
    """Processors applied to BOTH structlog-native and foreign stdlib records."""
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _truncate_event,
    ]


# ── Setup ────────────────────────────────────────────────────────────────────

def setup_logging(
    level: Optional[str] = None,
    *,
    log_format: Optional[str] = None,
    log_file: Optional[Path] = None,
    phase: Optional[str] = None,
    force: bool = True,
) -> None:
    """Configure structlog + stdlib logging for the whole process.

    Args:
        level: Root log level (DEBUG/INFO/WARNING/ERROR). Defaults to the
            ``LOG_LEVEL`` env var, else INFO.
        log_format: ``"json"`` or ``"console"``. Defaults to ``LOG_FORMAT`` env,
            else json on Railway / console locally.
        log_file: Optional rotating file sink (in addition to stdout).
        phase: Convenience for naming a per-phase log file under ``logs/``.
        force: Reconfigure even if already configured (default True).
    """
    global _configured
    if _configured and not force:
        return

    level_name = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    log_level = getattr(logging, level_name, logging.INFO)
    fmt = (log_format or _resolve_log_format()).lower()

    # structlog -> stdlib: native structlog calls are wrapped for ProcessorFormatter.
    structlog.configure(
        processors=_shared_processors()
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Final renderer turns the processed event_dict into the output string. The
    # same ProcessorFormatter renders foreign (plain stdlib) records too, via
    # foreign_pre_chain, so httpx/uvicorn lines match our format.
    if fmt == "json":
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    # foreign_pre_chain runs on plain stdlib records (httpx, uvicorn, and our own
    # logging.getLogger() calls). ExtraAdder surfaces their ``extra={...}`` fields
    # (e.g. elapsed_ms on task_completed) into the rendered output.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=_shared_processors() + [structlog.stdlib.ExtraAdder()],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(log_level)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # Optional rotating file sink.
    resolved_file = log_file
    if resolved_file is None and phase:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        resolved_file = LOG_DIR / f"{phase}.log"
    if resolved_file is not None:
        resolved_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            resolved_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    _suppress_third_party_noise()

    # Route warnings.warn(...) (e.g. gliner truncation UserWarnings) through logging
    # so they obey the same suppression/format instead of printing raw to stderr.
    logging.captureWarnings(True)
    warnings.filterwarnings("ignore", message=r".*has been truncated.*")

    _configured = True


def _suppress_third_party_noise() -> None:
    for name in _NOISY_LOGGERS_WARNING:
        logging.getLogger(name).setLevel(logging.WARNING)

    # Per-logger level overrides via env, e.g. LOG_LEVEL_scrapling=ERROR.
    for env_key, env_val in os.environ.items():
        if env_key.startswith("LOG_LEVEL_") and env_key != "LOG_LEVEL_":
            target = env_key[len("LOG_LEVEL_"):]
            lvl = getattr(logging, env_val.strip().upper(), None)
            if lvl is not None:
                logging.getLogger(target).setLevel(lvl)

    # scrapling attaches its OWN handler and also propagates to root, so every
    # fetch line prints twice. Silence its handler and stop propagation noise.
    scrapling_logger = logging.getLogger("scrapling")
    scrapling_logger.setLevel(logging.WARNING)
    scrapling_logger.propagate = False
    for handler in list(scrapling_logger.handlers):
        scrapling_logger.removeHandler(handler)

    # Belt-and-suspenders for HuggingFace progress bars / transformers chatter.
    try:  # pragma: no cover - optional dependency
        from transformers.utils import logging as hf_logging

        hf_logging.set_verbosity_error()
        hf_logging.disable_progress_bar()
    except Exception:
        pass


# ── Context helpers ──────────────────────────────────────────────────────────

def bind_log_context(**kwargs: Any) -> None:
    """Attach key/value context to all subsequent log lines on this thread/task
    (e.g. ``bind_log_context(task_id=..., run_id=..., source=...)``). ``None``
    values are dropped so callers can pass optional fields freely."""
    clean = {k: v for k, v in kwargs.items() if v is not None}
    if clean:
        structlog.contextvars.bind_contextvars(**clean)


def unbind_log_context(*keys: str) -> None:
    """Remove specific keys from the bound context."""
    if keys:
        structlog.contextvars.unbind_contextvars(*keys)


def clear_log_context() -> None:
    """Clear ALL bound context (call in a ``finally`` at task boundaries)."""
    structlog.contextvars.clear_contextvars()


def get_structlog(name: Optional[str] = None):
    """Return a structlog logger supporting structured kwargs:
    ``log.info("event_name", field=value)``."""
    return structlog.get_logger(name)


# ── Backward-compatible shims ────────────────────────────────────────────────

def configure_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
    phase: Optional[str] = None,
    json_logs: bool = False,
) -> None:
    """Deprecated alias for :func:`setup_logging`, kept for existing CLIs."""
    setup_logging(
        level,
        log_format="json" if json_logs else None,
        log_file=log_file,
        phase=phase,
    )


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a stdlib logger (unchanged behaviour for existing call sites)."""
    return logging.getLogger(name if name else __name__)


def log_short(logger: logging.Logger, level: int, message: str, max_len: int = 200) -> None:
    """Log a message, truncating if too long (retained for compatibility)."""
    if len(message) > max_len:
        message = message[:max_len] + "..."
    logger.log(level, message)
