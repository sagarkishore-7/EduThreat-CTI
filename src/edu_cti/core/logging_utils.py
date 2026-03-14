"""
Structured logging configuration for EduThreat-CTI.

Features:
- Separate log files per pipeline phase (phase1, phase2, api, scheduler)
- Rotating file handler (10MB max, 5 backups)
- JSON structured logging for file output (parseable by log aggregators)
- Human-readable console output
- Configurable verbosity per component
"""

from __future__ import annotations

import json
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional


class TruncatingFormatter(logging.Formatter):
    """Formatter that truncates long log messages."""

    MAX_MESSAGE_LENGTH = 500

    def format(self, record: logging.LogRecord) -> str:
        if len(record.getMessage()) > self.MAX_MESSAGE_LENGTH:
            original_msg = record.getMessage()
            truncated = original_msg[:self.MAX_MESSAGE_LENGTH] + "... [truncated]"
            record.msg = truncated
            record.args = ()
        return super().format(record)


class JSONFormatter(logging.Formatter):
    """JSON structured log formatter for file output."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info and record.exc_info[1]:
            log_entry["exception"] = {
                "type": type(record.exc_info[1]).__name__,
                "message": str(record.exc_info[1]),
            }

        return json.dumps(log_entry, ensure_ascii=False)


# Log directory
LOG_DIR = Path("logs")


def configure_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
    phase: Optional[str] = None,
    json_logs: bool = False,
) -> None:
    """
    Configure root logging with rotating file handler.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        log_file: Explicit log file path (overrides phase-based naming)
        phase: Pipeline phase name for auto-naming (phase1, phase2, api, scheduler)
        json_logs: If True, use JSON format for file logs
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Console formatter (human-readable)
    console_formatter = TruncatingFormatter(
        "%(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Determine log file path
    if log_file is None and phase:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / f"{phase}.log"
    elif log_file is None:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / "pipeline.log"

    # File handler with rotation (10MB max, 5 backups)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)

        if json_logs:
            file_handler.setFormatter(JSONFormatter())
        else:
            file_formatter = logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s:%(funcName)s:%(lineno)d] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            file_handler.setFormatter(file_formatter)

        root_logger.addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("curl_cffi").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get logger instance."""
    return logging.getLogger(name if name else __name__)


def log_short(logger: logging.Logger, level: int, message: str, max_len: int = 200) -> None:
    """Log a message, truncating if too long."""
    if len(message) > max_len:
        message = message[:max_len] + "..."
    logger.log(level, message)
