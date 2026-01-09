from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


class TruncatingFormatter(logging.Formatter):
    """Formatter that truncates long log messages."""
    
    MAX_MESSAGE_LENGTH = 500  # Maximum characters per log message
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record, truncating long messages."""
        # Truncate message if too long
        if len(record.getMessage()) > self.MAX_MESSAGE_LENGTH:
            original_msg = record.getMessage()
            truncated = original_msg[:self.MAX_MESSAGE_LENGTH] + "... [truncated]"
            record.msg = truncated
            record.args = ()
        
        return super().format(record)


def configure_logging(
    level: str = "INFO",
    log_file: Optional[Path] = None,
) -> None:
    """
    Configure root logging once per process.
    If log_file is provided, writes to both console and file.
    
    Optimized for Railway: truncates long messages, minimal verbosity.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Compact formatter for Railway (reduced verbosity)
    formatter = TruncatingFormatter(
        "%(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(log_level)
        # Use full formatter for file logs
        file_formatter = TruncatingFormatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get logger instance with optimized settings."""
    logger = logging.getLogger(name if name else __name__)
    return logger


def log_short(logger: logging.Logger, level: int, message: str, max_len: int = 200) -> None:
    """
    Log a message, truncating if too long.
    
    Args:
        logger: Logger instance
        level: Log level (logging.INFO, etc.)
        message: Message to log
        max_len: Maximum message length
    """
    if len(message) > max_len:
        message = message[:max_len] + "..."
    logger.log(level, message)
