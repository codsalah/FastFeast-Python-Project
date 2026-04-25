"""
Structured JSON logger for the Fast Feast Pipeline
Design decisions:
- Wraps structlog for structured, machine-readable logs
- Every log entry is a flat JSON object.
- Context binding (run_id, stage, entity) is done once per Pipeline Run.
- Disk protection: log files are rotated by size (10 MB) and kept for 7 days.
- Thread safe: structlog's BoundLogger is immutable (each thread has its own logger instance)
    # In a pipeline stage (bind context once, use everywhere)
    # info, warning, error, critical for Pipeline Level
    # debug for Stage Level

"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Any

import structlog # this for formatting the log

_LOG_MAX_BYTES = 10 * 1024 * 1024 # 10 MB
_LOG_BACKUP_COUNT = 7 # keep 7 days of logs


def configure_logging(log_dir: str = "logs", level: str = "INFO") -> None:
    """ Configure the logging system for the pipeline. """
    from datetime import datetime
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = Path(log_dir) / f"pipeline_{timestamp}.log"
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    

    file_handler = logging.handlers.RotatingFileHandler(
        filename = log_file,
        maxBytes = _LOG_MAX_BYTES,
        backupCount = _LOG_BACKUP_COUNT,
        encoding = "utf-8",
    )
    # numerical level to filter out less severe logs
    file_handler.setLevel(numeric_level)
    # structlog renders the full JSON string as the log message — use %(message)s
    # only so the file gets pure JSON lines with no stdlib prefix (e.g. "INFO:name:{...}")
    file_handler.setFormatter(logging.Formatter("%(message)s"))

    # console handler for live logging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    logging.basicConfig(
        level = numeric_level,
        handlers = [file_handler, console_handler],
        force = True,
    )

    shared_processors = [
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.format_exc_info,
        structlog.stdlib.PositionalArgumentsFormatter(),   
        structlog.processors.UnicodeDecoder(),
    ]

    # configure structlog to use the shared processors
    structlog.configure(processors=shared_processors + 
                        [structlog.processors.JSONRenderer()],
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            # Cache the bound logger per module for performance
            cache_logger_on_first_use=True,
    )

# Public factory functions (used by other modules)

def get_logger_name(name: str) -> structlog.stdlib.BoundLogger:
    """ Return a structlog logger with the given name. """
    return structlog.get_logger(name)

def log_stage_start(log: structlog.stdlib.BoundLogger, stage_name: str, file: str = "", **extra: Any) -> None:
    """ Log the start of a pipeline stage. 
        Call this at the very beginning of a pipeline stage before any work
        The pipeline runtime uses the stage+file pair to calculate per-file latency

    Args:
        log: The logger to use.
        stage_name: The name of the stage.
        file: The file that the stage is in.
        extra: Extra key-value pairs to log.
    """
    log.info("Stage start", stage=stage_name, file=file, **extra) 
    
def log_stage_complete(log: structlog.stdlib.BoundLogger, stage_name: str, records: int, latency_ms: float, **extra: Any) -> None:
    """ Log the completion of a pipeline stage. 
    Call this at the very end of a pipeline stage after the stage is complete
    The pipeline runtime uses the stage+file pair to calculate per-file latency

    Args:
        log: The logger to use.
        stage_name: The name of the stage.
        records: The number of records processed.
        latency_ms: The latency in milliseconds.
        extra: Extra key-value pairs to log.
    """
    log.info("Stage complete", stage=stage_name, records=records, 
                latency_ms=latency_ms, **extra)


def log_record_rejected(
    log: structlog.stdlib.BoundLogger,
    record: dict[str, Any],
    reason: str,
    tier: str = "",
    field: str = "",
    **extra: Any,
    ) -> None:
    """ Log a rejected record. 
    Call this when a record is rejected by a stage
    The pipeline runtime uses the stage+file pair to calculate per-file latency

    Args:
        log: The logger to use.
        record: The rejected record.
        reason: The reason for rejection.
        tier: The tier of the record.
        field: The field that caused the rejection.
        extra: Extra key-value pairs to log.
    """
    record_preview = str(record)[:500]
 
    log.warning("record_rejected", reason=reason, tier=tier, 
                field=field, record_preview=record_preview, **extra)


def log_file_skipped(log: structlog.stdlib.BoundLogger, file: str, reason: str,
                            **extra: Any) -> None:
    """ Log a skipped file. 
    Call this when a file is skipped by a stage
    The pipeline runtime uses the stage+file pair to calculate per-file latency

    Args:
        log: The logger to use.
        file: The file that was skipped.
        reason: The reason for skipping the file.
        extra: Extra key-value pairs to log.
    """
    log.info("file_skipped", file=file, reason=reason, **extra)


def log_alert_fired(
    log: structlog.stdlib.BoundLogger, alert_type: str, severity: str,
            message: str, **extra: Any,) -> None:
    """ 
    Log an alert that was fired. (warning event every time the AlertManager sends an alert)
    - log file alongside the pipeline events that triggered the alert.

    Args:
        log: The logger to use.
        alert_type: The type of alert.
        severity: The severity of the alert.
        message: The message of the alert.
        extra: Extra key-value pairs to log.
    """
    log.warning("alert_fired", alert_type=alert_type, severity=severity, 
                    message=message, **extra)