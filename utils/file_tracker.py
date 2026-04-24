"""
pipeline/file_tracker.py
────────────────────────
Idempotency layer for the FastFeast pipeline.

Uses file_path + file_hash together as the idempotency key.

Same file path + same hash  → already processed → SKIP
Same file path + new hash   → file regenerated  → PROCESS
New file path               → new file          → PROCESS

The hash is computed from the file content using SHA-256
before any parsing begins. This means even if a file is
rewritten with the same name, it will be reprocessed only
if the content actually changed.
"""

from __future__ import annotations

import hashlib
import os

from warehouse.connection import get_cursor, get_dict_cursor
from utils.retry import db_retry
from utils.logger import get_logger_name
from utils.file_utils import is_file_stable

logger = get_logger_name(__name__)


def compute_file_hash(file_path: str, chunk_size: int = 65536) -> str:
    """
    Compute SHA-256 hash of a file.
    
    Args:
        file_path: Path to the file.
        chunk_size: Bytes to read per chunk (default 64KB).
    
    Returns:
        Hexadecimal SHA-256 hash string (64 characters).
    
    Raises:
        OSError: If file cannot be read.
    """
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def get_file_size_bytes(file_path: str) -> int:
    """Get file size in bytes."""
    return os.path.getsize(file_path)


def is_file_processed(file_path: str, file_hash: str) -> bool:
    """
    Return True if this exact file (same path AND same hash) was already
    handled by the pipeline, either successfully or in a failed attempt.
    """
    # If file doesn't exist, it can't be processed
    if not os.path.exists(file_path):
        return False
    
    # Wait if file is still being written
    if not is_file_stable(file_path):
        return False

    with get_dict_cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pipeline_audit.file_tracker
            WHERE file_path = %s
              AND file_hash = %s
              AND status IN ('success', 'failed')
            LIMIT 1
            """,
            (file_path, file_hash)
        )
        row = cur.fetchone()
        already_done = row is not None

        if already_done:
            logger.debug("file_already_processed", file=file_path, hash=file_hash[:8])
        else:
            logger.debug("file_not_processed_yet", file=file_path, hash=file_hash[:8])

        return already_done


@db_retry
def register_file(
    run_id: int,
    file_path: str,
    file_hash: str,
    file_type: str,
) -> None:
    """Register a file as 'processing' in file_tracker."""
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_audit.file_tracker
                (file_path, file_hash, file_type, status, processed_at, pipeline_run_id)
            VALUES (%s, %s, %s, 'processing', now(), %s)
            ON CONFLICT (file_path, file_hash) DO UPDATE
                SET
                    status = 'processing',
                    processed_at = now(),
                    pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (file_path, file_hash, file_type, run_id)
        )

    logger.debug("file_registered", run_id=run_id, file=file_path, hash=file_hash[:8], type=file_type)


@db_retry
def mark_file_success(
    file_path: str,
    file_hash: str,
    records_total: int,
    records_loaded: int,
    records_quarantined: int,
) -> None:
    """
    Mark a file as successfully processed.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_audit.file_tracker
            SET
                status = 'success',
                records_total = %s,
                records_loaded = %s,
                records_quarantined = %s,
                processed_at = now()
            WHERE file_path = %s
              AND file_hash = %s
            """,
            (records_total, records_loaded, records_quarantined, file_path, file_hash)
        )

    logger.info(
        "file_success",
        file=file_path,
        hash=file_hash[:8],
        records_total=records_total,
        records_quarantined=records_quarantined,
    )


@db_retry
def mark_file_failed(file_path: str, file_hash: str, error_message: str) -> None:
    """Mark a file as failed."""
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_audit.file_tracker
            SET status = 'failed', processed_at = now()
            WHERE file_path = %s
              AND file_hash = %s
            """,
            (file_path, file_hash)
        )

    logger.warning("file_failed", file=file_path, hash=file_hash[:8], error=error_message)