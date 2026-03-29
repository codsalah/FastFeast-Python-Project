"""
pipeline/utils/file_utils.py
────────────────────────────
File system utilities used by the ingestion and archiving layers.

Key responsibilities:
  - SHA-256 checksums for idempotency (file-level dedup guard)
  - Atomic writes (write to tmp, then rename — never a partial file)
  - Directory management
  - File discovery for batch and stream watchers
  - Archiving processed files to data/processed/
  - Log rotation to prevent disk exhaustion

None of these functions touch the database.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional


# Checksums

def compute_sha256(path: str | Path) -> str:
    """
    Compute the SHA-256 hex digest of a file's contents.

    Used as the idempotency key alongside file_path in audit.file_loads.
    Streaming read (8 KB chunks) keeps memory usage constant regardless of
    file size.

    Returns a 64-character hex string.
    """
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def compute_record_sha256(record: dict) -> str:
    """
    Compute the SHA-256 of a dict (stable, sorted-key JSON serialisation).

    Used for SCD2 change detection — cheaper than column-by-column comparison.
    A different hash means the record changed; an identical hash means no update needed.
    """
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


# File stability check

def is_file_stable(filepath: str, wait_sec: int = 1, max_attempts: int = 5) -> bool:
    """Check if file is done being written by comparing size over multiple attempts."""
    try:
        previous_size = -1
        current_size = os.path.getsize(filepath)
        
        for attempt in range(max_attempts):
            if previous_size == current_size and attempt > 0:
                return True
            previous_size = current_size
            time.sleep(wait_sec)
            current_size = os.path.getsize(filepath)
        
        # After max attempts, if size changed, assume still writing
        return previous_size == current_size
    except OSError:
        return False


# Atomic writes

def atomic_write(path: str | Path, content: str, encoding: str = "utf-8") -> None:
    """
    Write content to path atomically using a temp-file + rename pattern.

    Why: a plain open(path, 'w').write() can leave a half-written file if
    the process is killed mid-write. A rename() is atomic on POSIX systems,
    so the destination file either has the old content or the new content —
    never a partial write.

    The temp file is created in the same directory as the target so the
    rename() stays on the same filesystem (cross-device rename would fail).
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # fd=-1 means the caller owns the file from the start
    fd, tmp_path = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding=encoding) as f:
            f.write(content)
        os.replace(tmp_path, target)  # atomic on POSIX
    except Exception:
        # Clean up the temp file if anything goes wrong
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_write_json(path: str | Path, data: dict | list, indent: int = 2) -> None:
    """Convenience wrapper: serialise data to JSON and write atomically."""
    atomic_write(path, json.dumps(data, default=str, indent=indent))


# Directory utilities

def ensure_dir(path: str | Path) -> Path:
    """
    Create directory (and all parents) if it does not exist.
    Returns the Path object for chaining.
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def list_files(
    directory: str | Path,
    extensions: Optional[List[str]] = None,
    recursive: bool = False,
) -> List[Path]:
    """
    Return a sorted list of files in directory.

    Args:
        directory:  Directory to scan.
        extensions: If provided, only return files with these extensions
                    (e.g. [".csv", ".json"]).  Case-insensitive.
        recursive:  If True, scan subdirectories as well.

    Returns a list sorted by name — consistent ordering is important for
    idempotent batch runs.
    """
    d = Path(directory)
    if not d.exists():
        return []

    pattern = "**/*" if recursive else "*"
    files = [f for f in d.glob(pattern) if f.is_file()]

    if extensions:
        exts = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions}
        files = [f for f in files if f.suffix.lower() in exts]

    return sorted(files)


def get_file_size_bytes(path: str | Path) -> int:
    """Return the file size in bytes, or 0 if the file does not exist."""
    try:
        return Path(path).stat().st_size
    except OSError:
        return 0


# Archiving & rotation

def archive_file(src: str | Path, dest_dir: str | Path) -> Path:
    """
    Move a successfully-processed source file into dest_dir (data/processed/).

    The file is renamed to include a UTC timestamp so multiple runs of the
    same filename don't overwrite each other.

    Returns the destination path.
    """
    src_path = Path(src)
    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest_file = dest_path / f"{src_path.stem}_{timestamp}{src_path.suffix}"

    shutil.move(str(src_path), str(dest_file))
    return dest_file


def rotate_old_files(directory: str | Path, ttl_days: int = 7) -> int:
    """
    Delete files in directory that are older than ttl_days.

    Used by the logger and quarantine handler to prevent unbounded disk growth.
    Only deletes regular files — never directories.

    Returns the count of deleted files.
    """
    cutoff = datetime.now(timezone.utc).timestamp() - (ttl_days * 86400)
    deleted = 0

    for f in Path(directory).glob("*"):
        if f.is_file() and f.stat().st_mtime < cutoff:
            f.unlink()
            deleted += 1

    return deleted


def detect_format(path: str | Path) -> str:
    """
    Infer file format from extension.

    Returns "csv", "json", or raises ValueError for unsupported formats.
    The pipeline uses this to select the correct parser.
    """
    suffix = Path(path).suffix.lower()
    format_map = {
        ".csv": "csv",
        ".json": "json",
        ".jsonl": "json",
    }
    if suffix not in format_map:
        raise ValueError(
            f"Unsupported file format '{suffix}' for file '{path}'. "
            f"Supported: {list(format_map.keys())}"
        )
    return format_map[suffix]