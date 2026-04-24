"""
handlers/backfill_handler.py
────────────────────────────
Batch-triggered orphan reconciliation wrapper.
"""

from __future__ import annotations

from pipelines.reconciliation_job import DEFAULT_MAX_RETRIES, run_reconciliation_after_batch


def run_backfill_after_batch(run_id: int, max_retries: int | None = None) -> dict[str, int]:
    """
    Run the full post-batch orphan reconciliation pass.
    """
    lim = max_retries if max_retries is not None else DEFAULT_MAX_RETRIES
    return run_reconciliation_after_batch(run_id, max_retries=lim)
