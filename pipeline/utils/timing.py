
"""
pipeline/utils/timing.py
────────────────────────
Stage latency tracking utilities.

Every pipeline stage should report how long it took.  These utilities make
that a one-line addition to any function.
"""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable, Tuple, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)


def timed(func: F) -> F:
    """
    Decorator that logs the wall-clock time of any function call.

    The log entry includes:
      - function name
      - duration_ms (float, milliseconds)
      - success or the exception class on failure

    The function's return value and exceptions are passed through unchanged.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            duration_ms = (time.perf_counter() - start) * 1000
            logger.debug(
                "stage_timing",
                extra={
                    "function": func.__qualname__,
                    "duration_ms": round(duration_ms, 2),
                    "status": "ok",
                },
            )
            return result
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            logger.warning(
                "stage_timing",
                extra={
                    "function": func.__qualname__,
                    "duration_ms": round(duration_ms, 2),
                    "status": "error",
                    "error_type": type(exc).__name__,
                },
            )
            raise

    return wrapper  # type: ignore[return-value]


def measure_ms(func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """
    Call func(*args, **kwargs) and return (result, duration_ms).

    Use this when you need the latency as a value (e.g. to write it to
    quality_runs) rather than just logging it.

    Usage:
        result, latency_ms = measure_ms(load_orders, records, run_id=run_id)
        quality_metrics.avg_processing_latency_ms = latency_ms
    """
    start = time.perf_counter()
    result = func(*args, **kwargs)
    duration_ms = (time.perf_counter() - start) * 1000
    return result, round(duration_ms, 2)


class StageTimer:
    """
    Context manager for timing a named pipeline stage.

    Usage:
        with StageTimer("load_dim_customer") as t:
            repository.upsert(df, run_id)
        print(f"Loaded in {t.duration_ms:.1f} ms")
    """

    def __init__(self, stage_name: str) -> None:
        self.stage_name = stage_name
        self.duration_ms: float = 0.0
        self._start: float = 0.0

    def __enter__(self) -> "StageTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.duration_ms = round((time.perf_counter() - self._start) * 1000, 2)
        status = "error" if exc_type else "ok"
        logger.debug(
            "stage_timer",
            extra={
                "stage": self.stage_name,
                "duration_ms": self.duration_ms,
                "status": status,
            },
        )
        return False  # never suppress exceptions