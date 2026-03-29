"""
Retry logic with exponential backoff using tenacity.

Design decisions:
  - Only retry infrastructure errors (DB connection lost, network blip).
  - NOTE: NEVER RETRY DATA ERRORS (bad schema, null required field) — retrying
    won't fix corrupt data and would hide bugs.
  - Three categories of errors are defined and used throughout the pipeline:
      db_retry      → for PostgreSQL errors
      file_retry    → for file system / IO errors
      network_retry → for network / SMTP errors
"""

from __future__ import annotations

import logging
from typing import Callable, TypeVar

import psycopg2
# use tenacity for retry logic
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)


# Retry logic
def db_retry(func: F) -> F:
    """
    Retry transient PostgreSQL connection errors with exponential backoff.
    Retries on:
    - psycopg2.OperationalError  — connection lost, server restart
    - psycopg2.InterfaceError    — connection closed unexpectedly
    Backoff: 2s → 4s → 8s (max 30s between attempts), up to 3 attempts total.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,          # re-raise original exception after all attempts fail
    )(func)


def file_retry(func: F) -> F:
    """
    Retry transient file system errors (e.g. NFS blips, locked files).
    Retries on:
    - OSError   — covers IOError, FileNotFoundError on network mounts
    - TimeoutError
    Backoff: 1s → 2s → 4s, up to 3 attempts.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((OSError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def network_retry(func: F) -> F:
    """
    Retry transient network errors (e.g. SMTP connection refused).

    Retries on:
      ConnectionError
      TimeoutError
      OSError

    Backoff: 5s → 10s → 20s, up to 3 attempts.
    Used primarily by the email notifier in the alerting layer.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=5, max=60),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_with(
    max_attempts: int = 3,
    min_wait: float = 2.0,
    max_wait: float = 30.0,
    exception_types: tuple = (Exception,),
) -> Callable[[F], F]:
    """
    Configurable retry factory for one-off cases that don't fit the standard decorators.
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(exception_types),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )