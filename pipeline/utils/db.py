"""
PostgreSQL connection pool management.
- Provides thread-safe pooling
- context managers to ensure safe connection
- automatic transactions
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Iterator

import psycopg2
import psycopg2.extras  # RealDictCursor
from psycopg2 import pool as pg_pool, OperationalError, InterfaceError
from psycopg2.extensions import TransactionRollbackError

from config.settings import Settings

logger = logging.getLogger(__name__)

# Module-level singleton — set once by init_pool(), read by get_conn()
_pool: pg_pool.ThreadedConnectionPool | None = None


# Initialisation

def init_pool(settings: Settings) -> None:
    """
    Create the connection pool at application startup.

    Call this exactly once from main.py before any pipeline stage runs.
    Calling it a second time replaces the pool and closes the previous one.
    """
    global _pool

    if _pool is not None:
        logger.warning("db.init_pool called while pool already exists — closing old pool")
        _pool.closeall()

    _pool = pg_pool.ThreadedConnectionPool(
        minconn=settings.db.pool_min,
        maxconn=settings.db.pool_max,
        dsn=settings.db.dsn,
        connect_timeout=10,
        # Kill any query that runs longer than 240 s (TO BE TUNED)
        options="-c statement_timeout=240000",
    )
    logger.info(
        "db_pool_initialised",
        extra={
            "host": settings.db.host,
            "port": settings.db.port,
            "dbname": settings.db.name,
            "pool_min": settings.db.pool_min,
            "pool_max": settings.db.pool_max,
        },
    )

    # Register JSONB support for dict objects 
    psycopg2.extras.register_json(conn_or_pool=_pool, globally=True)


def close_pool() -> None:
    """
    Release all connections and destroy the pool.

    Call this from the shutdown handler in main.py (SIGINT / SIGTERM).
    """
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("db_pool_closed")


def health_check() -> bool:
    """
    Verify the pool can reach PostgreSQL.

    Returns True on success, False on any error.
    Used by the orchestrator at startup before the first pipeline run.
    """
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone()[0] == 1
    except Exception as exc:
        logger.error("db_health_check_failed", extra={"error": str(exc)})
        return False


# Context managers (for connection pooling and transactions)

@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    """Borrow a connection with error handling for deadlocks and timeouts."""
    if _pool is None:
        raise RuntimeError("DB pool not initialised. Call init_pool(settings) in main.py first.")

    try:
        conn = _pool.getconn()
    except pg_pool.PoolError as exc:
        logger.error("db_pool_exhausted", extra={"error": str(exc)})
        raise

    try:
        yield conn
        conn.commit()
    except TransactionRollbackError as exc:
        conn.rollback()
        logger.error("db_deadlock_detected", extra={"error": str(exc)})
        raise
    except (OperationalError, InterfaceError) as exc:
        conn.rollback()
        logger.error("db_connection_error", extra={"error": str(exc)})
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


@contextmanager
def get_cursor() -> Generator[psycopg2.extensions.cursor, None, None]:
    """
    Convenience wrapper: borrow a connection and open a standard cursor.
    - NOTE: For queries that return column names as dict keys, use get_dict_cursor().
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            yield cur


@contextmanager
def get_dict_cursor() -> Generator[psycopg2.extras.RealDictCursor, None, None]:
    """
    Same as get_cursor() but rows come back as dicts keyed by column name.
    - Use this for queries that return column names as dict keys.
    """
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur


# ─────────────────────────────────────────────────────────────────────────────
# Helpers for bulk operations
# ─────────────────────────────────────────────────────────────────────────────

def execute_values(sql: str, records: list[dict], page_size: int = 1000) -> int:
    """
    Bulk insert using psycopg2.extras.execute_values.
    - Much faster than executemany() for large batches because it batches
    rows into a single multi-row INSERT rather than one INSERT per row.
    - Returns the number of rows affected.
    """
    if not records:
        return 0

    # Extract column order from first record
    columns = list(records[0].keys())
    values = [tuple(r[c] for c in columns) for r in records]

    with get_cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            sql,
            values,
            page_size=page_size,
        )
        return cur.rowcount


def execute_many(sql: str, params_list: list[tuple]) -> int:
    """
    Thin wrapper around cursor.executemany for parameterised statements.
    - Prefer execute_values() for INSERT — it is significantly faster.
    - Use execute_many() for UPDATE / DELETE where execute_values doesn't apply.
    - Returns the number of rows affected.
    """
    if not params_list:
        return 0

    with get_cursor() as cur:
        cur.executemany(sql, params_list)
        return cur.rowcount