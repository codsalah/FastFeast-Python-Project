"""
config/schema_manager.py
Apply warehouse + audit DDL, populate dim_date, insert unknown-member sentinels.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from config.settings import Settings
from loaders.dim_date_loader import load_dim_date
from utils.logger import get_logger_name
from warehouse.connection import get_conn

logger = get_logger_name(__name__)


@dataclass
class SchemaManager:
    settings: Settings
    warehouse_ddl_path: Path = field(default_factory=lambda: Path("warehouse/dwh_ddl.sql"))
    audit_ddl_path: Path     = field(default_factory=lambda: Path("warehouse/audit_ddl.sql"))
    seed_path: Path          = field(default_factory=lambda: Path("warehouse/seed.sql"))

    def ensure_all(self, *, with_seed: bool = False) -> None:
        """
        Apply DDL in dependency order, then seed dim_date.

        Order:
          1. audit_ddl    — must exist before warehouse references it
          2. warehouse_ddl
          3. seed.sql     — only when with_seed=True (inserts -1 sentinel rows)
          4. dim_date     — always populated / extended
        """
        self._apply_sql_file(self.audit_ddl_path)
        self._apply_sql_file(self.warehouse_ddl_path)
        if with_seed:
            self._apply_sql_file(self.seed_path)
        self.ensure_dim_date()

    def ensure_dim_date(self, start_year: int = 2020, end_year: int = 2030) -> None:
        """Populate dim_date for [start_year, end_year]. Idempotent."""
        load_dim_date(start_year=start_year, end_year=end_year)

    def _apply_sql_file(self, path: Path) -> None:
        if not path.exists():
            logger.warning("ddl_missing", path=str(path))
            return
        ddl = path.read_text(encoding="utf-8")
        with get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(ddl)
        logger.info("schema_applied", name=path.stem, path=str(path))