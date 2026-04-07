from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, timedelta
from collections import defaultdict

import pandas as pd

from handlers.quarantine_handler import send_batch_to_quarantine
from utils.retry import db_retry
from utils.timing import timed
from validators.schema_registry import get_contract
from validators.schema_validator import partition_critical_validation_rows, validate_entity
from warehouse.connection import get_cursor, get_dict_cursor

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    table_name:       str
    source_file:      str
    batch_date:       date
    total_in:         int = 0
    inserted:         int = 0
    scd2_updated:     int = 0
    scd1_updated:     int = 0
    same_day_updated: int = 0
    unchanged:        int = 0
    errors:           list[str] = field(default_factory=list)

    @property
    def processed_count(self) -> int:
        return self.inserted + self.scd2_updated + self.scd1_updated + self.same_day_updated

    @property
    def error_count(self) -> int:
        return len(self.errors)


class BaseSCD2Loader(ABC):

    @property
    @abstractmethod
    def table_name(self) -> str: ...

    @property
    @abstractmethod
    def natural_key(self) -> str: ...

    @property
    @abstractmethod
    def tracked_fields(self) -> list[str]: ...

    @abstractmethod
    def _build_insert_row(self, record: dict, batch_date: date) -> dict: ...

    @abstractmethod
    def _build_update_fields(self, record: dict) -> dict: ...

    def _scd1_condition(self, incoming: dict, active_row: dict) -> bool:
        return False

    @property
    def source_entity(self) -> str | None:
        """
        Optional entity type for schema validation.
        Override in subclass to enable validation (e.g., 'source_customers').
        Must match a key in the schema registry.
        """
        return None

    @timed
    def load(self, df: pd.DataFrame, batch_date: date, source_file: str, pipeline_run_id: int | None = None) -> LoadResult:
        result = LoadResult(table_name=self.table_name, source_file=source_file, batch_date=batch_date)

        if df.empty:
            logger.warning("scd2_loader_empty_df", table=self.table_name, file=source_file)
            return result

        # Store original count before any filtering
        original_count = len(df)

        # ── Pre-validation: coerce datetime columns to proper type ───────────
        # CSV sources often read datetime columns as strings; convert them
        # before schema validation to avoid "Type mismatch (expected datetime)"
        if self.source_entity:
            source_contract = get_contract(self.source_entity)
            datetime_cols = [c.name for c in source_contract.columns if c.dtype in ("datetime", "date")]
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")

        # ── Schema validation + quarantine (if entity type defined) ─────────
        if self.source_entity:
            errors = validate_entity(df, self.source_entity)
            bad_indices, structural_msg = partition_critical_validation_rows(df, errors)
            if structural_msg is not None:
                logger.error(
                    "scd2_structural_validation_failed",
                    extra={
                        "table": self.table_name,
                        "source_file": source_file,
                        "reason": structural_msg,
                    },
                )
                result.errors.append(f"Structural validation failed: {structural_msg}")
                result.total_in = original_count
                return result

            if bad_indices:
                # Build quarantine records
                errors_by_row: dict = defaultdict(list)
                for e in errors:
                    if e.level == "critical":
                        errors_by_row[e.row_index].append(e.reason)

                quarantine_records = [
                    {
                        "source_file": source_file,
                        "entity_type": self.source_entity,
                        "raw_record": df.loc[idx].to_dict(),
                        "error_type": "schema_validation",
                        "error_details": " | ".join(reasons),
                        "pipeline_run_id": pipeline_run_id,
                    }
                    for idx, reasons in errors_by_row.items()
                    if idx in df.index
                ]
                send_batch_to_quarantine(quarantine_records)

                # Log and filter out bad records
                for idx in bad_indices:
                    reasons = errors_by_row.get(idx, ["unknown error"])
                    record_data = df.loc[idx].to_dict()
                    record_preview = {k: str(v)[:50] for k, v in record_data.items()}
                    
                    # Build detailed error message
                    error_msg = f"QUARANTINE [{self.table_name}] Row {idx}: {' | '.join(reasons)}"
                    record_json = str(record_preview).replace("'", '"')
                    
                    result.errors.append(f"Row {idx}: schema validation failed - {' | '.join(reasons)}")
                    
                    # Explicit console output for debugging
                    print(f"[QUARANTINE] {error_msg}")
                    print(f"  Record: {record_json}")
                    print(f"  File: {source_file}")
                    
                    logger.warning(
                        "scd2_record_quarantined",
                        extra={
                            "row_index": idx,
                            "table": self.table_name,
                            "error_reasons": reasons,
                            "record_preview": record_preview,
                            "source_file": source_file,
                        }
                    )

                df = df.drop(index=list(bad_indices))

            if df.empty:
                logger.warning("scd2_all_records_quarantined", table=self.table_name, file=source_file)
                result.total_in = original_count
                return result

        # Set total_in to original count (before filtering) for accurate metrics
        result.total_in = original_count
        active_rows = self._load_active_rows()

        for _, row in df.iterrows():
            record   = row.to_dict()
            nk_value = record.get(self.natural_key)

            if nk_value is None:
                result.errors.append(f"NULL natural key ({self.natural_key}): {record}")
                continue

            try:
                nk_int = int(nk_value)
            except (TypeError, ValueError):
                result.errors.append(f"Non-integer natural key: {nk_value}")
                continue

            active_row = active_rows.get(nk_int)

            if active_row is None:
                self._insert_new(record, batch_date)
                result.inserted += 1

            elif not self._detect_changes(record, active_row):
                # No tracked changes - check for SCD1 (non-tracked field changes)
                if self._scd1_condition(record, active_row):
                    self._apply_scd1(record, active_row)
                    result.scd1_updated += 1
                else:
                    result.unchanged += 1

            elif active_row["valid_from"] == batch_date:
                self._apply_same_day_update(record, active_row)
                result.same_day_updated += 1

            else:
                # SCD2: Tracked fields changed → expire old row, insert new version
                self._expire_old_row(active_row, batch_date)
                self._insert_new(record, batch_date)
                result.scd2_updated += 1

        logger.info(
            "scd2_load_complete",
            extra={
                "table": self.table_name,
                "batch_date": str(batch_date),
                "original_count": original_count,
                "total_in": result.total_in,
                "inserted": result.inserted,
                "scd2_updated": result.scd2_updated,
                "scd1_updated": result.scd1_updated,
                "same_day_updated": result.same_day_updated,
                "unchanged": result.unchanged,
                "quarantined": len(result.errors),
                "errors": result.error_count,
            }
        )
        return result

    def _load_active_rows(self) -> dict[int, dict]:
        sql = f"""
            SELECT * FROM {self.table_name}
            WHERE  is_current = true
              AND  {self.natural_key} IS NOT NULL
        """
        with get_dict_cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        active = {int(row[self.natural_key]): dict(row) for row in rows if row[self.natural_key] is not None}
        logger.debug("scd2_active_rows_loaded", extra={"table": self.table_name, "count": len(active)})
        return active

    def _detect_changes(self, incoming: dict, active_row: dict) -> bool:
        """
        Detect changes in tracked fields.

        Important: Incoming records often contain strings (e.g. "true", "1") for boolean
        fields or mixed numeric types. We normalize values before comparing to avoid
        false positives that would create unnecessary SCD2 versions.
        """
        normalized_incoming = self._normalized_incoming_for_compare(incoming)

        for f in self.tracked_fields:
            inc_val = normalized_incoming.get(f)
            act_val = active_row.get(f)

            norm_in = self._normalize_value_for_compare(f, inc_val, reference=act_val)
            norm_act = self._normalize_value_for_compare(f, act_val, reference=act_val)

            if norm_in != norm_act:
                logger.debug(
                    "scd2_change_detected",
                    extra={
                        "table": self.table_name,
                        "field": f,
                        "old": norm_act,
                        "new": norm_in,
                        "nk_value": incoming.get(self.natural_key),
                    }
                )
                return True
        return False

    def _normalized_incoming_for_compare(self, incoming: dict) -> dict:
        """
        Prefer each loader's coercions from _build_update_fields() when present.
        This keeps compare semantics aligned with write semantics (e.g., bool/int coercion).
        """
        try:
            normalized = dict(incoming)
        except Exception:
            normalized = {}

        try:
            update_fields = self._build_update_fields(incoming) or {}
            # Only overlay fields that exist in the incoming record or tracked set.
            for k, v in update_fields.items():
                if k in incoming or k in self.tracked_fields:
                    normalized[k] = v
        except Exception:
            # If a loader's update builder throws for bad input, fall back to raw values.
            pass

        return normalized

    @staticmethod
    def _coerce_bool_like(value) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        s = str(value).strip().lower()
        if s in ("true", "1", "yes", "y", "t"):
            return True
        if s in ("false", "0", "no", "n", "f"):
            return False
        return None

    def _normalize_value_for_compare(self, field: str, value, reference=None):
        """
        Normalize values for stable comparisons across common ingestion quirks.
        - booleans: compare as bool where field looks like a boolean or reference is bool
        - numerics: if reference is numeric, try numeric coercion
        - strings: strip whitespace
        """
        if value is None:
            return None

        if field.startswith("is_") or isinstance(reference, bool):
            return self._coerce_bool_like(value)

        if isinstance(reference, int) and not isinstance(reference, bool):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return str(value).strip()

        if isinstance(reference, float):
            try:
                return float(value)
            except (TypeError, ValueError):
                return str(value).strip()

        if isinstance(value, str):
            return value.strip()

        return value

    @db_retry
    def _insert_new(self, record: dict, batch_date: date) -> None:
        row    = self._build_insert_row(record, batch_date)
        cols   = list(row.keys())
        sql    = f"INSERT INTO {self.table_name} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
        with get_cursor() as cur:
            cur.execute(sql, list(row.values()))

    @db_retry
    def _expire_old_row(self, active_row: dict, batch_date: date) -> None:
        sk  = self._surrogate_key_col()
        sql = f"UPDATE {self.table_name} SET valid_to = %s, is_current = false WHERE {sk} = %s"
        with get_cursor() as cur:
            cur.execute(sql, (batch_date - timedelta(days=1), active_row[sk]))

    @db_retry
    def _apply_scd1(self, record: dict, active_row: dict) -> None:
        fields = self._build_update_fields(record)
        if not fields:
            return
        sk      = self._surrogate_key_col()
        set_sql = ", ".join(f"{c} = %s" for c in fields)
        sql     = f"UPDATE {self.table_name} SET {set_sql} WHERE {sk} = %s AND is_current = true"
        with get_cursor() as cur:
            cur.execute(sql, [*fields.values(), active_row[sk]])

    @db_retry
    def _apply_same_day_update(self, record: dict, active_row: dict) -> None:
        fields = self._build_update_fields(record)
        if not fields:
            return
        sk      = self._surrogate_key_col()
        set_sql = ", ".join(f"{c} = %s" for c in fields)
        sql     = f"UPDATE {self.table_name} SET {set_sql} WHERE {sk} = %s AND is_current = true"
        with get_cursor() as cur:
            cur.execute(sql, [*fields.values(), active_row[sk]])

    def _surrogate_key_col(self) -> str:
        entity = self.table_name.split(".")[-1].replace("dim_", "")
        return f"{entity}_key"
