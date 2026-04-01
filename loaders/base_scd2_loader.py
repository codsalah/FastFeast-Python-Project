# File: ./base_scd2_loader.py
from __future__ import annotations
import logging
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from utils.retry import db_retry
from utils.timing import timed
from warehouse.connection import get_cursor, get_dict_cursor

logger = logging.getLogger(__name__)

@dataclass
class LoadResult:
    table_name: str
    source_file: str
    batch_date: date
    total_in: int = 0
    inserted: int = 0
    scd2_updated: int = 0
    scd1_updated: int = 0
    same_day_updated: int = 0
    unchanged: int = 0
    errors: list[str] = field(default_factory=list)

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
        """Override this to return True if only SCD1 (in-place) changes exist."""
        return False

    @timed
    def load(self, df: pd.DataFrame, batch_date: date, source_file: str) -> LoadResult:
        result = LoadResult(table_name=self.table_name, source_file=source_file, batch_date=batch_date)
        if df.empty: return result

        result.total_in = len(df)
        active_rows = self._load_active_rows()

        for _, row in df.iterrows():
            record = {k: (v if pd.notna(v) else None) for k, v in row.to_dict().items()}
            nk_value = record.get(self.natural_key)
            if nk_value is None: continue

            nk_int = int(nk_value)
            active_row = active_rows.get(nk_int)

            if active_row is None:
                self._insert_new(record, batch_date)
                result.inserted += 1

            elif not self._detect_changes(record, active_row):
                result.unchanged += 1

            elif self._scd1_condition(record, active_row):
                before = {f: active_row.get(f) for f in self.tracked_fields}
                after  = self._build_update_fields(record)
                diffs  = [f"{f}: {before[f]} -> {after[f]}" for f in self.tracked_fields if before.get(f) != after.get(f)]
                self._apply_scd1(record, active_row)
                print(f"\n[SCD1 CHANGE] Table: {self.table_name} | NK: {nk_int}")
                print(f"  CHANGES: {', '.join(diffs) or '(non-tracked field, e.g. rating_avg)'}")
                print(f"  record_before_scd1: {before}")
                print(f"  record_after_scd1:  {after}")
                result.scd1_updated += 1

            elif active_row["valid_from"] == batch_date:
                before = {f: active_row.get(f) for f in self.tracked_fields}
                after  = self._build_update_fields(record)
                diffs  = [f"{f}: {before[f]} -> {after[f]}" for f in self.tracked_fields if before.get(f) != after.get(f)]
                self._apply_same_day_update(record, active_row)
                print(f"\n[SAME-DAY UPDATE] Table: {self.table_name} | NK: {nk_int}")
                print(f"  CHANGES: {', '.join(diffs)}")
                print(f"  record_before_update: {before}")
                print(f"  record_after_update:  {after}")
                result.same_day_updated += 1

            else:
                # TRIPLE TRACKING WITH DIFF
                before = {f: active_row.get(f) for f in self.tracked_fields}
                after  = self._build_update_fields(record)
                diffs  = [f"{f}: {before[f]} -> {after[f]}" for f in self.tracked_fields if before[f] != after[f]]

                self._expire_old_row(active_row, batch_date)
                newly_added = self._insert_new(record, batch_date)

                print(f"\n[SCD2 CHANGE] Table: {self.table_name} | NK: {nk_int}")
                print(f"  CHANGES: {', '.join(diffs)}")
                print(f"  record_before_scd2: {before}")
                print(f"  record_after_scd2:  {after}")
                print(f"  record_newly_added: {newly_added}")

                result.scd2_updated += 1
        return result

    @db_retry
    def _insert_new(self, record: dict, batch_date: date) -> dict:
        row = self._build_insert_row(record, batch_date)
        cols = list(row.keys())
        sql = f"INSERT INTO {self.table_name} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
        with get_cursor() as cur:
            cur.execute(sql, list(row.values()))
        return row

    def _load_active_rows(self) -> dict:
        sql = f"SELECT * FROM {self.table_name} WHERE is_current = true"
        with get_dict_cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return {int(r[self.natural_key]): dict(r) for r in rows if r[self.natural_key] is not None}

    def _detect_changes(self, incoming: dict, active_row: dict) -> bool:
        norm_inc = self._normalized_incoming_for_compare(incoming)
        for f in self.tracked_fields:
            act_val = active_row.get(f)
            inc_val = norm_inc.get(f)

            # If the active DB row has NULL for this field, it means it was never
            # populated (e.g. a denormalized lookup that wasn't joined on first load).
            # Filling in a previously-NULL field is NOT a real business change — skip it.
            if act_val is None and inc_val is not None:
                continue

            if self._norm_val(f, inc_val, act_val) != self._norm_val(f, act_val, act_val):
                return True
        return False

    def _normalized_incoming_for_compare(self, incoming: dict) -> dict:
        norm = dict(incoming)
        norm.update(self._build_update_fields(incoming) or {})
        return norm

    def _norm_val(self, field, val, ref):
        if val is None:
            return None
        if isinstance(ref, bool) or field.startswith("is_"):
            return self._coerce_bool(val)
        if isinstance(ref, (int, float)):
            try: return float(val)
            except: return val
        return str(val).strip()

    @staticmethod
    def _coerce_bool(v) -> bool | None:
        if v is None or v == "": return None
        if isinstance(v, bool): return v
        s = str(v).strip().lower()
        return s in ("true", "1", "yes", "t", "y")

    @db_retry
    def _expire_old_row(self, active_row: dict, batch_date: date) -> None:
        sk = self._surrogate_key_col()
        sql = f"UPDATE {self.table_name} SET valid_to = %s, is_current = false WHERE {sk} = %s"
        with get_cursor() as cur:
            cur.execute(sql, (batch_date - timedelta(days=1), active_row[sk]))

    @db_retry
    def _apply_scd1(self, record: dict, active_row: dict) -> None:
        fields = self._build_update_fields(record)
        sk = self._surrogate_key_col()
        set_sql = ", ".join(f"{c} = %s" for c in fields)
        sql = f"UPDATE {self.table_name} SET {set_sql} WHERE {sk} = %s"
        with get_cursor() as cur:
            cur.execute(sql, [*fields.values(), active_row[sk]])

    @db_retry
    def _apply_same_day_update(self, record: dict, active_row: dict) -> None:
        self._apply_scd1(record, active_row)

    def _surrogate_key_col(self) -> str:
        return f"{self.table_name.split('.')[-1].replace('dim_', '')}_key"