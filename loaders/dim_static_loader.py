"""
loaders/dim_static_loader.py
PURE loader for static dimension tables - NO TRACKING INSIDE!
Matches the DDL where static tables use natural keys as primary keys.
"""

from __future__ import annotations

import os
from typing import Dict, List

import pandas as pd
from utils.readers import reader
from warehouse.connection import get_cursor
from utils.logger import get_logger_name

logger = get_logger_name(__name__)


class StaticDimLoader:
    """PURE loader for static dimensions"""
    
    def __init__(self, batch_dir: str):
        self.batch_dir = batch_dir
        
    def load_all(self) -> Dict[str, int]:
        """Load all static dimension tables."""
        results = {}
        results["dim_channel"] = self.load_channels()
        results["dim_priority"] = self.load_priorities()
        results["dim_reason"] = self.load_reasons()
        return results
    
    def load_channels(self) -> int:
        """Load dim_channel from channels.csv.
        DDL: channel_id is PRIMARY KEY, channel_name
        """
        file_path = os.path.join(self.batch_dir, "channels.csv")
        
        if not os.path.exists(file_path):
            logger.warning("channels_file_not_found", path=file_path)
            return 0
        
        df = reader(file_path)
        if df is None or df.empty:
            logger.warning("channels_empty", file=file_path)
            return 0
        
        records = []
        for _, row in df.iterrows():
            channel_id = row.get("channel_id")
            if channel_id is not None:
                records.append({
                    "channel_id": int(channel_id),
                    "channel_name": row.get("channel_name", "unknown")
                })
        
        if records:
            loaded = self._upsert_channels(records)
            logger.info("channels_loaded", count=loaded, file=file_path)
            return loaded
        return 0
    
    def load_priorities(self) -> int:
        """Load dim_priority from priorities.csv.
        DDL: priority_id is PRIMARY KEY
        """
        file_path = os.path.join(self.batch_dir, "priorities.csv")
        
        if not os.path.exists(file_path):
            logger.warning("priorities_file_not_found", path=file_path)
            return 0
        
        df = reader(file_path)
        if df is None or df.empty:
            logger.warning("priorities_empty", file=file_path)
            return 0
        
        records = []
        for _, row in df.iterrows():
            priority_id = row.get("priority_id")
            if priority_id is not None:
                records.append({
                    "priority_id": int(priority_id),
                    "priority_code": row.get("priority_code", "P4"),
                    "priority_name": row.get("priority_name", "Low"),
                    "sla_first_response_min": int(row.get("sla_first_response_min", 1)),
                    "sla_resolution_min": int(row.get("sla_resolution_min", 15))
                })
        
        if records:
            loaded = self._upsert_priorities(records)
            logger.info("priorities_loaded", count=loaded, file=file_path)
            return loaded
        return 0
    
    def load_reasons(self) -> int:
        """Load dim_reason from reasons.csv + reason_categories.csv.
        DDL: reason_id is PRIMARY KEY 
        """
        reasons_path = os.path.join(self.batch_dir, "reasons.csv")
        categories_path = os.path.join(self.batch_dir, "reason_categories.csv")
        
        if not os.path.exists(reasons_path):
            logger.warning("reasons_file_not_found", path=reasons_path)
            return 0
        
        # Load reason categories for lookup
        categories = {}
        if os.path.exists(categories_path):
            cat_df = reader(categories_path)
            if cat_df is not None:
                for _, row in cat_df.iterrows():
                    cat_id = row.get("reason_category_id")
                    if cat_id:
                        categories[int(cat_id)] = row.get("category_name", "Unknown")
        
        df = reader(reasons_path)
        if df is None or df.empty:
            logger.warning("reasons_empty", file=reasons_path)
            return 0
        
        records = []
        for _, row in df.iterrows():
            reason_id = row.get("reason_id")
            if reason_id is not None:
                cat_id = row.get("reason_category_id")
                category_name = categories.get(int(cat_id), "Unknown") if cat_id else "Unknown"
                
                records.append({
                    "reason_id": int(reason_id),
                    "reason_name": row.get("reason_name", "Unknown"),
                    "reason_category_name": category_name,
                    "severity_level": int(row.get("severity_level", 1)),
                    "typical_refund_pct": float(row.get("typical_refund_pct", 0.0))
                })
        
        if records:
            loaded = self._upsert_reasons(records)
            logger.info("reasons_loaded", count=loaded, file=reasons_path)
            return loaded
        return 0
    
    def _upsert_channels(self, records: List[dict]) -> int:
        """Upsert using channel_id as PK."""
        sql = """
            INSERT INTO warehouse.dim_channel (channel_id, channel_name)
            VALUES (%(channel_id)s, %(channel_name)s)
            ON CONFLICT (channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name
        """
        with get_cursor() as cur:
            for rec in records:
                cur.execute(sql, rec)
            return len(records)
    
    def _upsert_priorities(self, records: List[dict]) -> int:
        """Upsert using priority_id as PK."""
        sql = """
            INSERT INTO warehouse.dim_priority 
                (priority_id, priority_code, priority_name, 
                sla_first_response_min, sla_resolution_min)
            VALUES (%(priority_id)s, %(priority_code)s, %(priority_name)s,
                    %(sla_first_response_min)s, %(sla_resolution_min)s)
            ON CONFLICT (priority_id) DO UPDATE SET
                priority_code = EXCLUDED.priority_code,
                priority_name = EXCLUDED.priority_name,
                sla_first_response_min = EXCLUDED.sla_first_response_min,
                sla_resolution_min = EXCLUDED.sla_resolution_min
        """
        with get_cursor() as cur:
            for rec in records:
                cur.execute(sql, rec)
            return len(records)
    
    def _upsert_reasons(self, records: List[dict]) -> int:
        """Upsert using reason_id as PK (no separate reason_key)."""
        sql = """
            INSERT INTO warehouse.dim_reason 
                (reason_id, reason_name, reason_category_name, 
                severity_level, typical_refund_pct)
            VALUES (%(reason_id)s, %(reason_name)s, %(reason_category_name)s,
                    %(severity_level)s, %(typical_refund_pct)s)
            ON CONFLICT (reason_id) DO UPDATE SET
                reason_name = EXCLUDED.reason_name,
                reason_category_name = EXCLUDED.reason_category_name,
                severity_level = EXCLUDED.severity_level,
                typical_refund_pct = EXCLUDED.typical_refund_pct
        """
        with get_cursor() as cur:
            for rec in records:
                cur.execute(sql, rec)
            return len(records)


def load_static_dimensions(batch_dir: str, run_id: int) -> Dict[str, int]:
    """Load static dimensions. tracking handled by orchestrator."""
    loader = StaticDimLoader(batch_dir)
    return loader.load_all()