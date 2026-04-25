"""
FastFeast Analytics Module
============================
Query layer for OLAP views and KPI metrics.
Provides pandas DataFrames for dashboard and reporting.
"""

from __future__ import annotations

from typing import Optional
from datetime import date, datetime

import pandas as pd

from warehouse.connection import get_dict_cursor
from utils.logger import get_logger_name

logger = get_logger_name(__name__)


class AnalyticsClient:
    """Client for querying analytics views."""
    
    def __init__(self):
        self.schema = "warehouse"
        # Allow the client to be used from one-off scripts/tests.
        # The main app or dashboard initializes the pool already.
        try:
            from warehouse.connection import get_conn
            with get_conn():
                pass
        except RuntimeError:
            from config.settings import get_settings
            from warehouse.connection import init_pool
            init_pool(get_settings())
    
    def _query_to_df(self, query: str, params: tuple = ()) -> pd.DataFrame:
        """Execute query and return as DataFrame."""
        with get_dict_cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    
    def get_kpi_summary(self) -> dict:
        """Get overall KPI summary as a dictionary."""
        df = self._query_to_df(f"SELECT * FROM {self.schema}.v_kpi_summary")
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    
    def get_ticket_summary_daily(
        self, 
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 90
    ) -> pd.DataFrame:
        """
        Removed by design.

        The analytics scope is intentionally restricted to the required metrics only:
        - total tickets
        - SLA breach rate %
        - average resolution time
        - first response time
        - reopen rate %
        - refund amount
        - revenue impact
        - tickets by city/region
        - tickets by restaurant
        - tickets by driver
        """
        raise NotImplementedError("Daily trends are out of scope for the required analytics.")
    
    def get_tickets_by_location(self, top_n: int = 20) -> pd.DataFrame:
        """Get ticket breakdown by city/region."""
        return self._query_to_df(
            f"SELECT * FROM {self.schema}.v_tickets_by_location LIMIT %s",
            (top_n,)
        )
    
    def get_tickets_by_restaurant(self, top_n: int = 20) -> pd.DataFrame:
        """Get ticket breakdown by restaurant."""
        return self._query_to_df(
            f"SELECT * FROM {self.schema}.v_tickets_by_restaurant LIMIT %s",
            (top_n,)
        )
    
    def get_tickets_by_driver(self, top_n: int = 20) -> pd.DataFrame:
        """Get ticket breakdown by driver."""
        return self._query_to_df(
            f"SELECT * FROM {self.schema}.v_tickets_by_driver LIMIT %s",
            (top_n,)
        )
    
    def get_tickets_by_agent(self, top_n: int = 20) -> pd.DataFrame:
        raise NotImplementedError("Agent breakdown is out of scope for the required analytics.")
    
    def get_tickets_by_reason(self) -> pd.DataFrame:
        raise NotImplementedError("Reason breakdown is out of scope for the required analytics.")
    
    def get_reopen_rate(self) -> dict:
        """Get ticket reopen rate metrics."""
        df = self._query_to_df(f"SELECT * FROM {self.schema}.v_ticket_reopen_rate")
        if df.empty:
            return {"reopen_rate_pct": 0, "total_tickets": 0, "reopened_tickets": 0}
        return df.iloc[0].to_dict()
    
    def get_revenue_impact(self) -> dict:
        """Get revenue impact metrics."""
        df = self._query_to_df(f"SELECT * FROM {self.schema}.v_revenue_impact")
        if df.empty:
            return {}
        return df.iloc[0].to_dict()
    
    def get_hourly_trends(self) -> pd.DataFrame:
        raise NotImplementedError("Hourly trends are out of scope for the required analytics.")
    
    def get_sla_by_priority(self) -> pd.DataFrame:
        raise NotImplementedError("Priority SLA breakdown is out of scope for the required analytics.")
    
    def get_recent_tickets(
        self, 
        days: int = 7,
        status: Optional[str] = None
    ) -> pd.DataFrame:
        """Get recent tickets with details."""
        query = f"""
            SELECT 
                ft.ticket_id,
                ft.status,
                ft.created_at,
                ft.resolved_at,
                ft.first_response_minutes,
                ft.resolution_minutes,
                ft.sla_resolution_breached,
                ft.refund_amount,
                dr.reason_name,
                dr.reason_category_name,
                dp.priority_name,
                da.agent_name,
                dc.customer_name_masked,
                drest.restaurant_name,
                dd.driver_name
            FROM {self.schema}.fact_tickets ft
            JOIN {self.schema}.dim_reason dr ON ft.reason_id = dr.reason_id
            JOIN {self.schema}.dim_priority dp ON ft.priority_id = dp.priority_id
            JOIN {self.schema}.dim_agent da ON ft.agent_key = da.agent_key
            JOIN {self.schema}.dim_customer dc ON ft.customer_key = dc.customer_key
            JOIN {self.schema}.dim_restaurant drest ON ft.restaurant_key = drest.restaurant_key
            JOIN {self.schema}.dim_driver dd ON ft.driver_key = dd.driver_key
            WHERE ft.created_at >= NOW() - (%s::text || ' days')::interval
            {' AND ft.status = %s' if status else ''}
            ORDER BY ft.created_at DESC
            LIMIT 1000
        """
        params = [str(int(days))]
        if status:
            params.append(status)
        return self._query_to_df(query, tuple(params))


# Convenience functions for quick access
def get_summary_metrics() -> dict:
    """Get all key metrics in one call."""
    client = AnalyticsClient()
    
    kpi = client.get_kpi_summary()
    reopen = client.get_reopen_rate()
    revenue = client.get_revenue_impact()
    
    return {
        "total_tickets": kpi.get("total_tickets", 0),
        "sla_breach_rate_pct": kpi.get("sla_resolution_breach_rate_pct", 0),
        "avg_resolution_minutes": kpi.get("avg_resolution_minutes", 0),
        "avg_first_response_minutes": kpi.get("avg_first_response_minutes", 0),
        "reopen_rate_pct": reopen.get("reopen_rate_pct", 0),
        "total_refund_amount": kpi.get("total_refund_amount", 0),
        "net_revenue": revenue.get("net_revenue", 0),
        "refund_impact_rate_pct": revenue.get("refund_impact_rate_pct", 0),
    }


def ensure_analytics_schema() -> None:
    """Create analytics views if they don't exist."""
    from pathlib import Path
    from warehouse.connection import get_conn, init_pool
    from config.settings import get_settings
    
    ddl_path = Path(__file__).resolve().parents[1] / "warehouse" / "analytics_ddl.sql"
    if not ddl_path.exists():
        logger.error("analytics_ddl.sql not found", path=str(ddl_path))
        raise FileNotFoundError(f"DDL not found at {ddl_path}")
    
    ddl = ddl_path.read_text(encoding="utf-8")
    # Allow running this function in isolation (e.g. from CLI snippets).
    # main.py or dashboard already initialize the pool.
    try:
        with get_conn():
            pass
    except RuntimeError:
        init_pool(get_settings())

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    
    logger.info("analytics_schema_ensured", ddl_path=str(ddl_path))
