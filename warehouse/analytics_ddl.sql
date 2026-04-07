-- ============================================================
-- FastFeast Analytics Views (OLAP Layer)
-- Schema: warehouse
-- Contains: ONLY the required KPIs and breakdowns for the dashboard
-- ============================================================

-- PostgreSQL limitation: CREATE OR REPLACE VIEW cannot remove columns from an
-- existing view definition. To keep the analytics scope strict, we drop and
-- recreate the views deterministically.
DROP VIEW IF EXISTS warehouse.v_sla_by_priority;
DROP VIEW IF EXISTS warehouse.v_ticket_trends_hourly;
DROP VIEW IF EXISTS warehouse.v_tickets_by_agent;
DROP VIEW IF EXISTS warehouse.v_tickets_by_reason;
DROP VIEW IF EXISTS warehouse.v_ticket_summary_daily;
DROP VIEW IF EXISTS warehouse.v_kpi_summary;
DROP VIEW IF EXISTS warehouse.v_ticket_reopen_rate;
DROP VIEW IF EXISTS warehouse.v_revenue_impact;
DROP VIEW IF EXISTS warehouse.v_tickets_by_driver;
DROP VIEW IF EXISTS warehouse.v_tickets_by_restaurant;
DROP VIEW IF EXISTS warehouse.v_tickets_by_location;

-- ============================================================
-- 1. TICKETS BY CITY/REGION
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_tickets_by_location AS
SELECT 
    dc.region_name,
    dc.city_name,
    COUNT(DISTINCT ft.ticket_id) AS total_tickets,
    SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) AS resolution_breaches,
    ROUND(
        100.0 * SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0), 2
    ) AS sla_breach_rate_pct,
    ROUND(AVG(ft.resolution_minutes)::numeric, 2) AS avg_resolution_minutes,
    SUM(ft.refund_amount) AS total_refund_amount,
    ROUND(AVG(ft.refund_amount), 2) AS avg_refund_amount
FROM warehouse.fact_tickets ft
JOIN warehouse.dim_customer dc ON ft.customer_key = dc.customer_key
GROUP BY dc.region_name, dc.city_name
ORDER BY total_tickets DESC;

-- ============================================================
-- 2. TICKETS BY RESTAURANT
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_tickets_by_restaurant AS
SELECT 
    dr.restaurant_name,
    dr.category_name,
    dr.city_name,
    dr.region_name,
    COUNT(DISTINCT ft.ticket_id) AS total_tickets,
    SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) AS resolution_breaches,
    ROUND(
        100.0 * SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0), 2
    ) AS sla_breach_rate_pct,
    ROUND(AVG(ft.resolution_minutes)::numeric, 2) AS avg_resolution_minutes,
    SUM(ft.refund_amount) AS total_refund_amount,
    ROUND(AVG(ft.refund_amount), 2) AS avg_refund_amount
FROM warehouse.fact_tickets ft
JOIN warehouse.dim_restaurant dr ON ft.restaurant_key = dr.restaurant_key
GROUP BY dr.restaurant_name, dr.category_name, dr.city_name, dr.region_name
ORDER BY total_tickets DESC;

-- ============================================================
-- 3. TICKETS BY DRIVER
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_tickets_by_driver AS
SELECT 
    dd.driver_name,
    dd.vehicle_type,
    dd.city_name,
    dd.region_name,
    COUNT(DISTINCT ft.ticket_id) AS total_tickets,
    SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) AS resolution_breaches,
    ROUND(
        100.0 * SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0), 2
    ) AS sla_breach_rate_pct,
    ROUND(AVG(ft.resolution_minutes)::numeric, 2) AS avg_resolution_minutes,
    SUM(ft.refund_amount) AS total_refund_amount,
    ROUND(AVG(ft.refund_amount), 2) AS avg_refund_amount
FROM warehouse.fact_tickets ft
JOIN warehouse.dim_driver dd ON ft.driver_key = dd.driver_key
GROUP BY dd.driver_name, dd.vehicle_type, dd.city_name, dd.region_name
ORDER BY total_tickets DESC;

-- ============================================================
-- 4. OVERALL KPI SUMMARY (Single Row Summary)
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_kpi_summary AS
SELECT 
    COUNT(DISTINCT ft.ticket_id) AS total_tickets,
    ROUND(AVG(ft.first_response_minutes)::numeric, 2) AS avg_first_response_minutes,
    ROUND(AVG(ft.resolution_minutes)::numeric, 2) AS avg_resolution_minutes,
    ROUND(
        100.0 * SUM(CASE WHEN ft.sla_first_response_breached THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0), 2
    ) AS sla_first_response_breach_rate_pct,
    ROUND(
        100.0 * SUM(CASE WHEN ft.sla_resolution_breached THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0), 2
    ) AS sla_resolution_breach_rate_pct,
    SUM(ft.refund_amount) AS total_refund_amount,
    COUNT(DISTINCT CASE WHEN ft.refund_amount > 0 THEN ft.ticket_id END) AS tickets_with_refund
FROM warehouse.fact_tickets ft
;

-- ============================================================
-- 5. REOPEN RATE % (Based on Ticket Events)
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_ticket_reopen_rate AS
WITH ticket_status_changes AS (
    SELECT 
        ft.ticket_id,
        COUNT(DISTINCT CASE 
            WHEN fte.new_status = 'resolved' THEN fte.event_key 
        END) AS resolve_count,
        MAX(CASE 
            WHEN fte.new_status = 'resolved' THEN fte.event_ts 
        END) AS last_resolved_at,
        MAX(CASE 
            WHEN fte.new_status = 'reopened' THEN fte.event_ts 
        END) AS last_reopened_at
    FROM warehouse.fact_tickets ft
    LEFT JOIN warehouse.fact_ticket_events fte ON ft.ticket_key = fte.ticket_key
    GROUP BY ft.ticket_id
)
SELECT 
    COUNT(DISTINCT ticket_id) AS total_tickets,
    COUNT(DISTINCT CASE 
        WHEN last_reopened_at > last_resolved_at THEN ticket_id 
    END) AS reopened_tickets,
    ROUND(
        100.0 * COUNT(DISTINCT CASE 
            WHEN last_reopened_at > last_resolved_at THEN ticket_id 
        END) / 
        NULLIF(COUNT(DISTINCT ticket_id), 0), 2
    ) AS reopen_rate_pct,
    COUNT(DISTINCT CASE WHEN resolve_count > 1 THEN ticket_id END) AS multiply_resolved_tickets
FROM ticket_status_changes;

-- ============================================================
-- 6. REVENUE IMPACT
-- ============================================================
-- Order totals are aggregated once from orders_clean; refunds from tickets separately
-- so multiple tickets per order do not inflate SUM(total_amount).
CREATE OR REPLACE VIEW warehouse.v_revenue_impact AS
WITH order_totals AS (
    SELECT
        COALESCE(SUM(order_amount), 0)   AS total_order_revenue,
        COALESCE(SUM(discount_amount), 0) AS total_discounts,
        COALESCE(SUM(total_amount), 0)   AS total_revenue
    FROM warehouse.orders_clean
),
ticket_refunds AS (
    SELECT
        COALESCE(SUM(refund_amount), 0) AS total_refunds_from_tickets,
        COUNT(DISTINCT CASE WHEN refund_amount > 0 THEN order_id END) AS orders_with_refunds
    FROM warehouse.fact_tickets
),
revenue_at_risk AS (
    SELECT COALESCE(SUM(fo.total_amount), 0) AS revenue_at_risk
    FROM warehouse.orders_clean fo
    WHERE EXISTS (
        SELECT 1 FROM warehouse.fact_tickets ft
        WHERE ft.order_id = fo.order_id AND ft.refund_amount > 0
    )
)
SELECT
    ot.total_order_revenue,
    ot.total_discounts,
    ot.total_revenue,
    tr.total_refunds_from_tickets,
    ot.total_revenue - tr.total_refunds_from_tickets AS net_revenue,
    ROUND(
        100.0 * tr.total_refunds_from_tickets / NULLIF(ot.total_revenue, 0),
        4
    ) AS refund_impact_rate_pct,
    rar.revenue_at_risk,
    tr.orders_with_refunds
FROM order_totals ot
CROSS JOIN ticket_refunds tr
CROSS JOIN revenue_at_risk rar;
