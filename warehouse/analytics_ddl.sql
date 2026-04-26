-- ============================================================
-- FastFeast Analytics Views (OLAP Layer)
-- Schema: warehouse
-- Contains: ONLY the required KPIs and breakdowns for the dashboard
-- ============================================================

-- PostgreSQL limitation: CREATE OR REPLACE VIEW cannot remove columns from an
-- existing view definition. To keep the analytics scope strict, we drop and
-- recreate the views deterministically.
DROP VIEW IF EXISTS warehouse.v_business_kpi_summary;
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
DROP VIEW IF EXISTS warehouse.v_order_summary;
DROP VIEW IF EXISTS warehouse.v_order_trends_daily;
DROP VIEW IF EXISTS warehouse.v_orders_by_region;
DROP VIEW IF EXISTS warehouse.v_orders_by_restaurant;

-- ============================================================
-- 1. TICKETS BY CITY/REGION
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_tickets_by_location AS
WITH latest_orders_for_orphans AS (
    SELECT
        order_id,
        customer_key,
        driver_key,
        restaurant_key
    FROM warehouse.fact_orders
    WHERE version = (
        SELECT MAX(version)
        FROM warehouse.fact_orders fo2
        WHERE fo2.order_id = fact_orders.order_id
    )
    AND order_id IN (
        SELECT order_id
        FROM warehouse.fact_tickets
        WHERE customer_key = -1 OR driver_key = -1 OR restaurant_key = -1
    )
)
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
LEFT JOIN latest_orders_for_orphans lo ON ft.order_id = lo.order_id
INNER JOIN warehouse.dim_customer dc ON COALESCE(lo.customer_key, ft.customer_key) = dc.customer_key
WHERE COALESCE(lo.customer_key, ft.customer_key) != -1
  AND COALESCE(lo.driver_key, ft.driver_key) != -1
  AND COALESCE(lo.restaurant_key, ft.restaurant_key) != -1
  AND dc.region_name IS NOT NULL
  AND dc.city_name IS NOT NULL
GROUP BY dc.region_name, dc.city_name
ORDER BY total_tickets DESC;

-- ============================================================
-- 2. TICKETS BY RESTAURANT
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_tickets_by_restaurant AS
WITH latest_orders_for_orphans AS (
    SELECT
        order_id,
        customer_key,
        driver_key,
        restaurant_key
    FROM warehouse.fact_orders
    WHERE version = (
        SELECT MAX(version)
        FROM warehouse.fact_orders fo2
        WHERE fo2.order_id = fact_orders.order_id
    )
    AND order_id IN (
        SELECT order_id
        FROM warehouse.fact_tickets
        WHERE customer_key = -1 OR driver_key = -1 OR restaurant_key = -1
    )
)
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
LEFT JOIN latest_orders_for_orphans lo ON ft.order_id = lo.order_id
INNER JOIN warehouse.dim_restaurant dr ON COALESCE(lo.restaurant_key, ft.restaurant_key) = dr.restaurant_key
WHERE COALESCE(lo.customer_key, ft.customer_key) != -1
  AND COALESCE(lo.driver_key, ft.driver_key) != -1
  AND COALESCE(lo.restaurant_key, ft.restaurant_key) != -1
  AND dr.restaurant_name IS NOT NULL
  AND dr.category_name IS NOT NULL
  AND dr.city_name IS NOT NULL
  AND dr.region_name IS NOT NULL
GROUP BY dr.restaurant_name, dr.category_name, dr.city_name, dr.region_name
ORDER BY total_tickets DESC;

-- ============================================================
-- 3. TICKETS BY DRIVER
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_tickets_by_driver AS
WITH latest_orders_for_orphans AS (
    SELECT
        order_id,
        customer_key,
        driver_key,
        restaurant_key
    FROM warehouse.fact_orders
    WHERE version = (
        SELECT MAX(version)
        FROM warehouse.fact_orders fo2
        WHERE fo2.order_id = fact_orders.order_id
    )
    AND order_id IN (
        SELECT order_id
        FROM warehouse.fact_tickets
        WHERE customer_key = -1 OR driver_key = -1 OR restaurant_key = -1
    )
)
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
LEFT JOIN latest_orders_for_orphans lo ON ft.order_id = lo.order_id
INNER JOIN warehouse.dim_driver dd ON COALESCE(lo.driver_key, ft.driver_key) = dd.driver_key
WHERE COALESCE(lo.customer_key, ft.customer_key) != -1
  AND COALESCE(lo.driver_key, ft.driver_key) != -1
  AND COALESCE(lo.restaurant_key, ft.restaurant_key) != -1
  AND dd.driver_name IS NOT NULL
  AND dd.vehicle_type IS NOT NULL
  AND dd.city_name IS NOT NULL
  AND dd.region_name IS NOT NULL
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
    COUNT(DISTINCT CASE WHEN ft.refund_amount > 0 THEN ft.ticket_id END) AS tickets_with_refund,
    ROUND(AVG(CASE WHEN ft.refund_amount > 0 THEN ft.refund_amount END)::numeric, 2) AS avg_refund_amount
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
            WHEN fte.new_status = 'Resolved' THEN fte.event_key
        END) AS resolve_count,
        COUNT(DISTINCT CASE 
            WHEN fte.new_status = 'Reopened' THEN fte.event_key
        END) AS reopen_count
    FROM warehouse.fact_tickets ft
    LEFT JOIN warehouse.fact_ticket_events fte ON ft.ticket_key = fte.ticket_key
    GROUP BY ft.ticket_id
)
SELECT
    COUNT(DISTINCT ticket_id) AS total_tickets,
    COUNT(DISTINCT CASE
        WHEN reopen_count > 0 THEN ticket_id
    END) AS reopened_tickets,
    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN reopen_count > 0 THEN ticket_id
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

-- ============================================================
-- 7. ORDER SUMMARY (Business Metrics)
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_order_summary AS
SELECT 
    COUNT(DISTINCT fo.order_id) AS total_orders,
    ROUND(AVG(fo.order_amount)::numeric, 2) AS avg_order_amount,
    ROUND(AVG(fo.total_amount)::numeric, 2) AS avg_total_amount,
    ROUND(AVG(fo.delivery_fee)::numeric, 2) AS avg_delivery_fee,
    SUM(fo.total_amount) AS total_revenue,
    SUM(fo.discount_amount) AS total_discounts,
    COUNT(DISTINCT CASE WHEN fo.order_status = 'Delivered' THEN fo.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN fo.order_status = 'Cancelled' THEN fo.order_id END) AS cancelled_orders,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN fo.order_status = 'Delivered' THEN fo.order_id END) / 
        NULLIF(COUNT(DISTINCT fo.order_id), 0), 2
    ) AS delivery_rate_pct,
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM warehouse.fact_tickets ft WHERE ft.order_id = fo.order_id
    ) THEN fo.order_id END) AS orders_with_tickets
FROM warehouse.fact_orders fo;

-- ============================================================
-- 8. ORDER TRENDS DAILY
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_order_trends_daily AS
SELECT 
    DATE(fo.order_created_at) AS order_date,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.total_amount) AS daily_revenue,
    ROUND(AVG(fo.total_amount)::numeric, 2) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN fo.order_status = 'Delivered' THEN fo.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN fo.order_status = 'Cancelled' THEN fo.order_id END) AS cancelled_orders,
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM warehouse.fact_tickets ft WHERE ft.order_id = fo.order_id
    ) THEN fo.order_id END) AS orders_with_tickets
FROM warehouse.fact_orders fo
GROUP BY DATE(fo.order_created_at)
ORDER BY order_date DESC;

-- ============================================================
-- 9. ORDERS BY REGION
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_orders_by_region AS
SELECT
    dr.region_name,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.total_amount) AS total_revenue,
    ROUND(AVG(fo.total_amount)::numeric, 2) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN fo.order_status = 'Delivered' THEN fo.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM warehouse.fact_tickets ft WHERE ft.order_id = fo.order_id
    ) THEN fo.order_id END) AS orders_with_tickets
FROM warehouse.fact_orders fo
INNER JOIN warehouse.dim_restaurant dr ON fo.restaurant_key = dr.restaurant_key
WHERE fo.restaurant_key != -1
  AND dr.region_name IS NOT NULL
GROUP BY dr.region_name
ORDER BY total_orders DESC;

-- ============================================================
-- 10. ORDERS BY RESTAURANT
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_orders_by_restaurant AS
SELECT
    dr.restaurant_name,
    dr.category_name,
    dr.region_name,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(fo.total_amount) AS total_revenue,
    ROUND(AVG(fo.total_amount)::numeric, 2) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN fo.order_status = 'Delivered' THEN fo.order_id END) AS delivered_orders,
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM warehouse.fact_tickets ft WHERE ft.order_id = fo.order_id
    ) THEN fo.order_id END) AS orders_with_tickets
FROM warehouse.fact_orders fo
INNER JOIN warehouse.dim_restaurant dr ON fo.restaurant_key = dr.restaurant_key
WHERE fo.restaurant_key != -1
  AND dr.restaurant_name IS NOT NULL
  AND dr.category_name IS NOT NULL
  AND dr.region_name IS NOT NULL
GROUP BY dr.restaurant_name, dr.category_name, dr.region_name
ORDER BY total_orders DESC
LIMIT 20;

-- ============================================================
-- 11. COMPREHENSIVE BUSINESS KPI SUMMARY
-- ============================================================
CREATE OR REPLACE VIEW warehouse.v_business_kpi_summary AS
SELECT 
    -- Order Metrics
    os.total_orders,
    os.avg_order_amount,
    os.total_revenue,
    os.delivery_rate_pct,
    os.orders_with_tickets,
    ROUND(
        100.0 * os.orders_with_tickets / NULLIF(os.total_orders, 0), 2
    ) AS ticket_rate_pct,
    -- Ticket Metrics
    ks.total_tickets,
    ks.avg_first_response_minutes,
    ks.avg_resolution_minutes,
    ks.sla_first_response_breach_rate_pct,
    ks.sla_resolution_breach_rate_pct,
    ks.total_refund_amount,
    ks.tickets_with_refund,
    ks.avg_refund_amount,
    -- Revenue Impact
    ri.refund_impact_rate_pct,
    ri.net_revenue,
    -- Reopen Rate
    tr.reopen_rate_pct
FROM warehouse.v_order_summary os
CROSS JOIN warehouse.v_kpi_summary ks
CROSS JOIN warehouse.v_revenue_impact ri
CROSS JOIN warehouse.v_ticket_reopen_rate tr;
