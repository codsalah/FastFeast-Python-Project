-- ============================================================================
-- FastFeast DWH Seed Data 
-- Purpose: Pre-load '-1' Unknown members to handle orphan references cleanly.
-- ============================================================================

-- Set search path to warehouse schema
SET search_path TO warehouse, public;

-- Customer Dimension
INSERT INTO dim_customer (
    customer_key, customer_id, customer_name_masked, gender, 
    segment_name, region_name, city_name, signup_date, 
    valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', 'Unknown', NULL, 
    '1900-01-01', NULL, true
) ON CONFLICT (customer_key) DO NOTHING;

-- Driver Dimension
INSERT INTO dim_driver (
    driver_key, driver_id, driver_name, vehicle_type, shift, 
    region_name, city_name, is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', false, '1900-01-01', NULL, true
) ON CONFLICT (driver_key) DO NOTHING;

-- Restaurant Dimension
INSERT INTO dim_restaurant (
    restaurant_key, restaurant_id, restaurant_name, category_name, 
    price_tier, region_name, city_name, rating_avg, 
    is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', 'Unknown', NULL, 
    false, '1900-01-01', NULL, true
) ON CONFLICT (restaurant_key) DO NOTHING;

-- Agent Dimension
INSERT INTO dim_agent (
    agent_key, agent_id, agent_name, skill_level, 
    team_name, is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 
    'Unknown', false, '1900-01-01', NULL, true
) ON CONFLICT (agent_key) DO NOTHING;

-- Static Dimensions
INSERT INTO dim_channel (channel_id, channel_name) VALUES (-1, 'Unknown') ON CONFLICT DO NOTHING;
INSERT INTO dim_priority (priority_id, priority_code, priority_name, sla_first_response_min, sla_resolution_min) 
VALUES (-1, 'NA', 'Unknown', 0, 0) ON CONFLICT DO NOTHING;
INSERT INTO dim_reason (reason_id, reason_name, reason_category_name, severity_level, typical_refund_pct) 
VALUES (-1, 'Unknown', 'Unknown', 0, 0.0) ON CONFLICT DO NOTHING;

-- Fact Unknowns (shell rows for parent-child RI)
DO $$
DECLARE
    v_date_key INT;
BEGIN
    -- We need a valid date_key from dim_date. If dim_date is empty, this seeds nothing.
    SELECT date_key INTO v_date_key FROM warehouse.dim_date LIMIT 1;
    
    IF v_date_key IS NOT NULL THEN
        INSERT INTO warehouse.fact_orders (
            order_key, order_id, customer_key, driver_key, restaurant_key, 
            region_id, date_key, order_status, order_created_at
        ) VALUES (
            -1, '00000000-0000-0000-0000-000000000000', -1, -1, -1, 
            1, v_date_key, 'Unknown', '1900-01-01 00:00:00'
        ) ON CONFLICT (order_key) DO NOTHING;

        INSERT INTO warehouse.fact_tickets (
            ticket_key, ticket_id, order_key, order_id, customer_key, 
            agent_key, reason_id, priority_id, channel_id, date_key, 
            status, created_at
        ) VALUES (
            -1, '00000000-0000-0000-0000-000000000000', -1, '00000000-0000-0000-0000-000000000000', -1, 
            -1, -1, -1, -1, v_date_key, 
            'Unknown', '1900-01-01 00:00:00'
        ) ON CONFLICT (ticket_key) DO NOTHING;
    END IF;
END $$;
