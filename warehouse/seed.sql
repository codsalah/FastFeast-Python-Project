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
);

-- Driver Dimension
INSERT INTO dim_driver (
    driver_key, driver_id, driver_name, vehicle_type, shift, 
    region_name, city_name, is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', false, '1900-01-01', NULL, true
);

-- Restaurant Dimension
INSERT INTO dim_restaurant (
    restaurant_key, restaurant_id, restaurant_name, category_name, 
    price_tier, region_name, city_name, rating_avg, 
    is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', 'Unknown', NULL, 
    false, '1900-01-01', NULL, true
);
