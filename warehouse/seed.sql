-- ============================================================================
-- FastFeast DWH Seed Data
-- Unknown (-1) members: customer, driver, restaurant only (orphan dimensions).
-- Other dimensions are loaded from batch files only.
-- ============================================================================

SET search_path TO warehouse, public;

INSERT INTO dim_customer (
    customer_key, customer_id, customer_name_masked, gender,
    segment_name, region_name, city_name, signup_date,
    valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown',
    'Unknown', 'Unknown', 'Unknown', NULL,
    '1900-01-01', NULL, true
) ON CONFLICT (customer_key) DO NOTHING;

INSERT INTO dim_driver (
    driver_key, driver_id, driver_name, vehicle_type, shift,
    region_name, city_name, is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 'Unknown',
    'Unknown', 'Unknown', false, '1900-01-01', NULL, true
) ON CONFLICT (driver_key) DO NOTHING;

INSERT INTO dim_restaurant (
    restaurant_key, restaurant_id, restaurant_name, category_name,
    price_tier, region_name, city_name, rating_avg,
    is_active, valid_from, valid_to, is_current
) VALUES (
    -1, NULL, 'Unknown', 'Unknown',
    'Unknown', 'Unknown', 'Unknown', NULL,
    false, '1900-01-01', NULL, true
) ON CONFLICT (restaurant_key) DO NOTHING;
