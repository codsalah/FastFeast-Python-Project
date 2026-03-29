-- ============================================================================
-- FastFeast DWH Seed Data 
-- Purpose: Pre-load '-1' Unknown members to handle orphan references cleanly.
-- ============================================================================

-- Customer Dimension
INSERT INTO "dim_customer" (
    "customer_key", "customer_id", "customer_name_masked", "gender", 
    "segment_name", "region_name", "city_name", "signup_date", 
    "valid_from", "valid_to", "is_current"
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', 'Unknown', NULL, 
    '1900-01-01', NULL, true
);

-- Driver Dimension
INSERT INTO "dim_driver" (
    "driver_key", "driver_id", "driver_name", "vehicle_type", "shift", 
    "region_name", "city_name", "is_active", "valid_from", "valid_to", "is_current"
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', false, '1900-01-01', NULL, true
);

-- Restaurant Dimension
INSERT INTO "dim_restaurant" (
    "restaurant_key", "restaurant_id", "restaurant_name", "category_name", 
    "price_tier", "region_name", "city_name", "rating_avg", 
    "is_active", "valid_from", "valid_to", "is_current"
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 
    'Unknown', 'Unknown', 'Unknown', NULL, 
    false, '1900-01-01', NULL, true
);

-- Agent Dimension (For support tickets)
INSERT INTO "dim_agent" (
    "agent_key", "agent_id", "agent_name", "skill_level", "team_name", 
    "is_active", "valid_from", "valid_to", "is_current"
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 'Unknown', 
    false, '1900-01-01', NULL, true
);

-- Reason Dimension (For support tickets)
INSERT INTO "dim_reason" (
    "reason_key", "reason_id", "reason_name", "reason_category_name", 
    "severity_level", "typical_refund_pct"
) VALUES (
    -1, NULL, 'Unknown', 'Unknown', 0, 0.0000
);

-- Priority Dimension
INSERT INTO "dim_priority" (
    "priority_key", "priority_id", "priority_code", "priority_name", 
    "sla_first_response_min", "sla_resolution_min"
) VALUES (
    -1, NULL, 'UNK', 'Unknown', 0, 0
);

-- Channel Dimension
INSERT INTO "dim_channel" (
    "channel_key", "channel_id", "channel_name"
) VALUES (
    -1, NULL, 'Unknown'
);