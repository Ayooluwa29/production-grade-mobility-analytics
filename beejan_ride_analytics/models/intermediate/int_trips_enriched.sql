{{
    config(
        materialized='view',
        tags=["Intermediate", "trip"]
    )
}}

SELECT
    trip_id,
    city_id,
    rider_id,
    driver_id,
    vehicle_id,
    status,
    is_corporate,
    CASE
        WHEN is_corporate IS TRUE THEN 'Corporate'
        WHEN is_corporate IS FALSE THEN 'Personal'
    ELSE 'Unknown'
    END AS rider_type,
    payment_method,
    actual_fare,
    estimated_fare,
    surge_multiplier,
    CASE
        WHEN surge_multiplier > 10 THEN 'Extreme'
        WHEN surge_multiplier < 10 THEN 'Normal'
    END AS surge_multiplier_flag,
    {{ get_duration_minutes('pickup_at', 'dropoff_at') }} AS duration_minutes,
    pickup_at,
    created_at,
    dropoff_at,
    updated_at,
    requested_at
FROM {{ ref('stg_trips') }}