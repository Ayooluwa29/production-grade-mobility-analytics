{{
    config(
        materialized='incremental',
        tags=["staging"]
    )
}}

SELECT
    DISTINCT(trip_id) AS trip_id,
    city_id,
    rider_id,
    driver_id,
    vehicle_id,
    status,
    is_corporate,
    payment_method,
    actual_fare,
    estimated_fare,
    surge_multiplier,
    pickup_at,
    created_at,
    dropoff_at,
    updated_at,
    requested_at
FROM {{ source('raw', 'trips_raw') }}