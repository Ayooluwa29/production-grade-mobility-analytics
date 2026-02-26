{{
    config(
        materialized='incremental'
    )
}}

SELECT
    status,
    city_id,
    trip_id,
    rider_id,
    driver_id,
    pickup_at,
    created_at,
    dropoff_at,
    updated_at,
    vehicle_id,
    actual_fare,
    is_corporate,
    requested_at,
    estimated_fare,
    payment_method,
    surge_multiplier
FROM {{ source('raw', 'trips_raw') }}