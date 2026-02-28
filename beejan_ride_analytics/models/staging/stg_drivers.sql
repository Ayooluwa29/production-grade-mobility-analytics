{{
    config(
        materialized='incremental'
    )
}}

SELECT
    rating,
    city_id, 
    driver_id,
    created_at,
    updated_at,
    vehicle_id,
    driver_status,
    onboarding_date
FROM {{ source('raw', 'drivers_raw') }}