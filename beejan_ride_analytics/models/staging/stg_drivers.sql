{{
    config(
        materialized='incremental',
        tags=["staging"]
    )
}}

SELECT
    DISTINCT(driver_id) AS driver_id,
    city_id,
    vehicle_id,
    driver_status,
    rating,
    created_at,
    updated_at,
    onboarding_date
FROM {{ source('raw', 'drivers_raw') }}