{{
    config(
        materialized='incremental',
        unique_key='driver_id',
        tags=["mart", "dimensions", "drivers"]
    )
}}

SELECT
    d.driver_id,
    d.city_id,
    d.vehicle_id,
    d.driver_status,
    d.rating,
    COALESCE(dl.driver_lifetime_trips, 0) AS driver_lifetime_trips,
    d.onboarding_date,
    d.created_at,
    d.updated_at
FROM {{ ref('stg_drivers') }} d
LEFT JOIN {{ ref('int_driver_lifetime_trips') }} dl
    ON d.driver_id = dl.driver_id

{% if is_incremental() %}
    WHERE d.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}