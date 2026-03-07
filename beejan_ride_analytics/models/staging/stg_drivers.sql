{{
    config(
        materialized='incremental',
        unique_key = 'driver_id',
        tags=["staging", "drivers"]
    )
}}

SELECT
    CAST(driver_id AS INTEGER) AS driver_id,
    CAST(city_id AS INTEGER) AS city_id,
    CAST(vehicle_id AS STRING) AS vehicle_id,
    CAST(driver_status AS STRING) AS driver_status,
    CAST(rating AS NUMERIC) AS rating,

    -- Using the to_utc Macro for all timestamp fields
    {{ to_utc('created_at') }} AS created_at,
    {{ to_utc('updated_at') }} AS updated_at,
    {{ to_utc('onboarding_date') }} AS onboarding_date

FROM {{ source('raw', 'drivers_raw') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}