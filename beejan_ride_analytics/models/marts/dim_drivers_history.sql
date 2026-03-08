{{
    config(
        materialized='incremental',
        unique_key='driver_surrogate_key',
        tags=["mart", "dimensions", "drivers"]
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['driver_id', 'dbt_scd_id']) }} AS driver_surrogate_key,
    driver_id,
    city_id,
    vehicle_id,
    driver_status,
    rating,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END AS is_current,
    FALSE AS is_deleted,
    created_at,
    updated_at

FROM {{ ref('drivers_snapshot') }}

{% if is_incremental() %}
    WHERE dbt_updated_at > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}