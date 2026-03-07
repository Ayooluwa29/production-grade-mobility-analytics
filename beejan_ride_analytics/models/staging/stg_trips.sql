{{
    config(
        materialized='incremental',
        unique_key = 'trip_id',
        tags=["staging", "trips"]
    )
}}

SELECT
    CAST(trip_id AS INTEGER) AS trip_id,
    CAST(city_id AS INTEGER) AS city_id,
    CAST(rider_id AS INTEGER) AS rider_id,
    CAST(driver_id AS INTEGER) AS driver_id,
    CAST(vehicle_id AS STRING) AS vehicle_id,

    CAST(status AS STRING) AS status,
    CAST(is_corporate AS BOOLEAN) AS is_corporate,
    CAST(payment_method AS STRING) AS payment_method,
    CAST(actual_fare AS INTEGER) AS actual_fare,
    CAST(estimated_fare AS INTEGER) AS estimated_fare,
    CAST(surge_multiplier AS NUMERIC) AS surge_multiplier,

    -- Using the Macro for UK to UTC conversion
    {{ to_utc('pickup_at') }} AS pickup_at,
    {{ to_utc('created_at') }} AS created_at,
    {{ to_utc('dropoff_at') }} AS dropoff_at,
    {{ to_utc('updated_at') }} AS updated_at,
    {{ to_utc('requested_at') }} AS requested_at

FROM {{ source('raw', 'trips_raw') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}