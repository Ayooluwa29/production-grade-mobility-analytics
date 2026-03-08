{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        tags=["mart", "facts", "trips"]
    )
}}

SELECT
    t.trip_id,
    t.city_id,
    t.rider_id,
    t.driver_id,
    t.vehicle_id,

    dh.driver_surrogate_key,

    t.status,
    t.rider_type,
    t.payment_method,
    t.surge_multiplier_flag,
    t.duration_minutes,

    dh.driver_status AS driver_status_at_trip,
    dh.rating AS driver_rating_at_trip,
    dh.vehicle_id AS driver_vehicle_at_trip,

    t.actual_fare,
    t.estimated_fare,
    t.surge_multiplier,
    p.fee,
    p.amount AS amount_paid,
    p.net_revenue,
    p.payment_status,
    p.payment_provider,
    p.currency,
    p.duplicate_payments,

    f.fraud_detected,

    t.requested_at,
    t.created_at,
    t.pickup_at,
    t.dropoff_at,
    t.updated_at

FROM {{ ref('int_trips_enriched') }} t

LEFT JOIN {{ ref('dim_drivers_history') }} dh
    ON  t.driver_id   = dh.driver_id
    AND t.pickup_at  >= dh.valid_from
    AND (t.pickup_at  < dh.valid_to OR dh.valid_to IS NULL)

LEFT JOIN {{ ref('int_payments_enriched') }} p
    ON t.trip_id = p.trip_id

LEFT JOIN {{ ref('int_fraud_indicators') }} f
    ON t.trip_id = f.trip_id

{% if is_incremental() %}
    WHERE t.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}