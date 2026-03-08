{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        tags=["mart", "facts", "payments"]
    )
}}

SELECT
    p.payment_id,
    p.trip_id,
    f.driver_id,
    f.rider_id,
    p.payment_status,
    p.payment_provider,
    p.currency,
    p.duplicate_payments,
    p.fee,
    p.amount,
    p.net_revenue,
    f.fraud_detected,
    f.expected_fare,
    f.actual_fare,
    p.created_at

FROM {{ ref('int_payments_enriched') }} p
LEFT JOIN {{ ref('int_fraud_indicators') }} f
    ON p.trip_id = f.trip_id

{% if is_incremental() %}
    WHERE p.created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}