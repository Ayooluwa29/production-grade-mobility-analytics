{{
    config(
        materialized='view',
        tags=["intermediate","fraud_flag"]
    )
}}

WITH fraud_detect AS (
SELECT
    p.payment_id,
    t.trip_id,
    t.driver_id,
    t.rider_id,
    (t.estimated_fare * t.surge_multiplier) AS expected_fare,
    t.actual_fare,
    p.amount
FROM {{ ref('stg_trips') }} AS t
LEFT JOIN {{ ref('stg_payments') }} AS p
ON t.trip_id=p.trip_id
)
 SELECT
    payment_id,
    trip_id,
    driver_id,
    rider_id,
    expected_fare,
    actual_fare,
    amount AS amount_paid,
    CASE
        WHEN (expected_fare > actual_fare) THEN TRUE
        WHEN (actual_fare > amount) THEN TRUE
    ELSE FALSE
    END AS fraud_detected
FROM fraud_detect