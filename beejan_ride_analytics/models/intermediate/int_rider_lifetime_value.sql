{{
    config(
        materialized='view',
        tags=["intermediate", "rider", "value"]
    )
}}

SELECT
  t.rider_id,
  SUM({{ get_net_revenue('amount', 'fee') }}) AS rider_lifetime_value
FROM {{ ref('stg_trips') }} t
LEFT JOIN {{ ref('stg_payments') }} p
ON t.trip_id = p.trip_id
WHERE t.status = 'completed' AND p.payment_status = 'success'
GROUP BY t.rider_id