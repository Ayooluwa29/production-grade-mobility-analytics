{{
    config(
        materialized='view',
        tags=["Intermediate", "payments"]
    )
}}

SELECT
    payment_id,
    trip_id,
    payment_status,
    payment_provider,
    currency,
    CASE
      WHEN COUNT(*) OVER(PARTITION BY trip_id) > 1 
           AND payment_status = 'success' THEN TRUE
      ELSE FALSE
    END AS duplicate_payments,
    fee,
    amount,
    {{ get_net_revenue('amount', 'fee') }} AS net_revenue,
    created_at
FROM {{ ref('stg_payments') }}
