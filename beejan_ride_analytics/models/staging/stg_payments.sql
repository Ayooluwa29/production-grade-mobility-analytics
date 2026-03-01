{{
    config(
        materialized='incremental',
        tags=["staging"]
    )
}}

SELECT
  DISTINCT(payment_id) AS payment_id,
  trip_id,
  payment_status,
  payment_provider,
  currency,
  fee,
  amount,
  created_at
  FROM {{ source('raw', 'payments_raw') }}