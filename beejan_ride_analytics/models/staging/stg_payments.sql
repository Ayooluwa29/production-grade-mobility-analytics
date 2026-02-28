{{
    config(
        materialized='incremental'
    )
}}

SELECT
  fee,
  amount,
  trip_id,
  currency,
  created_at,
  payment_id,
  payment_status,
  payment_provider
  FROM {{ source('raw', 'payments_raw') }}