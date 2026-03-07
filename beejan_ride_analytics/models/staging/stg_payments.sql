{{
    config(
        materialized='incremental',
        unique_key = 'payment_id',
        tags=["staging", "payments"]
    )
}}

SELECT
  CAST(payment_id AS INTEGER) AS payment_id,
  CAST(trip_id AS INTEGER) AS trip_id,
  CAST(payment_status AS STRING) AS payment_status,
  CAST(payment_provider AS STRING) AS payment_provider,
  CAST(currency AS STRING) AS currency,
  CAST(fee AS NUMERIC) AS fee,
  CAST(amount AS INTEGER) AS amount,

  -- Using the Macro for UK to UTC conversion
  {{ to_utc('created_at') }} AS created_at

FROM {{ source('raw', 'payments_raw') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}