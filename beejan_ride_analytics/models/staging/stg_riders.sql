{{
    config(
        materialized='incremental'
    )
}}

SELECT
  country,
  rider_id,
  created_at,
  signup_date,
  referral_code
FROM {{ source('raw', 'riders_raw') }}