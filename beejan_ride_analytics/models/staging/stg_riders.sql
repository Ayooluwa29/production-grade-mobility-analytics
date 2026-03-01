{{
    config(
        materialized='incremental',
        tags=["staging"]
    )
}}

SELECT
  DISTINCT(rider_id) AS rider_id,
  country,
  referral_code,
  created_at,
  signup_date
FROM {{ source('raw', 'riders_raw') }}