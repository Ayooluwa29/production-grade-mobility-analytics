{{
    config(
        materialized='incremental',
        unique_key = 'rider_id',
        tags=["staging", "riders"]
    )
}}

SELECT
  CAST(rider_id AS INTEGER) AS rider_id,
  CAST(country AS STRING) AS country,
  CAST(referral_code AS STRING) AS referral_code,

  -- Using the Macro for UK to UTC conversion
  {{ to_utc('created_at') }} AS created_at,
  {{ to_utc('signup_date') }} AS signup_date

FROM {{ source('raw', 'riders_raw') }}

{% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}