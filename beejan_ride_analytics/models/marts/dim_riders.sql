{{
    config(
        materialized='incremental',
        unique_key='rider_id',
        tags=["mart", "dimensions", "riders"]
    )
}}

SELECT
    r.rider_id,
    r.country,
    r.referral_code,
    COALESCE(rv.rider_lifetime_value, 0) AS rider_lifetime_value,
    r.signup_date,
    r.created_at
FROM {{ ref('stg_riders') }} r
LEFT JOIN {{ ref('int_rider_lifetime_value') }} rv
    ON r.rider_id = rv.rider_id

{% if is_incremental() %}
    WHERE r.created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}