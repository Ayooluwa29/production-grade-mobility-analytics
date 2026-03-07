{{
    config(
        materialized='incremental',
        unique_key = 'city_id',
        tags=["staging","cities"]
    )
}}

SELECT
    CAST(city_id AS INTEGER) AS city_id,
    CAST(country AS STRING) AS country,
    CAST(city_name AS STRING) AS city_name,
    CAST(launch_date AS DATE) AS launch_date
FROM {{ source('raw', 'cities_raw') }}

{% if is_incremental() %}
    WHERE launch_date >= (SELECT MAX(launch_date) FROM {{ this }})
{% endif %}