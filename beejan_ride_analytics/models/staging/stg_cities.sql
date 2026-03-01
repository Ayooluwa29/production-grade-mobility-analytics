{{
    config(
        materialized='incremental',
        tags=["staging"]
    )
}}

SELECT
    DISTINCT(city_id) AS city_id,
    country,
    city_name,
    launch_date
FROM {{ source('raw', 'cities_raw') }}