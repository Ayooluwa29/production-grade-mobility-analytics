{{
    config(
        materialized='incremental'
    )
}}

SELECT
    city_id,
    country,
    city_name,
    launch_date
FROM {{ source('raw', 'cities_raw') }}