{{
    config(
        materialized='incremental',
        unique_key='city_id',
        tags=["mart", "dimensions", "cities"]
    )
}}

SELECT
    city_id,
    country,
    city_name,
    launch_date
FROM {{ ref('stg_cities') }}

{% if is_incremental() %}
    WHERE launch_date >= (SELECT MAX(launch_date) FROM {{ this }})
{% endif %}