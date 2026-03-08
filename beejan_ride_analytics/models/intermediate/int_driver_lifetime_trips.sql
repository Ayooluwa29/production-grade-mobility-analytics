{{
    config(
        materialized='view',
        tags=["intermediate", "driver", "trips"]
    )
}}

SELECT
    driver_id,
    count(trip_id) as driver_lifetime_trips
FROM {{ ref('stg_trips') }}
WHERE status = 'completed'
GROUP BY driver_id