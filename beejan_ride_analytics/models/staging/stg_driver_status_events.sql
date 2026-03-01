{{
    config(
        materialized='incremental',
        tags=["staging"]
    )
}}

SELECT
  DISTINCT(event_id) AS event_id,
  driver_id,
  status,
  event_timestamp 
FROM {{ source('raw', 'driver_status_events_raw') }}