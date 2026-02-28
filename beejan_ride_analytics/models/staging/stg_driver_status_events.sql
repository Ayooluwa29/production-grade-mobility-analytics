{{
    config(
        materialized='incremental'
    )
}}

SELECT
  status,
  event_id,
  driver_id,
  event_timestamp 
FROM {{ source('raw', 'driver_status_events_raw') }}