{{
    config(
        materialized='incremental',
        unique_key = 'event_id',
        tags=["staging"]
    )
}}

SELECT
  CAST(event_id AS INTEGER) AS event_id,
  CAST(driver_id AS INTEGER) AS driver_id,
  CAST(status AS STRING) AS status,

  -- Using the Macro for UK to UTC conversion
  {{ to_utc('event_timestamp') }} AS event_timestamp

FROM {{ source('raw', 'driver_status_events_raw') }}

{% if is_incremental() %}
    WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}