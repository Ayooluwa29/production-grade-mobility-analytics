{{
    config(
        materialized='incremental',
        unique_key='event_id',
        tags=["mart", "facts", "drivers"]
    )
}}

SELECT
    e.event_id,
    e.driver_id,
    d.city_id,
    e.status AS event_status,
    e.event_timestamp,
    -- Previous event context
    LAG(e.status) OVER (PARTITION BY e.driver_id ORDER BY e.event_timestamp
                        ) AS previous_status,
    -- Flag status transitions
    CASE
        WHEN LAG(e.status) OVER (PARTITION BY e.driver_id ORDER BY e.event_timestamp
                                ) != e.status THEN TRUE
        ELSE FALSE
    END AS is_status_change,
    -- Flag potential churn (active → inactive/suspended)
    CASE
        WHEN e.status IN ('inactive', 'suspended')
        AND LAG(e.status) OVER (PARTITION BY e.driver_id ORDER BY e.event_timestamp
                                ) = 'active' THEN TRUE
        ELSE FALSE
    END AS is_churn_event

FROM {{ ref('stg_driver_status_events') }} e
LEFT JOIN {{ ref('dim_drivers_current') }} d
    ON e.driver_id = d.driver_id

{% if is_incremental() %}
    WHERE e.event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
{% endif %}