{% snapshot drivers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='driver_id',
      strategy='check',
      check_cols=['driver_status', 'vehicle_id', 'rating'],
      invalidate_hard_deletes=True
    )
}}

select

    driver_id,
    city_id,
    vehicle_id,
    driver_status,
    rating,
    created_at,
    updated_at,
    onboarding_date

from {{ source('raw', 'drivers_raw') }}

{% endsnapshot %}