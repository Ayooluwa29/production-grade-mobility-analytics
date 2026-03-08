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

select * from {{ source('raw', 'drivers_raw') }}

{% endsnapshot %}