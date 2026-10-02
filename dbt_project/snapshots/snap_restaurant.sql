-- snap_restaurant.sql
-- SCD Type 2 snapshot for restaurant master data.
-- Tracks changes to: name, cuisine, commission_tier, is_active.
--
-- Each time any of these columns changes, a new row is added with:
--   dbt_valid_from = now
--   dbt_valid_to   = NULL (current row)
-- The previous row gets dbt_valid_to = now.

{% snapshot snap_restaurant %}

{{
  config(
    target_schema='snapshots',
    strategy='check',
    unique_key='restaurant_id',
    check_cols=['name', 'cuisine', 'commission_tier', 'is_active'],
    invalidate_hard_deletes=true
  )
}}

select
    restaurant_id,
    name,
    city,
    cuisine,
    commission_tier,
    is_active,
    cast(updated_ts as timestamp) as updated_ts

from {{ source('silver', 'restaurant_master') }}

{% endsnapshot %}
