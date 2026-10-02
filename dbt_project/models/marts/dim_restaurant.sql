-- dim_restaurant.sql
-- Current-state restaurant dimension, built from the SCD2 snapshot.
-- Only exposes the current (active) row for each restaurant.

select
    {{ dbt_utils.generate_surrogate_key(['restaurant_id', 'dbt_valid_from']) }} as restaurant_sk,
    restaurant_id,
    name             as restaurant_name,
    city,
    cuisine,
    commission_tier,
    is_active,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_restaurant') }}
