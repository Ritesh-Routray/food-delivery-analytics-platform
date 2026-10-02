-- stg_restaurants.sql
-- Staging model for silver.restaurant_master
-- Light cleaning and renaming only. No business logic.

with source as (
    select * from {{ source('silver', 'restaurant_master') }}
),

renamed as (
    select
        restaurant_id,
        name                                        as restaurant_name,
        city,
        cuisine,
        commission_tier,
        is_active,
        cast(updated_ts as timestamp)               as updated_ts,
        schema_version,
        -- Derived: whether this is a premium partner
        commission_tier in ('premium', 'enterprise') as is_premium_partner
    from source
)

select * from renamed
