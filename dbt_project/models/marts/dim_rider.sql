-- dim_rider.sql
-- Rider dimension with productivity metrics derived from fact_orders.

with rider_stats as (
    select
        rider_id,
        city,
        count(*)                                            as total_deliveries,
        round(avg(total_delivery_minutes), 2)              as avg_delivery_minutes,
        round(avg(distance_km), 2)                         as avg_distance_km,
        round(sum(coalesce(tip_amount, 0)), 2)             as total_tips_earned,
        min(placed_ts)                                     as first_delivery_ts,
        max(delivered_ts)                                  as last_delivery_ts
    from {{ ref('fact_orders') }}
    where is_delivered = true
      and rider_id is not null
    group by rider_id, city
),

tier as (
    select
        *,
        case
            when total_deliveries >= 500 then 'gold'
            when total_deliveries >= 100 then 'silver'
            else 'bronze'
        end as performance_tier
    from rider_stats
)

select
    {{ dbt_utils.generate_surrogate_key(['rider_id']) }} as rider_sk,
    rider_id,
    city,
    total_deliveries,
    avg_delivery_minutes,
    avg_distance_km,
    total_tips_earned,
    performance_tier,
    first_delivery_ts,
    last_delivery_ts,
    current_timestamp() as loaded_at
from tier
