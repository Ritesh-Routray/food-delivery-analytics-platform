{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

-- fact_orders.sql
-- Central fact table: one row per order with full lifecycle attributes.
--
-- Incremental strategy: merge on order_id, filtered by updated_at.
-- Lookback window (var: incremental_lookback_days) handles late-arriving updates
-- where an order can be updated hours/days after first seen.

with timeline as (
    select * from {{ ref('int_order_timeline') }}

    {% if is_incremental() %}
    -- Late-arriving update window: include orders updated in the last N days
    where updated_at > (
        select dateadd(day, -{{ var('incremental_lookback_days', 3) }}, max(updated_at))
        from {{ this }}
    )
    {% endif %}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_sk,

        -- Natural key
        order_id,

        -- Foreign keys (to dimension tables)
        customer_id,
        restaurant_id,
        rider_id,
        city,

        -- Date dimension key (placed date)
        cast(placed_ts as date) as order_date,

        -- Lifecycle timestamps
        placed_ts,
        confirmed_ts,
        preparing_ts,
        picked_up_ts,
        delivered_ts,
        cancelled_ts,
        rejected_ts,

        -- Measures
        order_value,
        item_count,
        payment_method,
        distance_km,
        tip_amount,
        coalesce(tip_amount, 0) + order_value as total_order_value,

        -- Duration metrics (minutes)
        confirmation_wait_minutes,
        rider_assignment_wait_minutes,
        transit_minutes,
        total_delivery_minutes,

        -- Status flags
        is_delivered,
        is_cancelled,
        is_rejected,
        not is_delivered and not is_cancelled and not is_rejected as is_active,

        -- Derived GMV flag (only count delivered orders in GMV)
        case when is_delivered then order_value else 0 end as gmv_contribution,

        -- Cancelled_by
        cancelled_by,

        -- Audit
        last_event_ts,
        updated_at

    from timeline
)

select * from final
