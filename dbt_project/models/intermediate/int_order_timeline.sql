-- int_order_timeline.sql
-- Pivots lifecycle events into one row per order with all stage timestamps.
-- This is the central intermediate model used by fact_orders and quality checks.

with events as (
    select * from {{ ref('stg_order_events') }}
),

order_timeline as (
    select
        order_id,

        -- Customer / restaurant / rider (take first non-null across events)
        max(customer_id)    as customer_id,
        max(restaurant_id)  as restaurant_id,
        max(rider_id)       as rider_id,
        max(city)           as city,

        -- Stage timestamps (pivot by event_type)
        max(case when event_type = 'order_placed'    then event_ts end) as placed_ts,
        max(case when event_type = 'order_confirmed' then event_ts end) as confirmed_ts,
        max(case when event_type = 'order_preparing' then event_ts end) as preparing_ts,
        max(case when event_type = 'order_picked_up' then event_ts end) as picked_up_ts,
        max(case when event_type = 'order_delivered' then event_ts end) as delivered_ts,
        max(case when event_type = 'order_cancelled' then event_ts end) as cancelled_ts,
        max(case when event_type = 'order_rejected'  then event_ts end) as rejected_ts,

        -- Order attributes from placed event
        max(case when event_type = 'order_placed' then order_value end)       as order_value,
        max(case when event_type = 'order_placed' then item_count end)         as item_count,
        max(case when event_type = 'order_placed' then payment_method end)     as payment_method,

        -- Delivery attributes from delivered event
        max(case when event_type = 'order_delivered' then distance_km end)    as distance_km,
        max(case when event_type = 'order_delivered' then tip_amount end)      as tip_amount,

        -- Cancellation attributes
        max(case when event_type = 'order_cancelled' then cancelled_by end)    as cancelled_by,

        -- Status flags
        max(case when event_type = 'order_delivered'  then 1 else 0 end) = 1  as is_delivered,
        max(case when event_type = 'order_cancelled'  then 1 else 0 end) = 1  as is_cancelled,
        max(case when event_type = 'order_rejected'   then 1 else 0 end) = 1  as is_rejected,

        -- Derived durations (in minutes) — all nullable until both timestamps exist
        datediff('minute', 
            max(case when event_type = 'order_placed'    then event_ts end),
            max(case when event_type = 'order_confirmed' then event_ts end)
        ) as confirmation_wait_minutes,

        datediff('minute',
            max(case when event_type = 'order_confirmed' then event_ts end),
            max(case when event_type = 'order_picked_up' then event_ts end)
        ) as rider_assignment_wait_minutes,

        datediff('minute',
            max(case when event_type = 'order_picked_up' then event_ts end),
            max(case when event_type = 'order_delivered' then event_ts end)
        ) as transit_minutes,

        datediff('minute',
            max(case when event_type = 'order_placed'    then event_ts end),
            max(case when event_type = 'order_delivered' then event_ts end)
        ) as total_delivery_minutes,

        -- Audit
        max(event_ts) as last_event_ts,
        current_timestamp() as updated_at

    from events
    group by order_id
)

select * from order_timeline
