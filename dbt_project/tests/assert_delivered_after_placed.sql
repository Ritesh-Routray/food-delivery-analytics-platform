-- assert_delivered_after_placed.sql
-- Custom singular test: for delivered orders, delivered_ts must be after placed_ts.
-- Returns rows that violate this rule (zero rows = test passes).

select
    order_id,
    placed_ts,
    delivered_ts
from {{ ref('fact_orders') }}
where
    is_delivered = true
    and delivered_ts is not null
    and placed_ts is not null
    and delivered_ts <= placed_ts
