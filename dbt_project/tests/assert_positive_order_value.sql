-- assert_positive_order_value.sql
-- Custom test: order_value must be > 0 for all placed orders.

select
    order_id,
    order_value
from {{ ref('fact_orders') }}
where
    order_value is not null
    and order_value <= 0
