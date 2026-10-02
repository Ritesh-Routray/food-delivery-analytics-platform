-- dim_customer.sql
-- Customer dimension — static (no SCD needed for this project).

with customers as (
    select distinct
        customer_id,
        city                        as home_city,
        -- Repeat customer flag comes from order history
        count(order_id) over (partition by customer_id) > 1 as is_repeat_customer
    from {{ ref('fact_orders') }}
    where customer_id is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
    customer_id,
    home_city,
    is_repeat_customer,
    current_timestamp()                                     as loaded_at
from customers
