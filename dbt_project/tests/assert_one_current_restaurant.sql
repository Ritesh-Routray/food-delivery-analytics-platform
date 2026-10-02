-- assert_one_current_restaurant.sql
-- Custom test: each restaurant_id must have exactly one current row in dim_restaurant.

select
    restaurant_id,
    count(*) as current_row_count
from {{ ref('dim_restaurant') }}
where is_current = true
group by restaurant_id
having count(*) <> 1
