select
    id as plan_id,
    active as is_plan_active,
    amount as plan_amount,
    interval as plan_interval,
    interval_count as plan_interval_count,
    nickname as plan_nickname,
    product as plan_product_id
from {{ var('plans') }}