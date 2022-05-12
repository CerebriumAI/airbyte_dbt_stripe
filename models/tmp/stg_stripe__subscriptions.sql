select
    id as subscription_id,
    customer as customer_id,
    status,
    {{ dbt_date.from_unixtimestamp('start') }} as start_date,
    {{ dbt_date.from_unixtimestamp('ended_at') }} as ended_at,
    billing as subscription_billing,
    {{ dbt_date.from_unixtimestamp('billing_cycle_anchor') }} as billing_cycle_anchor,
    {{ dbt_date.from_unixtimestamp('canceled_at') }} as canceled_at,
    {{ dbt_date.from_unixtimestamp('created') }} as created_at,
    {{ dbt_date.from_unixtimestamp('current_period_start') }} as current_period_start,
    {{ dbt_date.from_unixtimestamp('current_period_end') }} as current_period_end,
    {{ dbt_date.from_unixtimestamp('trial_start') }} as trial_start,
    {{ dbt_date.from_unixtimestamp('trial_end') }} as trial_end,
    days_until_due,
    cancel_at_period_end as is_cancel_at_period_end
from {{ var('subscriptions') }}