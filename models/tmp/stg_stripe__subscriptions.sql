select
    id as subscription_id,
    billing as subscription_billing,
    {{ dbt_date.from_unixtimestamp('start') }} as start_date,
    {{ dbt_date.from_unixtimestamp('ended_at') }} as ended_at
from {{ var('subscriptions') }}