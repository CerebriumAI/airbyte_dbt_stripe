select
    id as subscription_id,
    billing as subscription_billing,
    start as start_date,
    ended_at
from {{ var('subscriptions') }}