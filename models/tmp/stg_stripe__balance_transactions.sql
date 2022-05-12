select
    id as balance_transaction_id,
    {{ dbt_date.from_unixtimestamp('created') }} as created_at,
    {{ dbt_date.from_unixtimestamp('available_on') }} as available_on,
    fee,
    net as net_balance_change,
    type,
    status,
    amount,
    currency,
    exchange_rate,
    source
from {{ var('balance_transactions') }}