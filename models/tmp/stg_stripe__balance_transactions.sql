select
    id as balance_transaction_id,
    created as created_at,
    fee,
    net as net_balance_change,
    type,
    status,
    amount,
    currency,
    exchange_rate
from {{ var('balance_transactions') }}