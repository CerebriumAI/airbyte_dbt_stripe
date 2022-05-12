select
    id as payout_id,
    {{ dbt_date.from_unixtimestamp('arrival_date') }} as arrival_date,
    status,
    type,
    description,
    balance_transaction as balance_transaction_id
from {{ var('payouts')}}