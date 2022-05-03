select
    id as payout_id,
    arrival_date,
    status,
    type,
    description,
    balance_transaction as balance_transaction_id
from {{ var('payouts')}}