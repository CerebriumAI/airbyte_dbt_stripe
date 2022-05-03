select
    id as refund_id,
    charge as charge_id,
    amount as refund_amount,
    reason as refund_reason,
    status as refund_status,
    balance_transaction as balance_transaction_id
from {{ var('refunds')}}