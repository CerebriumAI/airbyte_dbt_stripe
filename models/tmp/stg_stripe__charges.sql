select
    _airbyte_charges_hashid as charges_hashid,
    id as charge_id,
    customer as customer_id,
    receipt_email,
    payment_intent as payment_intent_id,
    created as created_at,
    amount,
    currency,
    balance_transaction as balance_transaction_id
from {{ var('charges')}}