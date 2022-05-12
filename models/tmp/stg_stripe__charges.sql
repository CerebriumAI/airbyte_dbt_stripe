with charges_card as (
    select
        _airbyte_charges_hashid,
        brand as card_brand,
        funding as card_funding,
        country as card_country
    from {{ var('charges_card') }}
)

select
    id as charge_id,
    customer as customer_id,
    receipt_email,
    payment_intent as payment_intent_id,
    {{ dbt_date.from_unixtimestamp('created') }} as created_at,
    status as charge_status,
    amount as charge_amount,
    currency as charge_currency,
    captured as charge_is_captured,
    balance_transaction as balance_transaction_id,
    card_brand,
    card_funding,
    card_country
from {{ var('charges') }}
left join charges_card
    using(_airbyte_charges_hashid)