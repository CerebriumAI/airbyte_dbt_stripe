with balance_transaction as (
    select
        id as balance_transaction_id,
        created as created_at,
        fee,
        net as net_change,
        type,
        status,
        amount,
        currency,
        exchange_rate
    from {{ var('balance_transaction') }}
),

charges as (
    select
        id as charges_id
        customer as customer_id,
        receipt_email,
        payment_intent as payment_intent_id,
        created as charges_created_at
    from {{ var('charges')}}
),

payment_intents as (
    select
        *
    from {{ var('payment_intents')}}
),

cards as (
    select
        *
    from {{ var('card')}}
), 

payouts as (
    select
        *
    from {{ var('payouts')}}
), 

customers as (
    select
        *
    from {{ var('customers')}}
),

refunds as (
    select
        *
    from {{ var('refunds')}}
),

balance_transactions as (
    select 
        balance_transaction.*,
        charges.*,
        case (when balance_transaction.type = 'charges') then charges.amount end as customer_facing_amount, 
        case (when balance_transaction.type = 'charges') then charges.currency end as customer_facing_currency,
        customers.description as customer_description
    --     cards.brand as card_brand,
    --     cards.funding as card_funding,
    --     cards.country as card_country,
    --     payouts.payouts_id,
    --     payouts.arrival_date as payouts_expected_arrival_date,
    --     payouts.status as payouts_status,
    --     payouts.type as payouts_type,
    --     payouts.description as payouts_description,
    --     refunds.reason as refunds_reason
    from balance_transaction
    
    -- left join charges 
    --     on charges.balance_transaction_id = balance_transaction.balance_transaction_id
    -- left join customers 
    --     on charges.customer_id = customers.customer_id
    -- left join payment_intents 
    --     on charges.payment_intents_id = payment_intents.payment_intents_id
    -- left join cards 
    --     on charges.card_id = cards.card_id
    -- left join payouts 
    --     on payouts.balance_transaction_id = balance_transaction.balance_transaction_id
    -- left join refunds 
    --     on refunds.balance_transaction_id = balance_transaction.balance_transaction_id
    -- left join charges as refunds_charges 
    --     on refunds.charges_id = refunds_charges.charges_id
)

select * from balance_transactions