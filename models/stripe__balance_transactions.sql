with balance_transactions as (
    select
        *
    from {{ ref('stg_stripe__balance_transactions') }}
),

charges as (
    select
        * 
    from {{ ref('stg_stripe__charges')}}
),

payouts as (
    select
        *
    from {{ ref('stg_stripe__payouts')}}
), 

customers as (
    select
        *
    from {{ ref('stg_stripe__customers')}}
),

refunds as (
    select
        *
    from {{ ref('stg_stripe__refunds')}}
),

balance_transactions_summary as (
    select
        -- Balance Transactions
        balance_transactions.*,
        
        -- Charges 
        charges.charge_id,
        charges.created_at as charge_created_at,
        charges.card_brand,
        charges.card_funding,
        charges.card_country,
        case when 
            balance_transactions.type = 'charges' 
            then charges.charge_amount end 
        as charge_amount, 
        case when 
            balance_transactions.type = 'charges' 
            then charges.charge_currency end 
        as charge_currency,
       
       -- Customer details
        charges.customer_id,
        customers.customer_description,
        payouts.payout_id,
        payouts.arrival_date as payout_arrival_date,
        payouts.status as payout_status,
        payouts.type as payout_type,
        payouts.description as payout_description,
        refunds.refund_reason
    from balance_transactions
    left join charges 
        using(balance_transaction_id)
    left join customers 
        using(customer_id)
    left join payouts 
        using(balance_transaction_id)
    left join refunds 
        using(balance_transaction_id)
)

select * from balance_transactions_summary