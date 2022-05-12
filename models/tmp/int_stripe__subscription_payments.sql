with subscription_invoice_number as (
    select
        subscription_id,
        {{ dbt_utils.date_trunc("day", 'invoice_created_at') }} as created_date,
        -- invoice_number is used to determine for the order in which the invoices for a subscription occur
        -- This is useful when counting sources of new MRR. We use this as invoice number as it is cardinal.
        count(subscription_id) over (partition by subscription_id order by invoice_created_at asc) as invoice_number
    from
        {{ ref('stripe__invoice_line_items') }}
    where
        subscription_id is not null
    group by
        subscription_id,
        invoice_created_at
),

balance_transactions as(
    select
        *
    from
        {{ ref('stripe__balance_transactions') }}
),

subscription_items as (
    select
        sub_items.subscription_id,
        subscription_invoice_number.invoice_number,
        balance_transactions.amount,
        balance_transactions.net_balance_change,
        balance_transactions.exchange_rate,
        sub_items.plan_amount
    from
        {{ ref('stripe__invoice_line_items') }} sub_items
    left join subscription_invoice_number
        on subscription_invoice_number.subscription_id=sub_items.subscription_id
        and created_date={{ dbt_utils.date_trunc("day", 'invoice_created_at') }}
    left join balance_transactions
        using(balance_transaction_id)
    where
        sub_items.subscription_id is not null
),

subscription_payments as (
    select
        subscription_items.subscription_id,
        invoice_number,
        {{ dbt_utils.date_trunc("day", 'subs.created_at') }} as date,
        subs.canceled_at,
        subs.customer_email,
        subs.status,
        subs.average_invoice_amount as average_revenue,
        amount,
        net_balance_change,
        exchange_rate,
        plan_amount
    from
        subscription_items
    left join
        {{ ref('stripe__subscriptions') }} subs
            using(subscription_id)
)

select * from subscription_payments