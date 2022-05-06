with subscription_items as (
    select
        subscription_id,
        {{ dbt_utils.date_trunc("day", 'invoice_items.invoice_created_at') }} as created_date,
        -- invoice_number is used to determine for the order in which the invoices for a subscription occur
        -- This is useful when counting sources of new MRR
        count(subscription_id) over (partition by subscription_id order by invoice_created_at asc) as invoice_number
    from
        {{ ref('stripe__invoice_line_items') }} invoice_items
    where
        subscription_id IS NOT NULL
    group by
        invoice_items.subscription_id,
        invoice_items.invoice_created_at
),

subscription_payments as (
    select
        subscription_items.subscription_id,
        subs.average_invoice_amount as average_revenue,
        invoice_number,
        {{ dbt_utils.date_trunc("day", 'subs.created_at') }} as date,
        subs.canceled_at,
        subs.customer_email,
        subs.status
    from
        subscription_items
    left join
        {{ref('stripe__subscriptions')}} subs
            using(subscription_id)
)

select * from subscription_payments