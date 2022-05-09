with invoices as (
    select
        *
    from {{ ref('stg_stripe__invoices') }}  
),

charges as (
    select
        * 
    from {{ ref('stg_stripe__charges') }}  
),

invoice_line_items as (
    select
        * 
    from {{ ref('stg_stripe__invoice_line_items') }}  

),

subscriptions as (
    select
        *
    from {{ ref('stg_stripe__subscriptions') }}  

),

customers as (
    select
        *
    from {{ ref('stg_stripe__customers') }}  

),

line_items_by_invoice as (
    select
        invoices.invoice_id,
        invoices.amount_due,
        invoices.amount_paid,
        invoices.amount_remaining,
        invoices.created_at,
        max(invoice_line_items.subscription_id) as subscription_id,
        sum(invoice_line_items.line_item_amount) as total_item_amount,
        count(distinct invoice_line_items.invoice_line_item_id) as number_line_items
    from invoice_line_items
    left join invoices
        using(invoice_id)
  {{ dbt_utils.group_by(5) }}
),

invoice_stats_by_sub as (
    select
        subscription_id,
        count(distinct invoice_id) as number_invoices_generated,
        sum(amount_due) as total_amount_billed,
        sum(amount_paid) as total_amount_paid,
        sum(amount_remaining) total_amount_remaining,
        max(created_at) as most_recent_invoice_created_at,
        avg(amount_due) as average_invoice_amount,
        avg(total_item_amount) as average_line_item_amount,
        avg(number_line_items) as average_num_invoice_items
    from line_items_by_invoice
  {{ dbt_utils.group_by(1) }}
),

subscription_stats as (
    select
        subscriptions.subscription_id,
        subscriptions.customer_id,
        customers.customer_description,
        customers.customer_email,
        subscriptions.status,
        subscriptions.start_date,
        subscriptions.ended_at,
        subscriptions.subscription_billing,
        subscriptions.billing_cycle_anchor,
        subscriptions.canceled_at,
        subscriptions.created_at,
        subscriptions.current_period_start,
        subscriptions.current_period_end,
        subscriptions.trial_start,
        subscriptions.trial_end,
        subscriptions.days_until_due,
        subscriptions.is_cancel_at_period_end,
        number_invoices_generated,
        total_amount_billed,
        total_amount_paid,
        total_amount_remaining,
        most_recent_invoice_created_at,
        average_invoice_amount,
        average_line_item_amount,
        average_num_invoice_items
    from subscriptions
    left join invoice_stats_by_sub
        using(subscription_id)
    left join customers
        using(customer_id)    
)

select * from subscription_stats


