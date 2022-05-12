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

customers as (
    select
        *
    from {{ ref('stg_stripe__customers') }}

),

subscriptions as (
    select
        *
    from {{ ref('stg_stripe__subscriptions') }}

),

plans as (
    select
        *
    from {{ ref('stg_stripe__plans') }}
),

line_items_summary as (
    select 
        -- Invoices
        invoices.invoice_id,
        invoices.invoice_number as invoice_number,
        invoices.created_at as invoice_created_at,
        invoices.status,
        invoices.due_date,
        invoices.amount_due,
        invoices.subtotal,
        invoices.tax,
        invoices.total,
        invoices.amount_paid,
        invoices.amount_remaining,
        invoices.attempt_count,
        invoices.description,
        
        -- Line Items 
        invoice_line_items.invoice_line_item_id,
        invoice_line_items.line_item_description,
        invoice_line_items.line_item_amount,
        invoice_line_items.line_item_quantity,
        invoice_line_items.period_start,
        invoice_line_items.period_end,
        
        -- Charges
        charges.balance_transaction_id,
        charges.charge_amount, 
        charges.charge_status,
        charges.created_at as charge_created_at,
        
        -- Customer Details
        customers.customer_description,
        customers.customer_email,
        customers.customer_id,
        
        -- Subscription Details
        subscriptions.subscription_id,
        subscriptions.subscription_billing,
        subscriptions.start_date as subscription_start_date,
        subscriptions.ended_at as subscription_ended_at,
        plans.plan_id,
        plans.is_plan_active,
        plans.plan_amount,
        plans.plan_interval,
        plans.plan_interval_count,
        plans.plan_nickname,
        plans.plan_product_id
    from invoices
    left join charges
        using(charge_id)
    left join invoice_line_items
        using(invoice_id)
    left join subscriptions
        on invoice_line_items.subscription_id = subscriptions.subscription_id
    left join plans
        on invoice_line_items.plan_id = plans.plan_id
    left join customers
        on customers.customer_id = invoices.customer_id
)

select * from line_items_summary
