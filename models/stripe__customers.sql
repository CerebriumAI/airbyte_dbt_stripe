with balance_transactions as (
    select 
        *
    from {{ ref('stripe__balance_transactions') }}  
),

incomplete_charges as (
    select
      created_at,
      customer_id,
      charge_amount
    from {{ ref('stg_stripe__charges')}}
    where not charge_is_captured
),

customers as (
    select
        *
    from {{ ref('stg_stripe__customers') }}  
),

transactions_by_customer as (
    select
        customer_id,
        sum(
            case when 
                type in ('charge', 'payment') 
            then amount
            else 0 
            end
        ) as total_sales,
        sum(
            case when 
                type in ('payment_refund', 'refund') 
            then amount
            else 0 
            end
        ) as total_refunds,    
        sum(amount) as total_gross_transaction_amount,
        sum(fee) as total_fees,
        sum(net_balance_change) as total_net_transaction_amount,
        sum(
            case when 
                type in ('charge', 'payment') 
            then 1
            else 0 
            end
        ) as total_sales_count, 
        sum(
            case when 
                type in ('payment_refund', 'refund') 
            then 1
            else 0 
            end
        ) as total_refund_count,   
        sum(
            case when 
                type in ('charge', 'payment')
                and {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then amount 
            else 0 
            end
        ) as sales_this_month,
        sum(
            case when 
                type in ('payment_refund', 'refund')
                and {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then amount 
            else 0 
            end
        ) as refunds_this_month,
        sum(
            case when 
                {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then amount 
            else 0 
            end
        ) as gross_transaction_amount_this_month,
        sum(
            case when 
                {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then fee 
            else 0 
            end
        ) as fees_this_month,
        sum(
            case when 
                {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then net_balance_change
            else 0 
            end
        ) as net_transaction_amount_this_month,
        sum(
            case when 
                type in ('charge', 'payment')
                and {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then 1 
            else 0 
            end
        ) as sales_count_this_month,
        sum(
            case when 
                type in ('payment_refund', 'refund')
                and {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then 1 
            else 0 
            end
        ) as refund_count_this_month,
        min(
            case when 
                type in ('charge', 'payment') 
            then {{ dbt_utils.date_trunc("day", 'created_at') }}
            else null 
            end
        ) as first_sale_date,
        max(
            case when
                type in ('charge', 'payment') 
            then {{ dbt_utils.date_trunc("day", 'created_at') }}
            else null 
            end
        ) as most_recent_sale_date
    from balance_transactions
    where type in ('payment', 'charge', 'payment_refund', 'refund')
    {{ dbt_utils.group_by(1) }}
),

failed_charges_by_customer as (
    select
        customer_id,
        count(*) as total_failed_charge_count,
        sum(charge_amount) as total_failed_charge_amount,
        sum(
            case when 
                {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then 1
            else 0 
            end
        ) as failed_charge_count_this_month,
        sum(
            case when
                {{ dbt_utils.date_trunc("month", 'created_at') }} = {{ dbt_utils.date_trunc('month', dbt_date.today()) }}
            then charge_amount
            else 0 
            end
        ) as failed_charge_amount_this_month
    from incomplete_charges
    {{ dbt_utils.group_by(1) }}
),

no_customer_transactions_overview as (
    select
        customer_id,
        'No Associated Customer' as customer_description,
        customers.customer_email,
        customers.created_at as customer_created_at,
        customers.is_delinquent,
        coalesce(transactions_by_customer.total_sales/100.0, 0) as total_sales,
        coalesce(transactions_by_customer.total_refunds/100.0, 0) as total_refunds,
        coalesce(transactions_by_customer.total_gross_transaction_amount/100.0, 0) as total_gross_transaction_amount,
        coalesce(transactions_by_customer.total_fees/100.0, 0) as total_fees,
        coalesce(transactions_by_customer.total_net_transaction_amount/100.0, 0) as total_net_transaction_amount,
        coalesce(transactions_by_customer.total_sales_count, 0) as total_sales_count,
        coalesce(transactions_by_customer.total_refund_count, 0) as total_refund_count,    
        coalesce(transactions_by_customer.sales_this_month/100.0, 0) as sales_this_month,
        coalesce(transactions_by_customer.refunds_this_month/100.0, 0) as refunds_this_month,
        coalesce(transactions_by_customer.gross_transaction_amount_this_month/100.0, 0) as gross_transaction_amount_this_month,
        coalesce(transactions_by_customer.fees_this_month/100.0, 0) as fees_this_month,
        coalesce(transactions_by_customer.net_transaction_amount_this_month/100.0, 0) as net_transaction_amount_this_month,
        coalesce(transactions_by_customer.sales_count_this_month, 0) as sales_count_this_month,
        coalesce(transactions_by_customer.refund_count_this_month, 0) as refund_count_this_month,
        transactions_by_customer.first_sale_date,
        transactions_by_customer.most_recent_sale_date,
        0 as total_failed_charge_count,
        0 as total_failed_charge_amount,
        0 as failed_charge_count_this_month,
        0 as failed_charge_amount_this_month,
        customers.customer_currency,
        customers.default_card_id,
        customers.shipping_name,
        customers.shipping_address_line_1,
        customers.shipping_address_line_2,
        customers.shipping_address_city,
        customers.shipping_address_state,
        customers.shipping_address_country,
        customers.shipping_address_postal_code,
        customers.phone as phone_number
    from transactions_by_customer
    left join customers
        using(customer_id)
    where customers.customer_id is null and customers.customer_description is null
),

customer_transactions_overview as (
    select
        customer_id,
        coalesce(customers.customer_description, customers.customer_id) as customer_description,
        customers.customer_email,
        customers.created_at as customer_created_at,
        customers.is_delinquent,
        coalesce(transactions_by_customer.total_sales/100.0, 0) as total_sales,
        coalesce(transactions_by_customer.total_refunds/100.0, 0) as total_refunds,
        coalesce(transactions_by_customer.total_gross_transaction_amount/100.0, 0) as total_gross_transaction_amount,
        coalesce(transactions_by_customer.total_fees/100.0, 0) as total_fees,
        coalesce(transactions_by_customer.total_net_transaction_amount/100.0, 0) as total_net_transaction_amount,
        coalesce(transactions_by_customer.total_sales_count, 0) as total_sales_count,
        coalesce(transactions_by_customer.total_refund_count, 0) as total_refund_count,    
        coalesce(transactions_by_customer.sales_this_month/100.0, 0) as sales_this_month,
        coalesce(transactions_by_customer.refunds_this_month/100.0, 0) as refunds_this_month,
        coalesce(transactions_by_customer.gross_transaction_amount_this_month/100.0, 0) as gross_transaction_amount_this_month,
        coalesce(transactions_by_customer.fees_this_month/100.0, 0) as fees_this_month,
        coalesce(transactions_by_customer.net_transaction_amount_this_month/100.0, 0) as net_transaction_amount_this_month,
        coalesce(transactions_by_customer.sales_count_this_month, 0) as sales_count_this_month,
        coalesce(transactions_by_customer.refund_count_this_month, 0) as refund_count_this_month,
        transactions_by_customer.first_sale_date,
        transactions_by_customer.most_recent_sale_date,
        coalesce(failed_charges_by_customer.total_failed_charge_count, 0) as total_failed_charge_count,
        coalesce(failed_charges_by_customer.total_failed_charge_amount/100, 0) as total_failed_charge_amount,
        coalesce(failed_charges_by_customer.failed_charge_count_this_month, 0) as failed_charge_count_this_month,
        coalesce(failed_charges_by_customer.failed_charge_amount_this_month/100, 0) as failed_charge_amount_this_month,
        customers.customer_currency,
        customers.default_card_id,
        customers.shipping_name,
        customers.shipping_address_line_1,
        customers.shipping_address_line_2,
        customers.shipping_address_city,
        customers.shipping_address_state,
        customers.shipping_address_country,
        customers.shipping_address_postal_code,
        customers.phone as phone_number
    from customers
    left join transactions_by_customer
        using(customer_id)
    left join failed_charges_by_customer 
        using(customer_id)
),

customer_overview as (
    select
        *
    from no_customer_transactions_overview
    union all 
    select
        * 
    from customer_transactions_overview
)

select * from customer_overview