{%- set date_range_query -%}
    select 
        cast({{ dbt_utils.date_trunc("day",'created_at') }} as date) as min_date,
        cast({{ dbt_utils.date_trunc("day", dbt_date.today()) }} as date) as max_date
    from
        {{ ref('stripe__balance_transactions') }}
    order by
        created_at asc
    limit 1
{%- endset -%}
{% set date_range = run_query(date_range_query) %}
-- The dbt parser will cause dbt run to fail as these variables are set dynamically.
-- We avoid this by only calling set "in execution"
{% if execute %}
    {% set min_date = date_range.columns[0][0] %}
    {% set max_date = date_range.columns[1][0] %}
{% endif %}


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

daily_balance_transactions as (
    select
        case 
            when type = 'payout' 
            then {{ dbt_utils.date_trunc("day", 'available_on') }}  
            else {{ dbt_utils.date_trunc("day", 'created_at') }} 
        end as date,
        sum(
            case when 
                type in ('charge', 'payment') 
            then amount 
            else 0 end
        ) as total_sales,
        sum(case when 
                type in ('payment_refund', 'refund') 
            then amount 
            else 0 end
        ) as total_refunds,
        sum(case when 
                type = 'adjustment' 
            then amount 
            else 0 end
        ) as total_adjustments,
        sum(case when 
                type not in ('charge', 'payment', 'payment_refund', 'refund', 'adjustment', 'payout')
                and type not like '%transfer%' 
            then amount 
            else 0 end
        ) as total_other_transactions,
        sum(case when 
                type <> 'payout'
                and type not like '%transfer%' 
            then amount 
            else 0 end
        ) as total_gross_transaction_amount,
        sum(case when
                type <> 'payout'
                and type not like '%transfer%' 
            then net_balance_change
            else 0 end
        ) as total_net_transactions,
        sum(case when
                type = 'payout'
                or type like '%transfer%' 
            then fee * -1.0
            else 0 end
        ) as total_payout_fees,
        sum(case when
                type = 'payout' or type like '%transfer%' 
            then amount 
            else 0 end
        ) as total_gross_payout_amount,
        sum(case when
                type = 'payout' or type like '%transfer%' 
            then fee * -1.0 
            else net_balance_change end
        ) as daily_net_activity,
        sum(case when 
                type in ('payment', 'charge') 
            then 1 
            else 0 end
        ) as total_sales_count,
        sum(case when 
                type = 'payout' 
            then 1 
            else 0 end
        ) as total_payouts_count,
        count(distinct case when 
                type = 'adjustment' 
            then coalesce(source, payout_id) 
            else null end
        ) as total_adjustments_count
    from balance_transactions
    {{ dbt_utils.group_by(1) }}
), 

daily_failed_charges as (
    select
        {{ dbt_utils.date_trunc("day",'created_at') }} as date,
        count(*) as total_failed_charge_count,
        sum(charge_amount) as total_failed_charge_amount
    from incomplete_charges
    {{ dbt_utils.group_by(1) }}
),


date_spine as (
    {{ dbt_date.get_base_dates(start_date=min_date, end_date=max_date) }}
),

daily_transactions as (
    select
        date_spine.date_day as date,
        round(coalesce(daily_balance_transactions.total_sales/100.0, 0), 2) as total_sales,
        round(coalesce(daily_balance_transactions.total_refunds/100.0, 0), 2) as total_refunds,
        round(coalesce(daily_balance_transactions.total_adjustments/100.0, 0), 2) as total_adjustments,
        round(coalesce(daily_balance_transactions.total_other_transactions/100.0, 0), 2) as total_other_transactions,
        round(coalesce(daily_balance_transactions.total_gross_transaction_amount/100.0, 0), 2) as total_gross_transaction_amount,
        round(coalesce(daily_balance_transactions.total_net_transactions/100.0, 0), 2) as total_net_transactions,
        round(coalesce(daily_balance_transactions.total_payout_fees/100.0, 0), 2) as total_payout_fees,
        round(coalesce(daily_balance_transactions.total_gross_payout_amount/100.0, 0), 2) as total_gross_payout_amount,
        round(coalesce(daily_balance_transactions.daily_net_activity/100.0, 0), 2) as daily_net_activity,
        round(coalesce((daily_balance_transactions.daily_net_activity + daily_balance_transactions.total_gross_payout_amount)/100.0, 0), 2) as daily_end_balance,
        coalesce(daily_balance_transactions.total_sales_count, 0) as total_sales_count,
        coalesce(daily_balance_transactions.total_payouts_count, 0) total_payouts_count,
        coalesce(daily_balance_transactions.total_adjustments_count, 0) as total_adjustments_count,
        coalesce(daily_failed_charges.total_failed_charge_count, 0) as total_failed_charge_count,
        round(coalesce(daily_failed_charges.total_failed_charge_amount/100, 0.0), 2) as total_failed_charge_amount
    from daily_balance_transactions
    left join daily_failed_charges 
        using(date)
    right join date_spine
        on daily_balance_transactions.date = date_spine.date_day
)

select * from daily_transactions