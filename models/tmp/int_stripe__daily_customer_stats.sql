with customers as (
    select
        {{ dbt_utils.date_trunc("day", 'first_sale_date') }} as date,
        count(*) as new_customers,
        count(
            case when
                is_delinquent = false and total_sales > 0
            then 1 end
        ) as active_new_customers
    from
        {{ ref('stripe__customers') }}
    where
        first_sale_date is not null
    group by
        {{ dbt_utils.date_trunc("day", 'first_sale_date') }}
),

trials as (
    select
        {{ dbt_utils.date_trunc("day", 'created_at') }} as date,
        count(
            case when 
                trial_start is not null
            then 1 end
        ) as trials,
        count(
            case when
                trial_start is not null
                and total_amount_billed > 0
            then 1 end
        ) as trials_converted_on_day
    from
        {{ ref('stripe__subscriptions') }}
    group by
        {{ dbt_utils.date_trunc("day", 'created_at') }}
),

-- consider a trial converted on the trial end date
trial_conversions as (
    select
        {{ dbt_utils.date_trunc("day", 'trial_end') }} as date,
        count(
            case when 
                (trial_end is not null
                and total_amount_billed > 0) 
            then 1 end
        ) as trials_converted
    from
        {{ ref('stripe__subscriptions') }}
    group by
        {{ dbt_utils.date_trunc("day", 'trial_end') }}
),

customers_over_time as (
    select
        daily_overview.date,
        coalesce(customers.new_customers, 0) as new_customers,
        coalesce(customers.active_new_customers, 0) as new_paying_customers,
        sum(coalesce(customers.active_new_customers, 0)) over (order by daily_overview.date rows unbounded preceding) as active_customers,
        coalesce(trials.trials, 0) as trials,
        coalesce(trial_conversions.trials_converted, 0) as trials_converted,
        coalesce(round(cast(trials.trials_converted_on_day as decimal)/nullif(trials.trials, 0), 2) * 100, 0) as trial_conversion_rate
    from
        {{ref('int_stripe__daily_transactions')}} daily_overview
        left join customers
            using(date)
        left join trials
            using(date)
        left join trial_conversions
            using(date)
)

select * from customers_over_time