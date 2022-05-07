with daily_transactions as (
    select
        *
    from ref('int_stripe__daily_transactions')
),

subscription_payments as (
    select
        *
    from ref('int_stripe__subscription_payments')
),

customer_stats as (
    select
        *
    from ref('int_stripe__daily_customer_stats.sql')
)

sub_stats as (
    select
        {{ dbt_utils.date_trunc("day", 'dt.date') }} as date,
        (
            /*
            Churned subscriptions are counted when the current day is
            less than the day the subscription was canceled.
            */
            select
                count(
                    case when (
                        filtered_subs.status = 'canceled'
                        and dt.date = date_trunc('day', filtered_subs.canceled_at)
                        and filtered_subs.customer_email is not null
                    ) then
                        1
                    end) as "churned_subscriptions"
            from (
                select distinct on (sp.subscription_id)
                    subscription_payments.date,
                    subscription_id,
                    status,
                    customer_email,
                    canceled_at
                from
                    subscription_payments
                where
                    sp.date <= date_trunc('day', dt.date)
                order by
                    subscription_id,
                    date desc
            ) as filtered_subs
        ),
        (
            /*
            New subscriptions are counted on the first issued invoice.
            */
            select
                count(
                    case when (
                        (
                            filtered_subs.status = 'active'
                            or filtered_subs.status = 'past due'
                            or filtered_subs.status = 'canceled'
                        )
                        and filtered_subs.invoice_number = 1
                        and filtered_subs.customer_email is not null
                    ) then
                        1
                    end) as "new_subscriptions"
            from (
                select distinct on (sp.subscription_id)
                    subscription_payments.date,
                    subscription_id,
                    invoice_number,
                    status,
                    customer_email,
                    canceled_at
                from
                    subscription_payments
                where
                    sp.date = date_trunc('day', dt.date)
                order by
                    subscription_id,
                    sp.date desc
            ) as filtered_subs
        ),
        (
            /*
            Active subscriptions are counted when the status is active or past due.
            If the subscription is canceled, the sub will be counted only if the current
            day is less than the day the sub was canceled. This does not apply to subs
            that are 'set to cancel' the day they are canceled.
            */
            select
                count(
                    case when (
                        (
                            filtered_subs.status = 'active'
                            or filtered_subs.status = 'past due'
                            or (
                                filtered_subs.status = 'canceled'
                                and (cancel_at is null or date_trunc('day', filtered_subs.canceled_at) < date_trunc('day', filtered_subs.cancel_at))
                                and dt.date < date_trunc('day', filtered_subs.canceled_at)
                            )
                        )
                        and filtered_subs.customer_email is not null
                    ) then
                        1
                    end) as "active_subscriptions"
            from (
                select distinct on (sp.subscription_id)
                    subscription_payments.date,
                    subscription_id,
                    status,
                    customer_email,
                    canceled_at,
                    cancel_at
                from
                    subscription_payments
                where
                    sp.date <= date_trunc('day', dt.date)
                order by
                    subscription_id,
                    sp.date desc) as filtered_subs
        ),
        /*
        There are multiple different values that can be used when summing mrr. Stripe
        seems to use the average value of the subscription over its entire lifetime.
        */
        (
            select
                coalesce(round(sum(
                    case when (
                        filtered_subs.status = 'canceled'
                        and dt.date = date_trunc('day', filtered_subs.canceled_at)
                        and filtered_subs.customer_email is not null
                    ) then
                        filtered_subs.average_revenue / 100
                    end), 2), 0) as "churned_mrr"
            from (
                select distinct on (sp.subscription_id)
                    subscription_payments.date,
                    subscription_id,
                    average_revenue,
                    status,
                    customer_email,
                    canceled_at
                from
                    subscription_payments
                where
                    sp.date <= date_trunc('day', dt.date)
                order by
                    subscription_id,
                    sp.date desc
            ) as filtered_subs
        ),
        (
            select
                coalesce(round(sum(
                    case when (
                        (
                            filtered_subs.status = 'active'
                            or filtered_subs.status = 'past due'
                            or filtered_subs.status = 'canceled'
                        )
                        and filtered_subs.invoice_number = 1
                        and filtered_subs.customer_email is not null
                    ) then
                        filtered_subs.average_revenue / 100
                    end), 2), 0) as "new_mrr"
            from (
                select distinct on (sp.subscription_id)
                    subscription_payments.date,
                    subscription_id,
                    invoice_number,
                    average_revenue,
                    status,
                    customer_email
                from
                    subscription_payments
                where
                    sp.date = date_trunc('day', dt.date)
                order by
                    subscription_id,
                    date desc
            ) as filtered_subs
        ),
        (
            select
                coalesce(round(sum(
                    case when (
                        (
                            filtered_subs.status = 'active'
                            or filtered_subs.status = 'past due'
                            or (
                                filtered_subs.status = 'canceled'
                                and (cancel_at is null or date_trunc('day', filtered_subs.canceled_at) < date_trunc('day', filtered_subs.cancel_at))
                                and dt.date < date_trunc('day', filtered_subs.canceled_at)
                            )
                        )
                        and filtered_subs.customer_email is not null
                    ) then
                        filtered_subs.average_revenue / 100
                    end), 2), 0) as "mrr"
            from (
                select distinct on (sp.subscription_id)
                    subscription_payments.date,
                    subscription_id,
                    average_revenue,
                    status,
                    customer_email,
                    canceled_at,
                    cancel_at
                from
                    subscription_payments
                where
                    sp.date <= date_trunc('day', dt.date)
                order by
                    subscription_id,
                    date desc
            ) as filtered_subs
        )
    from (
        select
            date
        from
            daily_transactions
        ) as dt
),

daily_overview as (
    select
        *,
        coalesce(round(mrr/nullif(active_subscriptions, 0), 2), 0) as mrr_per_subscription,
        coalesce(round(mrr/nullif(active_customers, 0), 2), 0) as mrr_per_customer,
        coalesce(active_customers - lag(active_customers, 1) over (order by date), 0) as customers_diff,
        coalesce(active_subscriptions - lag(active_subscriptions, 1) over (order by date), 0) as subscriptions_diff,
        coalesce(mrr - lag(mrr, 1) over (order by date), 0) as mrr_diff
    from
        daily_transactions
        left join sub_stats
            using(date)
        left join customer_stats
            using(date)
    order by
        date asc   
)

select * from daily_overview