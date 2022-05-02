with commit_comments_customer as (
    select
        id as customer_id,
        url,
        type,
        login as customer_name
    from {{ var('commit_comments_customer') }}
),

commit_comment_reactions_customer as (
    select
        id as customer_id,
        url,
        type,
        login as customer_name
    from {{ var('commit_comment_reactions_customer') }}
),

customers_union as (
    select * from commit_comments_customer
    union all
        select * from commit_comment_reactions_customer
    union all
        select * from issue_comment_reactions_customer
),

customers as (
    select *
    from customers_union
    group by 1,2
)

select * from customers
