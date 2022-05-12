with customer_addresses as (
    select
        _airbyte_customers_hashid,
        line1,
        line2,
        city,
        state,
        country,
        postal_code
    from {{ var('customers_address') }}
)

select
    id as customer_id,
    {{ dbt_date.from_unixtimestamp('created') }} as created_at,
    description as customer_description,
    email as customer_email,
    delinquent as is_delinquent,
    currency as customer_currency,
    default_card as default_card_id,
    shipping as shipping_name,
    line1 as shipping_address_line_1,
    line2 as shipping_address_line_2,
    city as shipping_address_city,
    state as shipping_address_state,
    country as shipping_address_country,
    postal_code as shipping_address_postal_code,
    phone
from {{ var('customers')}}
left join customer_addresses
    using(_airbyte_customers_hashid)