select
    id as customer_id,
    description as customer_description,
    email as customer_email
from {{ var('customers')}}