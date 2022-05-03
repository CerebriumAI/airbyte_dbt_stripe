select
    id as customer_id,
    description
from {{ var('customers')}}