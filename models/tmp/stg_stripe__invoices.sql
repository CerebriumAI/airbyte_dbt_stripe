select
    id as invoice_id,
    {{ dbt_date.from_unixtimestamp('created') }} as created_at,
    number as invoice_number,
    description,
    paid,
    total,
    subtotal,
    tax,
    amount_due,
    amount_paid,
    amount_remaining,
    {{ dbt_date.from_unixtimestamp('due_date') }} as due_date,
    attempt_count,
    charge as charge_id,
    status,
    customer as customer_id
from {{ var('invoices') }}