-- models/gold_layer/dim_customers.sql

select
    customer_id,
    email,
    name,
    signup_date,
    country,
    city,
    segment
from {{ ref('customers') }}
