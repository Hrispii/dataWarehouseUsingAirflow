{{ config(
    schema='silver_layer',
    alias='orders',
    materialized='table'
) }}

SELECT 
    COALESCE(NULLIF(TRIM(order_id), ''), 'Unknown') AS order_id,

    COALESCE(
        NULLIF(regexp_replace(TRIM(customer_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS customer_id,

    CASE 
        WHEN order_date IS NULL THEN '0001-01-01'::date
        WHEN order_date::date > current_date THEN '0001-01-01'::date
        WHEN order_date::date < '1900-01-01'::date THEN '0001-01-01'::date
        ELSE order_date::date
    END AS order_date,

    COALESCE(INITCAP(LOWER(TRIM(status))), 'Unknown') AS status,

    COALESCE(
        NULLIF(regexp_replace(TRIM(total_amount::text), '[^0-9.]', '', 'g'), ''),
        '-1'
    )::float AS total_amount,

    COALESCE(INITCAP(LOWER(TRIM(payment_method))), 'Unknown') AS payment_method

FROM {{ source('bronze_layer', 'orders') }}
