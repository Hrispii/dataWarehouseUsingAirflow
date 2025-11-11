{{ config(
    schema='silver_layer',
    alias='payments',
    materialized='table'
) }}

SELECT 
    COALESCE(NULLIF(TRIM(payment_id), ''), 'Unknown') AS payment_id,
    COALESCE(NULLIF(TRIM(order_id), ''), 'Unknown') AS order_id,

    COALESCE(
        NULLIF(regexp_replace(TRIM(amount::text), '[^0-9.]', '', 'g'), ''),
        '-1'
    )::float AS amount,

    UPPER(TRIM(currency)) AS currency,

    CASE 
        WHEN payment_date IS NULL THEN '0001-01-01'::date
        WHEN payment_date::date > current_date THEN '0001-01-01'::date
        WHEN payment_date::date < '1900-01-01'::date THEN '0001-01-01'::date
        ELSE payment_date::date
    END AS payment_date
FROM {{ source('bronze_layer', 'payments') }}
WHERE COALESCE(TRIM(amount::text), '') <> '0'
