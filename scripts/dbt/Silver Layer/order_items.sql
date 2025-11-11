{{ config(
    schema='silver_layer',
    alias='order_items',
    materialized='table'
) }}

SELECT 
    COALESCE(NULLIF(TRIM(order_id), ''), 'Unknown') AS order_id,

    COALESCE(
        NULLIF(regexp_replace(TRIM(product_id), '\D', '', 'g'), ''),
        '-1'
    )::int AS product_id,

    COALESCE(NULLIF(quantity, 0), -1) AS quantity,

    COALESCE(
        NULLIF(regexp_replace(TRIM(unit_price), '[^0-9.]', '', 'g'), ''),
        '-1'
    )::float AS unit_price,

    CASE 
        WHEN (
            COALESCE(NULLIF(quantity, 0), -1)
            *
            COALESCE(
                NULLIF(regexp_replace(TRIM(unit_price), '[^0-9.]', '', 'g'), ''),
                '-1'
            )::float
        ) < 0 THEN -1
        ELSE
            COALESCE(NULLIF(quantity, 0), -1)
            *
            COALESCE(
                NULLIF(regexp_replace(TRIM(unit_price), '[^0-9.]', '', 'g'), ''),
                '-1'
            )::float
    END AS total_price

FROM {{ source('bronze_layer', 'order_items') }}
