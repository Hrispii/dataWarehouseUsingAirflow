{{ config(
    schema='silver_layer',
    alias='reviews',
    materialized='table'
) }}

SELECT 
    COALESCE(
        NULLIF(regexp_replace(TRIM(review_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS review_id,

    COALESCE(
        NULLIF(regexp_replace(TRIM(customer_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS customer_id,

    COALESCE(
        NULLIF(regexp_replace(TRIM(product_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS product_id,

    COALESCE(NULLIF(rating, -1), -1) AS rating,

    COALESCE(
        NULLIF(regexp_replace(TRIM(comment), '[\r\n\t]+', ' ', 'g'), ''),
        'Unknown'
    ) AS comment

FROM {{ source('bronze_layer', 'reviews') }}
