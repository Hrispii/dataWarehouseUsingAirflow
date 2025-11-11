{{ config(
    schema='silver_layer',
    alias='products',
    materialized='table'
) }}

SELECT 
    COALESCE(
        NULLIF(regexp_replace(TRIM(product_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS product_id,

    INITCAP(
        TRIM(
            split_part(regexp_replace(TRIM(name), '[^a-zA-Z ]', '', 'g'), ' ', 1) || ' ' ||
            split_part(regexp_replace(TRIM(name), '[^a-zA-Z ]', '', 'g'), ' ', 2)
        )
    ) AS name,

    COALESCE(INITCAP(LOWER(TRIM(category))), 'Unknown') AS category,
    COALESCE(INITCAP(LOWER(TRIM(brand))), 'Unknown') AS brand,

    COALESCE(
        NULLIF(regexp_replace(TRIM(price::text), '[^0-9.]', '', 'g'), ''),
        '-1'
    )::float AS price,

    UPPER(TRIM(currency)) AS currency
FROM {{ source('bronze_layer', 'products') }}
WHERE COALESCE(
        NULLIF(regexp_replace(TRIM(price::text), '[^0-9.]', '', 'g'), ''),
        '0'
      )::float != 0
