{{ config(
    schema='silver_layer',
    alias='inventory',
    materialized='table'
) }}

SELECT 
    COALESCE(NULLIF(regexp_replace(TRIM(product_id::text), '\D', '', 'g'), ''), '-1')::int AS product_id,
    COALESCE(NULLIF(regexp_replace(TRIM(warehouse_id::text), '\D', '', 'g'), ''), '-1')::int AS warehouse_id,
    COALESCE(NULLIF(regexp_replace(TRIM(stock::text), '\D', '', 'g'), ''), '-1')::int AS stock
FROM {{ source('bronze_layer', 'inventory') }}
