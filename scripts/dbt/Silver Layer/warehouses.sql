{{ config(
    schema='silver_layer',
    alias='warehouses',
    materialized='table'
) }}

SELECT 
    COALESCE(
        NULLIF(regexp_replace(TRIM(warehouse_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS warehouse_id,

    INITCAP(TRIM(name)) AS name,
    INITCAP(TRIM(location)) AS location

FROM {{ source('bronze_layer', 'warehouses') }}
