{{ config(
    schema='silver_layer',
    alias='shipments',
    materialized='table'
) }}

SELECT 
    COALESCE(NULLIF(TRIM(shipment_id), ''), 'Unknown') AS shipment_id,
    COALESCE(NULLIF(TRIM(order_id), ''), 'Unknown') AS order_id,
    COALESCE(NULLIF(TRIM(carrier), ''), 'Unknown') AS carrier,
    COALESCE(NULLIF(TRIM(status), ''), 'Unknown') AS status,

    CASE 
        WHEN delivery_date IS NULL THEN '0001-01-01'::date
        WHEN delivery_date::date > current_date THEN '0001-01-01'::date
        WHEN delivery_date::date < '1900-01-01'::date THEN '0001-01-01'::date
        ELSE delivery_date::date
    END AS delivery_date

FROM {{ source('bronze_layer', 'shipments') }}
