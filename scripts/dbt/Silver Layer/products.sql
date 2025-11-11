---------------------------------- MODEL OVERVIEW ----------------------------------------
-- Model Name: products                                                                --
-- Layer: Silver Layer                                                                  --
------------------------------------------------------------------------------------------
--                              Model Purpose                                           --
-- This model cleans, validates, and standardizes the raw dataset coming from the       --
-- Bronze Layer. It removes formatting issues, fixes corrupted values, enforces proper  --
-- data types, and applies business rules that prepare the data for analytical use.     --
-- The output of this model is used directly by the Gold Layer and downstream ML        --
-- pipelines.                                                                           --
------------------------------------------------------------------------------------------
--                                                                                      --
--                           Key Transformations                                        --
-- • Normalizes identifiers (removes non-numeric symbols, trims whitespace)             --
-- • Cleans text fields (lowercasing, trimming, invalid character removal)              --
-- • Replaces nulls with default values                                                 --
-- • Validates and sanitizes dates                                                      --
-- • Standardizes categorical fields                                                    --
-- • Ensures type safety and consistent naming                                          --
------------------------------------------------------------------------------------------
--                                                                                      --
--                               Data Sources                                           --
-- Source table: bronze_layer.products                                                 --
-- The model reads raw, unmodified JSON-ingested data from the Bronze Layer.            --
-----------------------------------------------------------------------------------------
--                                                                                      --
--                          Output Characteristics                                      --
-- Materialization: table                                                               --
-- Schema: silver_layer                                                                 --
-- Purpose: Serve as cleaned, reliable, business-friendly data for downstream models    --
--          and Gold Layer marts.                                                       --
------------------------------------------------------------------------------------------
--                                                                                      --
--                              Notes for Developers                                    --
-- • Only apply transformations that enhance data quality                               --
-- • Avoid business logic; keep it clean-layer-focused                                  --
-- • Ensure that all identifiers, dates, and categories are validated                   --
-- • Gold Layer will rely on this model for stable joins and metrics                    --
------------------------------------------------------------------------------------------

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

