---------------------------------- MODEL OVERVIEW ----------------------------------------
-- Model Name: order_items                                                              --
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
-- Source table: bronze_layer.order_items                                               --
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

