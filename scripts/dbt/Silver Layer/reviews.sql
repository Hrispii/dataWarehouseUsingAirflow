---------------------------------- MODEL OVERVIEW ----------------------------------------
-- Model Name: products                                                                 --
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
-- Source table: bronze_layer.products                                                  --
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

