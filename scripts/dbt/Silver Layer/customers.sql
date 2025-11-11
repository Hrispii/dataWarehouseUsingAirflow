---------------------------------- MODEL OVERVIEW ----------------------------------------
-- Model Name: customers                                                                --
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
-- Source table: bronze_layer.customers                                                 --
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
    alias='customers',
    materialized='table'
) }}

SELECT
    COALESCE(
        NULLIF(regexp_replace(TRIM(customer_id::text), '\D', '', 'g'), ''),
        '-1'
    )::int AS customer_id,

    COALESCE(TRIM(email), 'Unknown') AS email,

    INITCAP(
        TRIM(
            split_part(regexp_replace(TRIM(name), '[^a-zA-Z ]', '', 'g'), ' ', 1) || ' ' ||
            split_part(regexp_replace(TRIM(name), '[^a-zA-Z ]', '', 'g'), ' ', 2)
        )
    ) AS name,

    CASE 
        WHEN signup_date IS NULL THEN '0001-01-01'::date
        WHEN signup_date::date > current_date THEN '0001-01-01'::date
        WHEN signup_date::date < '1900-01-01'::date THEN '0001-01-01'::date
        ELSE signup_date::date
    END AS signup_date,

    CASE 
        WHEN lower(trim(country)) = 'usa' THEN 'USA'
        WHEN lower(trim(country)) = 'uk' THEN 'UK'
        WHEN TRIM(country) IS NULL THEN 'Unknown'
        ELSE INITCAP(LOWER(TRIM(country)))
    END AS country,

    COALESCE(INITCAP(LOWER(TRIM(city))), 'Unknown') AS city,

    COALESCE(TRIM(segment), 'Unknown') AS segment
FROM {{ source('bronze_layer', 'customers') }}

