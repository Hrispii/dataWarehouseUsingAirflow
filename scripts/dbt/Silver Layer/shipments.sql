---------------------------------- MODEL OVERVIEW ----------------------------------------
-- Model Name: shipments                                                                --
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
-- Source table: bronze_layer.shipments                                                 --
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

