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
