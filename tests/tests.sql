# ✅ Data Quality Tests for PostgreSQL
# These SQL queries validate consistency, completeness, structure,
# referential integrity, duplication, and general data hygiene
# across all layers of the warehouse.

--------------------------------------------------------------------------------
-- 1. TEST FOR NULLS IN PRIMARY KEYS
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE {primary_key} IS NULL;

--------------------------------------------------------------------------------
-- 2. TEST FOR DUPLICATE PRIMARY KEYS
--------------------------------------------------------------------------------
SELECT {primary_key}, COUNT(*)
FROM {schema}.{table}
GROUP BY {primary_key}
HAVING COUNT(*) > 1;

--------------------------------------------------------------------------------
-- 3. TEST FOR NEGATIVE NUMERIC VALUES (useful for prices, stock, quantity)
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE {numeric_column} < 0;

--------------------------------------------------------------------------------
-- 4. TEST FOR INVALID DATES (future dates, too old dates)
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE {date_column} > CURRENT_DATE
   OR {date_column} < '1900-01-01';

--------------------------------------------------------------------------------
-- 5. TEST FOR REFERENTIAL INTEGRITY (FK: parent value does not exist)
--------------------------------------------------------------------------------
SELECT child.{child_fk}
FROM {child_schema}.{child_table} child
LEFT JOIN {parent_schema}.{parent_table} parent
    ON child.{child_fk} = parent.{parent_pk}
WHERE parent.{parent_pk} IS NULL;

--------------------------------------------------------------------------------
-- 6. TEST FOR EMPTY STRINGS IN TEXT FIELDS
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE {text_column} = '';

--------------------------------------------------------------------------------
-- 7. TEST FOR NON-ALPHANUMERIC CHARACTERS (useful for IDs coming from raw files)
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE {column} !~ '^[A-Za-z0-9\-_ ]+$';

--------------------------------------------------------------------------------
-- 8. TEST FOR UNEXPECTED ENUM VALUES (method, status, category, etc.)
--------------------------------------------------------------------------------
SELECT DISTINCT {column}
FROM {schema}.{table}
WHERE {column} NOT IN ({expected_values});

-- Example:
-- WHERE payment_method NOT IN ('card', 'cash', 'paypal')

--------------------------------------------------------------------------------
-- 9. TEST FOR OUTLIERS USING Z-SCORE (useful in Gold Layer)
--------------------------------------------------------------------------------
SELECT *
FROM (
    SELECT *,
           (value - AVG(value) OVER()) /
           NULLIF(STDDEV(value) OVER(), 0) AS z_score
    FROM {schema}.{table}
) x
WHERE ABS(z_score) > 3;

--------------------------------------------------------------------------------
-- 10. TEST FOR NON-MATCHING DATA TYPES (check implicit casts)
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE {column} ~ '[^0-9]'   -- column should be integer
;

--------------------------------------------------------------------------------
-- 11. TEST FOR NULL RATIOS (helps detect bad-quality columns)
--------------------------------------------------------------------------------
SELECT
    '{column}' AS column_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(
        SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END)::numeric
        / COUNT(*)::numeric * 100, 2
    ) AS null_percentage
FROM {schema}.{table};

--------------------------------------------------------------------------------
-- 12. TEST FOR DUPLICATE ROWS ACROSS ALL COLUMNS
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
GROUP BY *
HAVING COUNT(*) > 1;

--------------------------------------------------------------------------------
-- 13. TEST FOR INCORRECT CURRENCY FORMAT
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE currency NOT IN ('USD', 'EUR', 'CAD', 'GBP');

--------------------------------------------------------------------------------
-- 14. TEST FOR UNUSUAL STRING LENGTHS (truncated or corrupted text)
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE LENGTH({column}) > 5000
   OR LENGTH({column}) = 0;

--------------------------------------------------------------------------------
-- 15. TEST FOR MISMATCHED PRICES (unit_price * quantity != total_price)
--------------------------------------------------------------------------------
SELECT *
FROM {schema}.{table}
WHERE total_price != unit_price * quantity;
