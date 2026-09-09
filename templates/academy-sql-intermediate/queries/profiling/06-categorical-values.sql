-- Categorical value check.
--
-- Change the table name and the text column below and run this again on
-- any column you are about to group by, filter on, or join on.
--
-- Worked example: customers.country.

SELECT
    country,
    COUNT(*) AS row_count
FROM customers
GROUP BY country
ORDER BY row_count DESC;

-- Read this as the list of values the column actually holds. Casing,
-- leading or trailing spaces, and near-duplicate spellings all show up here
-- as separate rows even though a person would read them as the same value.

-- Normalize the value and see whether the distinct count drops.

SELECT
    UPPER(TRIM(country)) AS normalized_country,
    COUNT(*)             AS row_count
FROM customers
GROUP BY UPPER(TRIM(country))
ORDER BY row_count DESC;

-- Compare the two directly.

SELECT
    COUNT(DISTINCT country)                AS distinct_values_raw,
    COUNT(DISTINCT UPPER(TRIM(country)))   AS distinct_values_normalized
FROM customers;

-- If distinct_values_raw is larger than distinct_values_normalized, some of
-- what looked like different values were casing or whitespace variants of
-- the same one. Decide whether to normalize at the source, at load time, or
-- at query time, and apply that decision everywhere the column is used, not
-- only in the query in front of you.
