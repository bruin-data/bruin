-- Key uniqueness check.
--
-- Change the table name and the key column below and run this again on any
-- column you are about to trust as a key.
--
-- Worked example: customers, claimed key is customer_id.

SELECT
    customer_id,
    COUNT(*) AS row_count
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY row_count DESC;

-- Every row this returns is a key value that shows up more than once. If
-- this is empty, the key is unique for every row in the table. If it is not
-- empty, row_count tells you how many rows share that one key value.

-- The fix depends on what those duplicate rows look like, so check that
-- next. Compare the row count for the whole table against the row count once
-- exact duplicate rows are collapsed.

SELECT
    COUNT(*)          AS total_rows,
    COUNT(*) - (
        SELECT COUNT(*) FROM (SELECT DISTINCT * FROM customers)
    )                  AS exact_duplicate_rows
FROM customers;

-- If exact_duplicate_rows covers all the duplication you found above, the
-- same row was loaded more than once, and de-duplicating is enough. If it
-- does not, some duplicate keys carry rows that differ from each other,
-- which means the source disagrees with itself about that record, and
-- picking a row (for example, the latest one) is a decision, not a cleanup.
