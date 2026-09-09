-- Orphan key check, an anti-join.
--
-- Change the fact table, the foreign key column, and the dimension table
-- and key below and run this again on any relationship you are about to
-- join on.
--
-- Worked example: order_items.product_id against products.product_id.

SELECT COUNT(*) AS orphan_rows
FROM order_items oi
LEFT JOIN products p
  ON p.product_id = oi.product_id
WHERE p.product_id IS NULL;

-- orphan_rows counts fact rows whose foreign key value has no matching row
-- in the dimension. Anything above zero means an inner join on this
-- relationship would silently drop those rows, and a left join would carry
-- NULLs for every dimension column.

-- See which key values are the offenders, and how many rows each one
-- accounts for.

SELECT
    oi.product_id,
    COUNT(*) AS orphan_rows
FROM order_items oi
LEFT JOIN products p
  ON p.product_id = oi.product_id
WHERE p.product_id IS NULL
GROUP BY oi.product_id
ORDER BY orphan_rows DESC;

-- A key value that is NULL here means the fact row never carried a product
-- id at all. A key value that is not NULL and still has no match means the
-- dimension is missing a row it should have, or the fact row points at the
-- wrong table version of that key.
