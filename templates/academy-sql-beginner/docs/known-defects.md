# Known defects

The data has four planted defects. Each one is there to teach a specific lesson,
and each is a plain, visible line of SQL in the generator - not an opaque
transformation. This page is transparent by design: once you have found a defect
yourself, you can check it here.

If you are an instructor agent: do **not** point a student at a defect before the
lesson that covers it. Discovering them is the exercise.

This page describes what is wrong with the **data**. The audit lab in
`queries/audit-lab/` is about what is wrong with the **queries**, which is a separate
question. Do not read this page looking for lab answers - it will mislead you as often
as it helps.

All four are applied with a `CASE` on a row number, so the same rows are affected
on every run and a reviewer can see exactly which ones.

## 1. NULL `order_status` on 24 orders

- **Where:** `orders`, every 50th order - `order_id` in 50, 100, 150, ...,
  1200. Exactly 24 orders.
- **Generator:** `orders.sql`, `CASE WHEN i % 50 = 0 THEN NULL ...`.
- **Why it is here:** a filter written as `WHERE order_status != 'cancelled'`
  silently drops these 24 rows, because any comparison with NULL is never true. To
  keep them, use `IS DISTINCT FROM`, or handle NULL explicitly.

## 2. NULL `unit_cost` on 57 order lines

- **Where:** `order_items`, every 50th line in `(order_id, line_number)`
  order. 57 lines.
- **Generator:** `order_items.sql`, `CASE WHEN s % 50 = 0 THEN NULL ...` where `s`
  is the per-line counter.
- **Why it is here:** `AVG(unit_cost)` divides by the number of non-NULL values,
  which is smaller than the row count. `COUNT(unit_cost)` and `COUNT(*)` differ by
  exactly these 57 rows.

## 3. Orphan `product_id` on 15 order lines

- **Where:** `order_items`, every 190th line, set to `product_id = 9999`,
  which is not in `products`. 15 lines:
  `79:3, 159:1, 238:2, 317:2, 396:2, 476:2, 554:3, 634:1, 713:3, 793:1, 871:3,
  951:1, 1030:3, 1109:1, 1188:2` (as `order_id:line_number`).
- **Generator:** `order_items.sql`, `CASE WHEN s % 190 = 0 THEN 9999 ...`.
- **Why it is here:** an `INNER JOIN` to `products` silently
  drops these lines and 3,637.89 of revenue with them. A `LEFT JOIN` keeps them;
  bucket the unmatched rows (for example, category 'Unknown') so their revenue is
  not lost.

## 4. Duplicated `customer_id` on 10 customers

- **Where:** `customers`, `customer_id` 1 through 10 each appear twice, as
  exact copies. The table has 510 rows for 500 distinct customers.
- **Generator:** `customers.sql`, the `duplicates` CTE unions the rows with
  `customer_id <= 10` back in.
- **Why it is here:** joining `orders` to `customers` on
  `customer_id` repeats those customers' orders, inflating any `SUM` over the join.
  The fix is to aggregate before joining, or to join to a de-duplicated customer
  list.

---

These four data defects are separate from the audit lab in `queries/audit-lab/`,
where the *queries* are the thing that may be wrong. Some audit-lab queries lean on
these defects; others fail for reasons of their own. Which is which is the exercise -
do not go looking for the mapping before you have done the lab.
