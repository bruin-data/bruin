-- Drill 2. Target: annotated in 60 seconds.
--
-- Question: for each product category, what was the monthly line revenue in
-- 2024 and how did each month compare with the one before it?
--
-- Annotate each step with what one row of it represents.

WITH t1 AS (
    SELECT
        oi.order_id,
        oi.line_number,
        oi.product_id,
        oi.quantity,
        oi.net_price,
        o.ordered_at
    FROM order_items oi
    JOIN orders o
      ON o.order_id = oi.order_id
    WHERE o.ordered_at >= TIMESTAMP '2024-01-01 00:00:00'
      AND o.ordered_at <  TIMESTAMP '2025-01-01 00:00:00'
),
t2 AS (
    SELECT
        p.category_name,
        strftime(t1.ordered_at, '%Y-%m')        AS ym,
        SUM(t1.quantity * t1.net_price)         AS amt
    FROM t1
    JOIN products p
      ON p.product_id = t1.product_id
    GROUP BY p.category_name, strftime(t1.ordered_at, '%Y-%m')
),
final AS (
    SELECT
        category_name,
        ym,
        amt,
        LAG(amt) OVER (
            PARTITION BY category_name
            ORDER BY ym
        )                                       AS amt_prev,
        amt - LAG(amt) OVER (
            PARTITION BY category_name
            ORDER BY ym
        )                                       AS amt_delta
    FROM t2
)
SELECT *
FROM final
ORDER BY category_name, ym;
