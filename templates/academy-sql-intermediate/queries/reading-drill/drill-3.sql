-- Drill 3. Target: annotated in 90 seconds.
--
-- Question: for 2024, what was the line revenue by customer country and
-- quarter, and how many orders sat behind each figure?
--
-- Annotate each step with what one row of it represents. One step changes the
-- grain without saying so.

WITH order_scope AS (
    SELECT
        order_id,
        customer_id,
        store_id,
        ordered_at,
        order_total
    FROM orders
    WHERE ordered_at >= TIMESTAMP '2024-01-01 00:00:00'
      AND ordered_at <  TIMESTAMP '2025-01-01 00:00:00'
),
order_lines AS (
    SELECT
        os.order_id,
        os.customer_id,
        os.ordered_at,
        oi.line_number,
        oi.product_id,
        oi.quantity,
        oi.net_price
    FROM order_scope os
    JOIN order_items oi
      ON oi.order_id = os.order_id
),
lines_with_product AS (
    SELECT
        ol.order_id,
        ol.customer_id,
        ol.ordered_at,
        ol.line_number,
        ol.quantity,
        ol.net_price,
        p.category_name,
        p.subcategory_name
    FROM order_lines ol
    JOIN products p
      ON p.product_id = ol.product_id
),
lines_with_customer AS (
    SELECT
        lwp.order_id,
        lwp.ordered_at,
        lwp.line_number,
        lwp.quantity,
        lwp.net_price,
        lwp.category_name,
        c.customer_id,
        c.country,
        c.segment
    FROM lines_with_product lwp
    JOIN customers c
      ON c.customer_id = lwp.customer_id
),
dated AS (
    SELECT
        lwc.country,
        lwc.segment,
        lwc.category_name,
        lwc.order_id,
        lwc.quantity,
        lwc.net_price,
        d.year,
        d.quarter
    FROM lines_with_customer lwc
    JOIN dates d
      ON d.date_day = CAST(lwc.ordered_at AS DATE)
)
SELECT
    country,
    quarter,
    COUNT(DISTINCT order_id)            AS orders,
    SUM(quantity * net_price)           AS line_revenue
FROM dated
GROUP BY country, quarter
ORDER BY line_revenue DESC;
