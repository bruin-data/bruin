/* @bruin

name: product_price_summary
type: duckdb.sql
materialization:
  type: table

depends:
  - products

columns:
  - name: price_range
    type: VARCHAR
    description: "Range of product prices"
  - name: total_stock
    type: INTEGER
    description: "Total stock available in the price range"
    checks:
      - name: non_negative
  - name: product_count
    type: INTEGER
    description: "Number of products in the price range"
    checks:
      - name: non_negative
@bruin */

WITH price_buckets AS (
    SELECT
        CASE
            WHEN price < 200 THEN 'Below $200'
            WHEN price BETWEEN 200 AND 500 THEN '$200 - $500'
            WHEN price BETWEEN 501 AND 1000 THEN '$501 - $1000'
            ELSE 'Above $1000'
            END AS price_range,
        stock
    FROM products
)

SELECT
    price_range,
    SUM(stock) AS total_stock,
    COUNT(*) AS product_count
FROM price_buckets
GROUP BY price_range
ORDER BY price_range;
