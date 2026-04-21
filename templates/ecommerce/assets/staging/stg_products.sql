/* @bruin
name: staging.stg_products
type: sql
materialization:
  type: table
depends:
  - raw.shopify_products
columns:
  - name: product_id
    type: varchar
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
    id AS product_id,
    title AS product_name,
    product_type AS category,
    vendor,
    status AS product_status,
    CAST(price AS DECIMAL(12,2)) AS price,
    tags,
    created_at,
    updated_at
FROM raw.shopify_products
WHERE status = 'active'
