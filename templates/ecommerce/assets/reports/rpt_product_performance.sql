/* @bruin
name: reports.rpt_product_performance
type: sql
materialization:
  type: table
depends:
  - staging.stg_products
columns:
  - name: product_id
    type: varchar
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
    product_id,
    product_name,
    category,
    vendor,
    price,
    product_status,
    created_at,
    updated_at
FROM staging.stg_products
ORDER BY product_name
