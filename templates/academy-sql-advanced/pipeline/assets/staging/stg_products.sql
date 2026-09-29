/* @bruin
name: stg_products
type: duckdb.sql
description: "Canonical product catalogue with whitespace removed from brand values."
depends:
  - products
materialization:
  type: table
  strategy: create+replace
columns:
  - name: product_id
    type: integer
  - name: product_code
    type: varchar
  - name: product_name
    type: varchar
  - name: brand
    type: varchar
  - name: category_name
    type: varchar
  - name: subcategory_name
    type: varchar
  - name: list_price
    type: decimal
@bruin */

SELECT product_id, product_code, product_name, trim(brand) AS brand, category_name,
       subcategory_name, list_price
FROM products
ORDER BY product_id;
