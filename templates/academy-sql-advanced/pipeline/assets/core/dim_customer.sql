/* @bruin
name: dim_customer
type: duckdb.sql
description: "Current customer dimension, one row per customer id."
depends:
  - stg_customers
materialization:
  type: table
  strategy: create+replace
columns:
  - name: customer_id
    type: integer
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: first_name
    type: varchar
  - name: last_name
    type: varchar
  - name: city
    type: varchar
  - name: state
    type: varchar
  - name: country
    type: varchar
  - name: segment
    type: varchar
@bruin */

SELECT customer_id, first_name, last_name, city, state, country, segment
FROM stg_customers
ORDER BY customer_id;
