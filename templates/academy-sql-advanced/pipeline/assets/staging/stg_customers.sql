/* @bruin
name: stg_customers
type: duckdb.sql
description: "One current customer row per customer id, preserving the source text values."
depends:
  - customers
materialization:
  type: table
  strategy: create+replace
columns:
  - name: customer_id
    type: integer
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
  - name: signed_up_on
    type: date
  - name: segment
    type: varchar
@bruin */

SELECT customer_id, first_name, last_name, city, state, country, signed_up_on, segment
FROM (
    SELECT c.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY city, last_name) AS rn
    FROM customers AS c
) AS ranked
WHERE rn = 1
ORDER BY customer_id;
