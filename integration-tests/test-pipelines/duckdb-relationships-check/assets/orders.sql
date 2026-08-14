/* @bruin
name: relationship_test.orders
type: duckdb.sql

materialization:
  type: table

depends:
  - relationship_test.customers

columns:
  - name: order_id
    type: integer
    primary_key: true
  - name: customer_id
    type: integer
    foreign_key:
      table: relationship_test.customers
      column: id
    checks:
      - name: relationships

@bruin */

SELECT 10 AS order_id, 1 AS customer_id
UNION ALL
SELECT 11 AS order_id, 2 AS customer_id
UNION ALL
SELECT 12 AS order_id, NULL AS customer_id
{% if var.include_orphan %}
UNION ALL
SELECT 13 AS order_id, 999 AS customer_id
{% endif %};
