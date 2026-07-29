/* @bruin
name: local.bruin_test.scd2_column_customers
type: spark.sql

materialization:
  type: table
  strategy: scd2_by_column
  incremental_key: updated_at

columns:
  - name: customer_id
    type: INT
    primary_key: true
  - name: customer_name
    type: STRING
  - name: tier
    type: STRING
  - name: updated_at
    type: TIMESTAMP
@bruin */

SELECT 1 AS customer_id, 'Alice' AS customer_name, 'bronze' AS tier, TIMESTAMP '2026-01-01 00:00:00' AS updated_at
UNION ALL
SELECT 2 AS customer_id, 'Bob' AS customer_name, 'silver' AS tier, TIMESTAMP '2026-01-01 00:00:00' AS updated_at
UNION ALL
SELECT 3 AS customer_id, 'Carla' AS customer_name, 'gold' AS tier, TIMESTAMP '2026-01-01 00:00:00' AS updated_at
