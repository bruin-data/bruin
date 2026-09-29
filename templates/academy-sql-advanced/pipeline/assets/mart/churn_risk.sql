/* @bruin
name: churn_risk
type: duckdb.sql
description: "One row per customer whose last observed order is older than the churn threshold."
depends:
  - dim_customer
  - stg_orders
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
  - name: last_order_at
    type: timestamp
  - name: reason
    type: varchar
    checks:
      - name: not_null
custom_checks:
  - name: churn share is plausible
    description: "A missing recent status must not make nearly every customer look churned."
    query: |
      SELECT CASE WHEN COUNT(*) > 0 AND
                         SUM(CASE WHEN reason = 'churned' THEN 1 ELSE 0 END) > COUNT(*) * 0.40
                       THEN 1 ELSE 0 END
      FROM churn_risk
    value: 0
@bruin */

-- Optional MotherDuck path. Uncomment the platform-specific partition and cluster
-- configuration when running the cloud cost exercise.
-- materialization:
--   partition_by: [last_order_at]
--   cluster_by: [customer_id]

WITH last_orders AS (
    SELECT customer_id, MAX(ordered_at) AS last_order_at
    FROM stg_orders
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    l.last_order_at,
    'churned' AS reason
FROM dim_customer AS c
LEFT JOIN last_orders AS l ON c.customer_id = l.customer_id
WHERE l.last_order_at IS NULL
   OR l.last_order_at < (SELECT MAX(ordered_at) FROM stg_orders) - INTERVAL '{{ var.churn_days }}' DAY
ORDER BY c.customer_id;
