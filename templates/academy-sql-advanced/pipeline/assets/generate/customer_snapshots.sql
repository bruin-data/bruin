/* @bruin
name: customer_snapshots
type: duckdb.sql
description: "Monthly source snapshots for the 30 customers whose attributes change."
depends:
  - customers
materialization:
  type: table
  strategy: create+replace
columns:
  - name: customer_id
    type: integer
  - name: snapshot_at
    type: date
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
  - name: valid_from
    type: date
  - name: valid_until
    type: date
@bruin */

WITH months AS (
    SELECT CAST(i AS INTEGER) AS month_number,
           (DATE '2023-01-01' + i * INTERVAL '1' MONTH)::DATE AS snapshot_at
    FROM range(0, 36) AS t(i)
), changed AS (
    SELECT CAST(i AS INTEGER) AS customer_id
    FROM range(1, 31) AS t(i)
)
SELECT
    c.customer_id,
    m.snapshot_at,
    'Snapshot' || c.customer_id AS first_name,
    'Customer' || c.customer_id AS last_name,
    CASE WHEN c.customer_id % 3 = 0 AND m.month_number >= 18 THEN 'Berlin'
         WHEN c.customer_id % 2 = 0 AND m.month_number >= 12 THEN 'London'
         ELSE 'New York' END AS city,
    CASE WHEN c.customer_id % 3 = 0 AND m.month_number >= 18 THEN 'Berlin'
         WHEN c.customer_id % 2 = 0 AND m.month_number >= 12 THEN 'England'
         ELSE 'New York' END AS state,
    'USA' AS country,
    CASE WHEN m.month_number >= 24 AND c.customer_id % 4 = 0 THEN 'enterprise'
         WHEN c.customer_id % 2 = 0 AND m.month_number >= 12 THEN 'small_business'
         ELSE 'consumer' END AS segment,
    m.snapshot_at AS valid_from,
    CASE
        WHEN c.customer_id BETWEEN 1 AND 5 THEN m.snapshot_at + INTERVAL '40' DAY
        WHEN c.customer_id BETWEEN 6 AND 10 THEN m.snapshot_at + INTERVAL '20' DAY
        ELSE (m.snapshot_at + INTERVAL '1' MONTH)::DATE
    END::DATE AS valid_until
FROM changed AS c
CROSS JOIN months AS m
ORDER BY c.customer_id, m.snapshot_at;
