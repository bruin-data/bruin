/* @bruin
name: dim_customer_history
type: duckdb.sql
description: "Stub for the student-built SCD2 customer history; first real run needs full refresh."
depends:
  - customer_snapshots
materialization:
  type: table
  strategy: create+replace
columns:
  - name: customer_id
    type: integer
    primary_key: true
  - name: snapshot_at
    type: date
@bruin */

-- TODO: replace this empty stub with a scd2_by_time projection in lesson 7.
SELECT customer_id, snapshot_at
FROM customer_snapshots
WHERE 1 = 0;
