/* @bruin
name: test.location_checks
type: bq.sql
custom_checks:
  - name: row count
    query: |
      SELECT COUNT(*) FROM my_table
  - name: null check
    query: |
      SELECT COUNT(*) FROM my_table WHERE id IS NULL
@bruin */

SELECT 1;
