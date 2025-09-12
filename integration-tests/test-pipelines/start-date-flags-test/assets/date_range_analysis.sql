/* @bruin

name: date_range_analysis
type: duckdb.sql
materialization:
  type: table

depends:
  - date_capture

@bruin */

SELECT 
    captured_start_date,
    captured_end_date
FROM date_capture;