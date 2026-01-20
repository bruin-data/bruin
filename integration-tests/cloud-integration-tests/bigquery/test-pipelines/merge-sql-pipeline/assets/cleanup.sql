/* @bruin
name: cloud_integration_test.merge_sql_cleanup
type: bq.sql


depends:
  - cloud_integration_test.merge_sql_target_table

@bruin */

-- Cleaning up environment for upcoming tests

-- Drop tables after this asset succeeds
DROP TABLE IF EXISTS cloud_integration_test.merge_sql_initial_data;
DROP TABLE IF EXISTS cloud_integration_test.merge_sql_updated_source;
DROP TABLE IF EXISTS cloud_integration_test.merge_sql_target_table;