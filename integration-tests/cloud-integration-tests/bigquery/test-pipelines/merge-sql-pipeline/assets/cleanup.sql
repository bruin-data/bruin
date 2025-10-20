/* @bruin
name: dataset.cleanup
type: bq.sql


depends:
  - dataset.target_table

@bruin */

-- Cleaning up environment for upcoming tests

-- Drop tables after this asset succeeds
DROP TABLE IF EXISTS dataset.initial_data;
DROP TABLE IF EXISTS dataset.updated_source;
DROP TABLE IF EXISTS dataset.target_table;