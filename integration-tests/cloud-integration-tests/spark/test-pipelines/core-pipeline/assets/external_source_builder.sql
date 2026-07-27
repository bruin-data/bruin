/* @bruin
name: local.bruin_test.external_source_builder
type: spark.sql
@bruin */

USE local.bruin_test;
SET spark.sql.shuffle.partitions = 8;
CREATE TABLE IF NOT EXISTS external_source
USING iceberg
AS SELECT 1 AS id, 'managed-elsewhere' AS source_name
