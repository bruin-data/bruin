-- @bruin.name: some-sql-task
-- @bruin.description: some description goes here
-- @bruin.type: bq.sql
-- @bruin.depends: task1, task2
-- @bruin.depends: task3,task4
-- @bruin.depends: task5, task3
-- @bruin.parameters.param1: first-parameter
-- @bruin.parameters.param2: second-parameter
-- @bruin.parameters.s3_file_path: s3://bucket/path
-- @bruin.connection: conn2
-- @bruin.materialization.type: table
-- @bruin.materialization.partition_by: dt
-- @bruin.materialization.cluster_by: event_name
-- @bruin.materialization.strategy: delete+insert
-- @bruin.materialization.incremental_key: dt
-- @bruin.columns.some_column.primary_key: true
-- @bruin.columns.some_column.type: numeric
-- @bruin.columns.some_column.precision: 10
-- @bruin.columns.some_column.scale: 2
-- @bruin.columns.some_other_column.primary_key: false
-- @bruin.columns.some_other_column.default: 'active'
-- @bruin.columns.some_other_column.collation: en_US
-- @bruin.columns.some_other_column.length: 255

select *
from foo;
