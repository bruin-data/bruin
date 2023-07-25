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

select *
from foo;
