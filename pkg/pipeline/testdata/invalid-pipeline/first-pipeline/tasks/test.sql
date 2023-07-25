-- @bruin.name: some-sql-task
-- @bruin.description: some description goes here
-- @bruin.type: bq.sql
-- @bruin.depends: task1, task2
-- @bruin.depends: task3,task4
-- @bruin.depends: task5, task3
-- @bruin.parameters.param1: first-parameter
-- @bruin.parameters.param2: second-parameter
-- @bruin.connections.conn1: first-connection
-- @bruin.connections.conn2: second-connection

select *
from foo;
