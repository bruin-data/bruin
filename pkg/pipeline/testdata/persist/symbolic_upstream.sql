/* @bruin
name: some-sql-task
type: bq.sql

materialization:
  type: table

depends:
  - uri: upstream.id
    mode: symbolic

@bruin */

select *
from foo;
