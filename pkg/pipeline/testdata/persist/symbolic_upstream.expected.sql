/* @bruin

name: some-sql-task
type: bq.sql

materialization:
  type: table

depends:
  - mode: symbolic
    uri: upstream.id

@bruin */

select *
from foo;
