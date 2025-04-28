/* @bruin
name: some-sql-task
type: bq.sql

materialization:
  type: table

depends:
  - uri: upstream.a
    mode: symbolic

  - uri: upstream.b

  - uri: upstream.c
    mode: full

  - asset: upstream.d

  - asset: upstream.e
    mode: symbolic

  - upstream.f

@bruin */

select *
from foo;
