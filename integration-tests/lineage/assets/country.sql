/* @bruin
name: country
type: duckdb.sql
materialization:
   type: table

depends:
  - users
@bruin */

select id, country from users where id > 10000;