/* @bruin
name: example
type: duckdb.sql
materialization:
   type: table

depends:
  - country
  - people

@bruin */


from people a 
join country b on a.id = b.id;