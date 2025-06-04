/* @bruin
name: country
type: duckdb.sql
materialization:
   type: table

depends:
  - users
columns:
  - name: id
    type: integer
    primary_key: true
    
@bruin */

select id, country from users where id > 10000;