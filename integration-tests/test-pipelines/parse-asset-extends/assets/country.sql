/* @bruin
name: country
type: duckdb.sql
materialization:
   type: table

extends: 
  - Customer
columns:
  - name: mycol1
  - name: street_name
    extends: Customer.Language
@bruin */

select 1


