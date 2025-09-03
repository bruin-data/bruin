/* @bruin
name: country
type: duckdb.sql
start_date: 2024-01-01
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


