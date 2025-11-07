/* @bruin
name: test.customers
type: duckdb.sql
materialization:
  type: table
  strategy: ddl
columns:
  - name: customer_id
    type: INTEGER
    primary_key: true
  - name: name
    type: VARCHAR
  - name: email
    type: VARCHAR
  - name: created_at
    type: TIMESTAMP
@bruin */
