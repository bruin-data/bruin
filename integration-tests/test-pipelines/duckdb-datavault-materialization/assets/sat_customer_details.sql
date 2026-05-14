/* @bruin
name: rdv.sat_customer_details
type: duckdb.sql
depends:
  - stg.customer_orders
materialization:
  type: table
  strategy: datavault_satellite
columns:
  - name: customer_hk
    type: VARCHAR
    primary_key: true
  - name: hashdiff
    type: VARCHAR
  - name: load_dts
    type: TIMESTAMP
  - name: record_source
    type: VARCHAR
  - name: customer_name
    type: VARCHAR
  - name: email
    type: VARCHAR
@bruin */

SELECT customer_hk, hashdiff, load_dts, record_source, customer_name, email
FROM stg.customer_orders
