/* @bruin
name: rdv.hub_customer
type: duckdb.sql
depends:
  - stg.customer_orders
materialization:
  type: table
  strategy: datavault_hub
columns:
  - name: customer_hk
    type: VARCHAR
    primary_key: true
  - name: customer_bk
    type: VARCHAR
  - name: load_dts
    type: TIMESTAMP
  - name: record_source
    type: VARCHAR
@bruin */

SELECT customer_hk, customer_bk, load_dts, record_source
FROM stg.customer_orders
