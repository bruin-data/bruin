/* @bruin
name: rdv.link_customer_order
type: duckdb.sql
depends:
  - stg.customer_orders
materialization:
  type: table
  strategy: datavault_link
columns:
  - name: customer_order_hk
    type: VARCHAR
    primary_key: true
  - name: customer_hk
    type: VARCHAR
  - name: order_hk
    type: VARCHAR
  - name: load_dts
    type: TIMESTAMP
  - name: record_source
    type: VARCHAR
@bruin */

SELECT customer_order_hk, customer_hk, order_hk, load_dts, record_source
FROM stg.customer_orders
