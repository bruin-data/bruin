/* @bruin
name: stg.customer_orders
type: duckdb.sql
materialization:
  type: table
@bruin */

SELECT
  'cust_1_hk' AS customer_hk,
  'C001' AS customer_bk,
  'order_10_hk' AS order_hk,
  'cust1_order10_hk' AS customer_order_hk,
  TIMESTAMP '2024-01-03 00:00:00' AS load_dts,
  'CRM' AS record_source,
  'hash_alice_v2' AS hashdiff,
  'Alice A.' AS customer_name,
  'alice.a@example.com' AS email
UNION ALL
SELECT
  'cust_2_hk' AS customer_hk,
  'C002' AS customer_bk,
  'order_20_hk' AS order_hk,
  'cust2_order20_hk' AS customer_order_hk,
  TIMESTAMP '2024-01-03 00:00:00' AS load_dts,
  'CRM' AS record_source,
  'hash_bob_v1' AS hashdiff,
  'Bob' AS customer_name,
  'bob@example.com' AS email
UNION ALL
SELECT
  'cust_3_hk' AS customer_hk,
  'C003' AS customer_bk,
  'order_30_hk' AS order_hk,
  'cust3_order30_hk' AS customer_order_hk,
  TIMESTAMP '2024-01-03 00:00:00' AS load_dts,
  'ERP' AS record_source,
  'hash_carol_v1' AS hashdiff,
  'Carol' AS customer_name,
  'carol@example.com' AS email
UNION ALL
SELECT
  'cust_1_hk' AS customer_hk,
  'C001' AS customer_bk,
  'order_12_hk' AS order_hk,
  'cust1_order12_hk' AS customer_order_hk,
  TIMESTAMP '2024-01-04 00:00:00' AS load_dts,
  'CRM' AS record_source,
  'hash_alice_v2' AS hashdiff,
  'Alice A.' AS customer_name,
  'alice.a@example.com' AS email
