/* @bruin
name: my_schema.table_check
type: duckdb.sql

materialization:
   type: table

depends:
  - my_schema.products

@bruin */

WITH expected_columns AS (
    SELECT 'product_id' AS column_name, 'INTEGER' AS data_type
    UNION ALL SELECT 'product_name', 'VARCHAR'
    UNION ALL SELECT 'price', 'FLOAT'
    UNION ALL SELECT 'stock', 'INTEGER'
),
    actual_columns AS (
        SELECT column_name, upper(data_type) AS data_type
        FROM information_schema.columns
        WHERE table_schema = 'my_schema'
        AND table_name = 'products'
     )
SELECT
    e.column_name,
    a.column_name IS NOT NULL AS exists_in_table,
    e.data_type = a.data_type AS data_type_matches
FROM expected_columns e
LEFT JOIN actual_columns a ON e.column_name = a.column_name;