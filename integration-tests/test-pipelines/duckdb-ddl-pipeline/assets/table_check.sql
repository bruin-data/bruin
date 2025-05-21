/* @bruin
name: my_schema.table_check
type: duckdb.sql

materialization:
   type: table

depends:
  - my_schema.products

custom_checks:
   - name:  row_check
     value: 1
     query: SELECT COUNT(column_name) = 4 FROM my_schema.table_check
   - name:  no_null_names
     value: 0
     query: SELECT COUNT(CASE WHEN column_name IS NULL THEN 1 END) AS null_count
            FROM my_schema.table_check;
   - name:  no_null_types
     value: 0
     query: SELECT COUNT(CASE WHEN column_name IS NULL THEN 1 END) AS null_count
            FROM my_schema.table_check;
   - name: name_check
     value: 1
     query: SELECT SUM(CASE WHEN column_name IN ('product_id', 'product_name', 'price', 'stock') THEN 1 END) = 4
            FROM my_schema.table_check;
   - name: type_check
     value: 1
     query: SELECT SUM(CASE WHEN column_type IN ('INTEGER', 'VARCHAR', 'FLOAT') THEN 1 END) = 4
            FROM my_schema.table_check;
   - name: primary_key_check
     value: 1
     query: SELECT SUM(CASE WHEN column_name IN ('product_id', 'product_name') THEN 1 END) = 2
                FROM my_schema.table_check;

@bruin */

SELECT * FROM (DESCRIBE MY_SCHEMA.products);