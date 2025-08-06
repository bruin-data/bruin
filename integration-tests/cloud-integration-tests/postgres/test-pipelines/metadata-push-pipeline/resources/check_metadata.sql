-- This query is used to check the metadata of the sample_data table.

SELECT
  cols.table_schema,
  cols.table_name,
  cols.column_name,
  col_description(c.oid, cols.ordinal_position) AS column_description,
  obj_desc.description AS table_description
FROM information_schema.columns AS cols
JOIN pg_class AS c ON c.relname = cols.table_name
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = cols.table_schema
LEFT JOIN pg_description obj_desc ON obj_desc.objoid = c.oid AND obj_desc.objsubid = 0
WHERE cols.table_schema = 'test_metadata'
  AND cols.table_name = 'sample_data';