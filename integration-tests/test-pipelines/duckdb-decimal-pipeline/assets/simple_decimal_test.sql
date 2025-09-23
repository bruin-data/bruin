/* @bruin
name: simple_decimal_test
type: duckdb.sql

materialization:
  type: view

@bruin */

-- Simple test to verify decimal return types
SELECT 
    2 as integer_value,
    3 as another_integer,
    299 as large_integer,
    123.45 as decimal_value,
    0.1234 as small_decimal,
    999999999.999999 as large_decimal,
    'test string' as string_value,
    true as boolean_value,
    NULL as null_value
