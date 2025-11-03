/* @bruin

type: duckdb.sql

materialization:
  type: view

@bruin */

-- Simple test to verify decimal return types
SELECT
    2 AS integer_value,
    3 AS another_integer,
    299 AS large_integer,
    123.45 AS decimal_value,
    0.1234 AS small_decimal,
    999999999.999999 AS large_decimal,
    'test string' AS string_value,
    true AS boolean_value,
    null AS null_value
