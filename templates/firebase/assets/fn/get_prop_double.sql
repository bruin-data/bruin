/* @bruin

name: fn.get_prop_double
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION `fn.get_prop_double`(user_properties ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>, param_key STRING) AS (
(
    SELECT 
        COALESCE(
            value.double_value,
            value.float_value,
            CAST(value.int_value as FLOAT64),
            SAFE_CAST(value.string_value as FLOAT64)
        )
    FROM UNNEST(user_properties) WHERE key = param_key
)
);