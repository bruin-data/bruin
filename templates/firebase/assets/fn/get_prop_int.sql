/* @bruin

name: fn.get_prop_int
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION `fn.get_prop_int`(user_properties ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>, param_key STRING) AS (
(
    SELECT 
        COALESCE(
            value.int_value,
            SAFE_CAST(value.float_value as int64),
            SAFE_CAST(value.double_value as int64),
            SAFE_CAST(value.string_value as int64)
        )
    FROM UNNEST(user_properties) WHERE key = param_key
)
);

