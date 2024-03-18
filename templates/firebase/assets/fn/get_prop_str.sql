/* @bruin

name: fn.get_prop_str
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION `fn.get_prop_str`(user_properties ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>, param_key STRING) AS (
(
    SELECT 
        COALESCE(
            value.string_value,
            CAST(value.int_value as string),
            CAST(value.float_value as string),
            CAST(value.double_value as string)
        )
    FROM UNNEST(user_properties) WHERE key = param_key
)
);

