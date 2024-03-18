/* @bruin

name: fn.get_prop_bool
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION `fn.get_prop_bool`(user_properties ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>>>, param_key STRING) AS 
(
    COALESCE(
        SAFE_CAST(fn.get_prop_int(user_properties, param_key) as bool),
        SAFE_CAST(fn.get_prop_str(user_properties, param_key) as bool)
    )
);