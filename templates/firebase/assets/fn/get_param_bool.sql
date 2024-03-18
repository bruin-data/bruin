/* @bruin

name: fn.get_param_bool
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION `fn.get_param_bool`(event_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>, param_key STRING) AS (

    COALESCE(
        SAFE_CAST(fn.get_param_int(event_params, param_key) as bool),
        SAFE_CAST(fn.get_param_str(event_params, param_key) as bool)
    )
    
);