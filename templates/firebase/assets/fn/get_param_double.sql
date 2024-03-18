/* @bruin

name: fn.get_param_double
type: bq.sql

@bruin */

CREATE OR REPLACE FUNCTION fn.get_param_double(event_params ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>>>, param_key STRING) AS 
((
	SELECT
		COALESCE(
			value.float_value,
			value.double_value,
			CAST(value.int_value as FLOAT64),
			SAFE_CAST(value.string_value as FLOAT64)
		)
	FROM UNNEST(event_params) WHERE key = param_key	
));