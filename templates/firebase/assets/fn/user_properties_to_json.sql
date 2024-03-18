/* @bruin
name: fn.user_properties_to_json
type: bq.sql
description: Function to convert user_properties to JSON. It excludes user_id, first_open_time, _ltv_* and firebase_exp_* properties.

@bruin */

CREATE OR REPLACE FUNCTION fn.user_properties_to_json(
    user_properties ARRAY<STRUCT<
        key STRING, 
        value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64, set_timestamp_micros INT64>
    >>
) AS 
((
  with t1 as
  (
      select
        array_agg(p.key) as keys, 
        array_agg(coalesce(p.value.string_value, cast(p.value.int_value as string), cast(coalesce(p.value.double_value, p.value.float_value) as string))) as vals
      from unnest(user_properties) as p 
      where not starts_with(p.key, '_ltv')
        and not starts_with(p.key, 'firebase_exp')
        and p.key not in ("user_id", "first_open_time")
  )
  select 
    case when array_length(keys) > 0 then json_object(keys, vals) end 
  from t1
));
