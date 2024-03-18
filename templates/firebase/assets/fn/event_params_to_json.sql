/* @bruin 

name: fn.event_params_to_json
type: bq.sql
description: Function to convert event_params to JSON

@bruin */

CREATE OR REPLACE FUNCTION `fn.event_params_to_json`(
    event_params ARRAY<STRUCT<
        key STRING, 
        value STRUCT<string_value STRING, int_value INT64, float_value FLOAT64, double_value FLOAT64>
    >>
) 
AS ((
  with t1 as
  (
      select
        array_agg(p.key) as keys, 
        array_agg(coalesce(
          p.value.string_value, 
          cast(p.value.int_value as string), 
          cast(coalesce(p.value.double_value, p.value.float_value) as string)
        )) as vals
      from unnest(event_params) as p 
      where key not in ("firebase_screen","firebase_screen_class","ga_session_id","ga_session_number","engaged_session_event","engagement_time_msec", "firebase_event_origin", "firebase_previous_id", "firebase_screen_id")
  )
  select 
    case when array_length(keys) > 0 then json_object(keys, vals) end 
  from t1
));
