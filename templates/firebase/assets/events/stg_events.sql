/* @bruin

type: bq.sql
description: |
  Staging view over the raw Firebase events_* wildcard table.
  Contains all the parsing/flattening logic (event_params, user_properties, experiments, device, geo, etc.) in one place.

  Referenced by:
    - events.events_json: materialized incremental table, filters by pipeline date range for historical data.
    - events.events: unions events_json (materialized) with this view filtered to data newer than end_date,
      providing near-real-time intraday coverage without re-scanning historical partitions.

materialization:
  type: view

depends:
  - analytics_123456789.parse_version # TODO: Change 123456789 to your analytics ID
  - analytics_123456789.events_intraday # TODO: If you only have daily export, depend on events instead

@bruin */

SELECT
    app_info.id as app,
    platform,
    PARSE_DATE('%Y%m%d', replace(_TABLE_SUFFIX, 'intraday_', '')) as dt,
    TIMESTAMP_MICROS(event_timestamp) as ts,
    TIMESTAMP_MICROS(user_first_touch_timestamp) as user_first_touch_ts,
    user_pseudo_id,
    user_id,
    lower(event_name) as event_name,
    analytics_123456789.parse_version(app_info.version) as app_version, -- TODO: Change 123456789 to your analytics ID
    (
      select
        case when array_length(keys) > 0 then json_object(keys, vals) end
      from (
        select
            array_agg(p.key) as keys,
            array_agg(coalesce(
              p.value.string_value,
              cast(p.value.int_value as string),
              cast(coalesce(p.value.double_value, p.value.float_value) as string)
            )) as vals
          from unnest(event_params) as p
      )
    ) as event_params,
    (
      select case when array_length(keys) > 0 then json_object(keys, vals) end
      from (
          select
            array_agg(p.key) as keys,
            array_agg(coalesce(
              p.value.string_value,
              cast(p.value.int_value as string),
              cast(coalesce(p.value.double_value, p.value.float_value) as string)
            )) as vals
          from unnest(user_properties) as p
          where not starts_with(p.key, '_ltv')
            and not starts_with(p.key, 'firebase_exp')
            and p.key not in ("user_id")
      )
    ) as user_properties,
    (
      select array_agg(struct(
        safe_cast(replace(key, "firebase_exp_", "") as int64) as id,
        safe_cast(value.string_value as int64) as value
      ))
      from unnest(user_properties) where key like 'firebase_exp%'
    ) as experiments,
    to_json(geo) as geo,
    to_json((
      select as struct device.* except(is_limited_ad_tracking, time_zone_offset_seconds),
        CASE lower(device.is_limited_ad_tracking)
          WHEN 'yes' THEN True
          WHEN 'no' THEN False
        END as is_limited_ad_tracking,
        device.time_zone_offset_seconds / 3600 as device_time_zone_offset,
    )) as device,
    to_json(privacy_info) as privacy_info,
    event_server_timestamp_offset / 1000 as event_server_timestamp_offset,
    event_value_in_usd,
from `analytics_123456789.events_*` -- TODO: Change 123456789 to your analytics ID
where replace(_TABLE_SUFFIX, 'intraday_', '') between '20200101' and '21000101'
  and replace(_TABLE_SUFFIX, 'intraday_', '') between '{{ start_date_nodash }}' and '{{ end_date | add_days(1) | date_format("%Y%m%d") }}'
