/* @bruin

name: events.events_json
type: bq.sql
description: |
    Flattened events table with JSON fields. 
    This table is partitioned by date and clustered by event_name.
    This table is used for ad-hoc analysis and is not used for reporting.
    Excluded some of the parameters that are already unnested from event_params and user_properties.

materialization:
  type: table
  strategy: delete+insert
  incremental_key: dt

depends:
  - fn.event_params_to_json
  - fn.user_properties_to_json
  - fn.parse_version
  - fn.date_in_range
  - fn.get_param_str
  - fn.get_param_int
  - analytics_123456789.events #TODO: Change 123456789 to your analytics ID

columns:
    - name: app
      type: STRING
      description: The app ID
    - name: platform
      type: STRING
      description: The platform (Android, iOS, Web)
    - name: dt
      type: DATE
      description: The date of the event
    - name: ts
      type: TIMESTAMP
      description: The timestamp of the event
    - name: user_first_touch_ts
      type: TIMESTAMP
      description: The timestamp of the first touch of the user
    - name: user_pseudo_id
      type: STRING
      description: The pseudo ID of the user
    - name: user_id
      type: STRING
      description: The user ID
    - name: event_name
      type: STRING
      description: The name of the event
    - name: app_version
      type: STRING
      description: The version of the app
    - name: ep
      type: STRING
      description: The event parameters as JSON
    - name: up
      type: STRING
      description: The user properties as JSON
    - name: screen
      type: STRING
      description: The name of the screen. Triggered automatically by Firebase SDK.
    - name: previous_screen
      type: STRING
      description: The name of the previous screen. Triggered automatically by Firebase SDK.
    - name: screen_class
      type: STRING
      description: The class of the screen. Triggered automatically by Firebase SDK.
    - name: previous_screen_class
      type: STRING
      description: The class of the previous screen. Triggered automatically by Firebase SDK.
    - name: session_id
      type: INTEGER
      description: The session ID. Triggered automatically by Firebase SDK.
    - name: session_number
      type: INTEGER
      description: The session number. Triggered automatically by Firebase SDK.

@bruin */

SELECT 
    app_info.id as app,
    platform,
    PARSE_DATE('%Y%m%d', event_date) as dt,
    TIMESTAMP_MICROS(event_timestamp) as ts,
    TIMESTAMP_MICROS(user_first_touch_timestamp) as user_first_touch_ts,
    user_pseudo_id,
    user_id,
    lower(event_name) as event_name,
    fn.parse_version(app_info.version) as app_version,

    fn.event_params_to_json(event_params) as ep,
    fn.user_properties_to_json(user_properties) as up,
    (
      select array_agg(struct(
        safe_cast(replace(key, "firebase_exp_", "") as int64) as id, 
        safe_cast(value.string_value as int64) as value
      ))
      from unnest(user_properties) where key like 'firebase_exp%'
    ) as experiments,

    fn.get_param_str(event_params, 'firebase_screen') as screen,
    fn.get_param_str(event_params, 'firebase_previous_screen') as previous_screen,
    fn.get_param_str(event_params, 'firebase_screen_class') as screen_class,
    fn.get_param_str(event_params, 'firebase_previous_class') as previous_screen_class,
    fn.get_param_int(event_params, 'ga_session_id') as session_id,
    fn.get_param_int(event_params, 'ga_session_number') as session_number,
    fn.get_param_int(event_params, 'engaged_session_event') as engaged_session_event,
    fn.get_param_int(event_params, 'engagement_time_msec') / 1000 as engagement_time_sec,
    fn.get_param_str(event_params, 'firebase_event_origin') as event_origin,
    
    device.advertising_id,
    device.vendor_id,
    geo.country as geo_country,
    geo.region as geo_region,
    geo.city as geo_city,
    device.category as device_type,
    device.mobile_brand_name as device_brand,
    device.mobile_marketing_name as device_marketing_name, 
    device.mobile_model_name as device_model,
    device.mobile_os_hardware_model as device_hardware_model,
    device.language as device_language,
    CASE lower(device.is_limited_ad_tracking)
      WHEN 'yes' THEN True
      WHEN 'no' THEN False
    END as device_limited_ad_tracking,
    device.operating_system as device_os,
    device.operating_system_version as os_version,
    event_server_timestamp_offset / 1000 as event_server_timestamp_offset,
    device.time_zone_offset_seconds / 3600 as device_time_zone_offset,
    event_value_in_usd,
    to_json(privacy_info) as privacy_info,
from `analytics_123456789.events_*` --TODO: Change 123456789 to your analytics ID
where fn.date_in_range(_TABLE_SUFFIX, '{{ start_date_nodash }}', '{{ end_date_nodash }}')
