/* @bruin

name: events.events
type: bq.sql
materialization:
    type: view
description:
    The events table contains all events and parameters from the Firebase Analytics export.
    The underlying table is partitioned by date and clustered by event_name.
    This table is used for ad-hoc analysis and is not used for reporting.
depends:
  - events.events_json
@bruin */

select
  app,
  platform,
  dt,
  ts,
  user_first_touch_ts,
  user_pseudo_id,
  user_id,
  event_name,
  app_version,
  event_params,
  user_properties,
  if(array_length(experiments) > 0, (select json_object(array_agg(e.id), array_agg(e.value)) from unnest(experiments) as e), JSON "{}") as experiments,
  struct(
    lax_string(device.advertising_id) as advertising_id,
    lax_string(device.browser) as browser,
    lax_string(device.browser_version) as browser_version,
    lax_string(device.category) as category,
    lax_bool(device.is_limited_ad_tracking) as is_limited_ad_tracking,
    lax_string(device.language) as language,
    lax_string(device.mobile_brand_name) as mobile_brand_name,
    lax_string(device.mobile_marketing_name) as mobile_marketing_name,
    lax_string(device.mobile_model_name) as mobile_model_name,
    lax_string(device.mobile_os_hardware_model) as mobile_os_hardware_model,
    lax_string(device.operating_system) as operating_system,
    lax_string(device.operating_system_version) as operating_system_version,
    lax_int64(device.time_zone_offset_seconds) / 3600 as device_time_zone_offset,
    lax_string(device.vendor_id) as vendor_id,
    lax_string(device.web_info) as web_info
  ) as device,
  struct(
    lax_string(geo.city) as city,
    lax_string(geo.continent) as continent,
    lax_string(geo.country) as country,
    lax_string(geo.metro) as metro,
    lax_string(geo.region) as region,
    lax_string(geo.sub_continent) as sub_continent
  ) as geo,
  struct(
    lax_string(privacy_info.ads_storage) as ads_storage,
    lax_string(privacy_info.analytics_storage) as analytics_storage,
    lax_string(privacy_info.uses_transient_token) as uses_transient_token
  ) as privacy_info,
  event_server_timestamp_offset,

  -- FIREBASE
  lax_string(event_params.firebase_screen) as screen,
  lax_string(event_params.firebase_previous_screen) as previous_screen,
  lax_string(event_params.firebase_screen_class) as screen_class,
  lax_string(event_params.firebase_previous_class) as previous_screen_class,
  lax_int64(event_params.ga_session_id) as session_id,
  lax_int64(event_params.ga_session_number) as session_number,
  lax_int64(event_params.engaged_session_event) as engaged_session_event,
  lax_int64(event_params.engagement_time_msec) / 1000 as engagement_time_sec,
  lax_string(event_params.firebase_event_origin) as event_origin,
  lax_string(event_params.firebase_conversion) as firebase_conversion,
  lax_int64(event_params.entrances) as entrances,
  lax_int64(event_params.session_engaged) as session_engaged,
  lax_int64(event_params.previous_first_open_count) as previous_first_open_count,
  lax_int64(event_params.update_with_analytics) as update_with_analytics,
  lax_int64(event_params.system_app) as system_app,
  lax_int64(event_params.system_app_update) as system_app_update,
  lax_string(event_params.source) as source,
  lax_string(event_params.campaign_info_source) as campaign_info_source,
  lax_string(event_params.medium) as medium,
  lax_string(event_params.previous_app_version) as previous_app_version,
  lax_string(event_params.previous_os_version) as previous_os_version,
  lax_string(event_params.firebase_error) as firebase_error,
  lax_string(event_params.fatal) as fatal,
  lax_string(event_params.timestamp) as timestamp,
  lax_string(event_params.error_value) as error_value,
  lax_string(event_params.term) as term,

  --TODO: add other parameters and properties specific to your app

  -- ALTERNATIVE 1:
  -- lax_int64(event_params.level) as level,
  -- lax_string(event_params.result) as result,

  -- ALTERNATIVE 2:
  -- struct(
  --   lax_int64(event_params.level) as level,
  --   lax_string(event_params.result) as result,
  -- ) as progression

from `events.events_json` 
