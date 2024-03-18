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

select * except(up, ep),

  -- FIREBASE
  lax_string(ep.firebase_previous_class) as firebase_previous_class,
  lax_string(ep.firebase_conversion) as firebase_conversion,
  lax_int64(ep.entrances) as entrances,
  lax_int64(ep.session_engaged) as session_engaged,
  lax_string(ep.firebase_previous_screen) as firebase_previous_screen,
  lax_int64(ep.previous_first_open_count) as previous_first_open_count,
  lax_int64(ep.update_with_analytics) as update_with_analytics,
  lax_int64(ep.system_app) as system_app,
  lax_int64(ep.system_app_update) as system_app_update,
  lax_string(ep.source) as source,
  lax_string(ep.campaign_info_source) as campaign_info_source,
  lax_string(ep.medium) as medium,
  lax_string(ep.previous_app_version) as previous_app_version,
  lax_string(ep.previous_os_version) as previous_os_version,
  lax_string(ep.firebase_error) as firebase_error,
  lax_string(ep.fatal) as fatal,
  lax_string(ep.timestamp) as timestamp,
  lax_string(ep.error_value) as error_value,
  lax_string(ep.term) as term,

  --TODO: add other parameters and properties specific to your app

from `events.events_json` 
