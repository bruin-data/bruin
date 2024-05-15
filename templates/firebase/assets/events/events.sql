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
  lax_string(event_params.firebase_previous_class) as firebase_previous_class,
  lax_string(event_params.firebase_conversion) as firebase_conversion,
  lax_int64(event_params.entrances) as entrances,
  lax_int64(event_params.session_engaged) as session_engaged,
  lax_string(event_params.firebase_previous_screen) as firebase_previous_screen,
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

from `events.events_json` 
