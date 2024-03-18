/* @bruin

name: user_model.users_daily
type: bq.sql
materialization:
    type: table
    strategy: delete+insert
    incremental_key: dt
    partition_by: dt
    cluster_by: 
      - user_id

depends:
  - events.events

description:
    The users_daily table contains daily user-level metrics and dimensions.
    The underlying table is partitioned by date and clustered by user_id.
    This table is used for reporting and ad-hoc analysis.

columns:
  - name: user_id
    type: STRING
    description: The user ID
    primary_key: true
  - name: dt
    type: DATE
    description: The date of the event
    primary_key: true
  - name: platform
    type: STRING
    description: The platform (Android, iOS, Web). Gets the first platform used by the user in the day.

@bruin */

SELECT
  user_id,
  dt,
  min_by(platform, ts) as platform, --TODO: Gets the first platform used by the user. Change it, or convert to dimension if needed.

  -- User Attributes
  array_agg(app_version ignore nulls order by ts limit 1)[safe_offset(0)] as first_app_version,
  array_agg(app_version ignore nulls order by ts desc limit 1)[safe_offset(0)] as last_app_version,
  array_agg(geo_country ignore nulls order by ts limit 1)[safe_offset(0)] as first_country,
  array_agg(geo_country ignore nulls order by ts desc limit 1)[safe_offset(0)] as last_country,
  coalesce(array_agg(device_brand ignore nulls order by ts limit 1)[safe_offset(0)], 'unknown') as first_device_brand,
  coalesce(array_agg(device_brand ignore nulls order by ts desc limit 1)[safe_offset(0)], 'unknown') as last_device_brand,
  coalesce(array_agg(device_model ignore nulls order by ts limit 1)[safe_offset(0)], 'unknown') as first_device_model,
  coalesce(array_agg(device_model ignore nulls order by ts desc limit 1)[safe_offset(0)], 'unknown') as last_device_model,
  coalesce(array_agg(device_language ignore nulls order by ts limit 1)[safe_offset(0)], 'unknown') as first_device_language,
  coalesce(array_agg(device_language ignore nulls order by ts desc limit 1)[safe_offset(0)], 'unknown') as last_device_language,
  array_agg(os_version ignore nulls order by ts limit 1)[safe_offset(0)] as first_os_version,
  array_agg(os_version ignore nulls order by ts desc limit 1)[safe_offset(0)] as last_os_version,

  -- Session Attributes
  count(*) as events,
  min(ts) as min_ts,
  max(ts) as max_ts,
  countif(event_name not in ("session_start", "user_engagement", "firebase_campaign", "ad_reward")) > 0 as engaged, --TODO: ADD MORE EVENTS IF NEEDED BY GAMES. DO NOT REMOVE ANY DEFAULT FIREBASE EVENTS
  countif(event_name = "session_start") as session_starts,
  min(session_number) as min_session_number,
  max(session_number) as max_session_number,
  count(distinct timestamp_trunc(ts, minute)) as session_duration, -- Counts distinct minutes in the session, more robust then max(ts) - min(ts).
  array_agg(if(event_name not in ("user_engagement"), event_name, null) ignore nulls order by ts limit 1)[safe_offset(0)] as first_event,
  array_agg(if(event_name not in ("user_engagement"), event_name, null) ignore nulls order by ts desc limit 1)[safe_offset(0)] as last_event,

  -- Revenue and Transactions
  countif(event_name="ad_impression") as ad_imp_cnt,
  countif(event_name="ad_impression" and ad_format="INTER") as inters,
  countif(event_name="ad_impression" and ad_format="REWARDED") as rewardeds,
  countif(event_name="ad_impression" and ad_format="BANNER") as banners,
  sum(if(event_name="ad_impression", value, 0)) as ad_rev,
  sum(if(event_name="ad_impression" and ad_format="INTER", value, 0)) as inter_rev,
  sum(if(event_name="ad_impression" and ad_format="REWARDED", value, 0)) as rewarded_rev,
  sum(if(event_name="ad_impression" and ad_format="BANNER", value, 0)) as banner_rev,
  countif(event_name="in_app_purchase") as transactions,
  coalesce(sum(event_value_in_usd), 0) as iap_rev,
  sum(if(event_name="ad_impression", value, 0)) + coalesce(sum(event_value_in_usd), 0) as total_rev,

  --TODO: add game specific metrics 

from events.events
where user_id is not null
  and event_name not in ("app_remove", "os_update", "app_clear_data", "app_update", "app_exception")
  and dt between '{{ start_date }}' and '{{ end_date }}'
group by 1,2
