/* @bruin

type: bq.sql
description: The users_daily table contains daily user-level metrics and dimensions. The underlying table is partitioned by date and clustered by user_id. This table is used for reporting and ad-hoc analysis.

materialization:
  type: table
  strategy: time_interval
  partition_by: dt
  cluster_by:
    - user_id
  incremental_key: dt
  time_granularity: date

depends:
  - events.events

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
    user_id, --TODO: change to user_pseudo_id if needed
    dt,
    min_by(platform, ts) AS platform, --TODO: Gets the first platform used by the user. Change it, or convert to dimension if needed.

    -- User Attributes
    array_agg(app_version IGNORE NULLS ORDER BY ts limit 1)[safe_offset(0)]
        AS first_app_version,
    array_agg(app_version IGNORE NULLS ORDER BY ts DESC limit 1)[
        safe_offset(0)
    ] AS last_app_version,
    array_agg(geo_country IGNORE NULLS ORDER BY ts limit 1)[safe_offset(0)]
        AS first_country,
    array_agg(geo_country IGNORE NULLS ORDER BY ts DESC limit 1)[
        safe_offset(0)
    ] AS last_country,
    coalesce(
        array_agg(device_brand IGNORE NULLS ORDER BY ts limit 1)[
            safe_offset(0)
        ],
        'unknown'
    ) AS first_device_brand,
    coalesce(
        array_agg(device_brand IGNORE NULLS ORDER BY ts DESC limit 1)[
            safe_offset(0)
        ],
        'unknown'
    ) AS last_device_brand,
    coalesce(
        array_agg(device_model IGNORE NULLS ORDER BY ts limit 1)[
            safe_offset(0)
        ],
        'unknown'
    ) AS first_device_model,
    coalesce(
        array_agg(device_model IGNORE NULLS ORDER BY ts DESC limit 1)[
            safe_offset(0)
        ],
        'unknown'
    ) AS last_device_model,
    coalesce(
        array_agg(device_language IGNORE NULLS ORDER BY ts limit 1)[
            safe_offset(0)
        ],
        'unknown'
    ) AS first_device_language,
    coalesce(
        array_agg(device_language IGNORE NULLS ORDER BY ts DESC limit 1)[
            safe_offset(0)
        ],
        'unknown'
    ) AS last_device_language,
    array_agg(os_version IGNORE NULLS ORDER BY ts limit 1)[safe_offset(0)]
        AS first_os_version,
    array_agg(os_version IGNORE NULLS ORDER BY ts DESC limit 1)[
        safe_offset(0)
    ] AS last_os_version,

    -- Session Attributes
    count(*) AS events,
    min(ts) AS min_ts,
    max(ts) AS max_ts,
    countif(event_name NOT IN ('session_start', 'user_engagement', 'firebase_campaign', 'ad_reward')) > 0 AS engaged, --TODO: ADD MORE EVENTS IF NEEDED BY GAMES. DO NOT REMOVE ANY DEFAULT FIREBASE EVENTS
    countif(event_name = 'session_start') AS session_starts,
    min(session_number) AS min_session_number,
    max(session_number) AS max_session_number,
    count(DISTINCT timestamp_trunc(ts, MINUTE)) AS session_duration, -- Counts distinct minutes in the session, more robust then max(ts) - min(ts).
    array_agg(
        if(event_name NOT IN ('user_engagement'), event_name, null) IGNORE NULLS
        ORDER BY ts limit 1
    )[safe_offset(0)] AS first_event,
    array_agg(
        if(event_name NOT IN ('user_engagement'), event_name, null) IGNORE NULLS
        ORDER BY ts DESC limit 1
    )[safe_offset(0)] AS last_event,

    -- Revenue and Transactions
    countif(event_name = 'ad_impression') AS ad_imp_cnt,
    countif(event_name = 'ad_impression' AND ad_format = 'INTER') AS inters,
    countif(event_name = 'ad_impression' AND ad_format = 'REWARDED')
        AS rewardeds,
    countif(event_name = 'ad_impression' AND ad_format = 'BANNER') AS banners,
    sum(if(event_name = 'ad_impression', value, 0)) AS ad_rev,
    sum(if(event_name = 'ad_impression' AND ad_format = 'INTER', value, 0))
        AS inter_rev,
    sum(if(event_name = 'ad_impression' AND ad_format = 'REWARDED', value, 0))
        AS rewarded_rev,
    sum(if(event_name = 'ad_impression' AND ad_format = 'BANNER', value, 0))
        AS banner_rev,
    countif(event_name = 'in_app_purchase') AS transactions,
    coalesce(sum(event_value_in_usd), 0) AS iap_rev,
    sum(if(event_name = 'ad_impression', value, 0))
    + coalesce(sum(event_value_in_usd), 0) AS total_rev

--TODO: add game specific metrics 

FROM events.events
WHERE
    user_id IS NOT null --TODO: change to user_pseudo_id if needed
    AND event_name NOT IN (
        'app_remove',
        'os_update',
        'app_clear_data',
        'app_update',
        'app_exception'
    )
    AND dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
GROUP BY 1, 2
