/* @bruin
name: town.building_access_events
description: |
  Badge presentations at the managed buildings. A refused presentation is still
  recorded, so an expired or withdrawn badge leaves a trail too.
materialization:
  type: table
depends:
  - town.badges
  - town.building_readers
  - _gen.access_scene
columns:
  - name: event_id
    type: varchar
    description: Event identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: reader_id
    type: varchar
    description: Reader the badge was presented to
    checks:
      - name: not_null
  - name: badge_id
    type: varchar
    description: Badge presented
    checks:
      - name: not_null
  - name: ts
    type: timestamp
    description: When the badge was presented
    checks:
      - name: not_null
  - name: result
    type: varchar
    description: granted or refused
@bruin */

WITH badge_readers AS (
    SELECT
        b.badge_id,
        b.citizen_id,
        b.status,
        b.building,
        row_number() OVER (ORDER BY b.badge_id) AS bn,
        -- most badge holders keep to a routine, which is what makes a departure
        -- from one worth a second look
        CASE WHEN sc.part = 'caretaker' THEN 18
             ELSE {{ rnd_int('b.badge_id', 6401, 7, 19) }} END    AS routine_hour,
        sc.part
    FROM town.badges b
    LEFT JOIN _gen.access_scene sc USING (citizen_id)
),
reader_pool AS (
    SELECT building, reader_id, zone,
           row_number() OVER (PARTITION BY building ORDER BY reader_id) AS zn,
           count(*) OVER (PARTITION BY building) AS n_readers
    FROM town.building_readers
),
spine AS (
    SELECT
        br.badge_id,
        br.bn,
        br.building,
        br.status,
        br.routine_hour,
        br.part,
        d.day,
        k.k
    FROM badge_readers br
    CROSS JOIN generate_series(0, {{ var.ping_window_days | int }} - 1) d(day)
    CROSS JOIN generate_series(1, 3) k(k)
    WHERE br.part = 'caretaker'
       OR {{ rnd('br.bn * 100000 + d.day * 10 + k.k', 6410) }} < list_extract([0.62, 0.34, 0.09], k.k)
),
drawn AS (
    SELECT
        s.badge_id,
        rp.reader_id,
        ({{ movement_start() }} + INTERVAL (s.day) DAY
            + INTERVAL (CASE WHEN s.k = 1 THEN s.routine_hour ELSE {{ rnd_int('s.bn * 100000 + s.day * 10 + s.k', 6411, 7, 21) }} END) HOUR
            + INTERVAL (CASE WHEN s.part = 'caretaker' AND s.k = 1 THEN 2
                             ELSE {{ rnd_int('s.bn * 100000 + s.day * 10 + s.k', 6412, 0, 59) }} END) MINUTE)::TIMESTAMP AS ts,
        CASE WHEN s.status = 'active' THEN 'granted' ELSE 'refused' END AS result,
        s.building
    FROM spine s
    JOIN reader_pool rp
      ON rp.building = s.building
     AND rp.zn = 1 + floor({{ rnd('s.bn * 100000 + s.day * 10 + s.k', 6413) }} * rp.n_readers)::BIGINT
),
pinned AS (
    SELECT
        b.badge_id,
        rp.reader_id,
        ({{ rally_date() }}::TIMESTAMP + INTERVAL 18 HOUR + INTERVAL (sc.minute_past_eighteen) MINUTE) AS ts,
        'granted' AS result
    FROM _gen.access_scene sc
    JOIN town.badges b ON b.citizen_id = sc.citizen_id
    JOIN reader_pool rp ON rp.building = 'Loma House' AND rp.zn = sc.zone_n
)
SELECT
    'BA-' || lpad(row_number() OVER (ORDER BY ts, badge_id, reader_id, result)::VARCHAR, 6, '0') AS event_id,
    reader_id,
    badge_id,
    ts,
    result
FROM (
    SELECT badge_id, reader_id, ts, result FROM drawn
    WHERE NOT (building = 'Loma House'
               AND ts >= {{ rally_date() }}::TIMESTAMP + INTERVAL 17 HOUR + INTERVAL 30 MINUTE
               AND ts <  {{ rally_date() }}::TIMESTAMP + INTERVAL 19 HOUR + INTERVAL 30 MINUTE)
    UNION ALL
    SELECT badge_id, reader_id, ts, result FROM pinned
)
