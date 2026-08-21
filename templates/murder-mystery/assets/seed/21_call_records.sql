/* @bruin
name: town.call_records
description: |
  Ninety days of connected and attempted calls on the Ashmont networks. A
  duration of zero means the call was not answered. cell_id is the site the
  calling handset was registered on when the call began.
materialization:
  type: table
depends:
  - _gen.device_base
  - _gen.device_overrides
  - _gen.call_overrides
columns:
  - name: call_id
    type: varchar
    description: Call identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: caller_msisdn
    type: varchar
    description: Number that placed the call
    checks:
      - name: not_null
  - name: callee_msisdn
    type: varchar
    description: Number that was called
    checks:
      - name: not_null
  - name: started_at
    type: timestamp
    description: When the call began
    checks:
      - name: not_null
  - name: duration_sec
    type: integer
    description: Seconds connected, zero when the call was not answered
  - name: cell_id
    type: varchar
    description: Site the calling handset was on
  - name: direction
    type: varchar
    description: outgoing or incoming, from the calling handset's record
@bruin */

WITH devices AS (
    SELECT
        dev,
        msisdn,
        home_cell,
        citizen_id,
        row_number() OVER (ORDER BY dev) AS idx
    FROM _gen.device_base
),
device_count AS (SELECT count(*) AS n FROM devices),
-- Handsets whose traffic is pinned upstream take no part in the ordinary draw.
pinned_devices AS (
    SELECT b.dev
    FROM _gen.device_overrides o
    JOIN _gen.device_base b USING (device_id)
),
-- Around forty of the counter-sold handsets keep to a handful of numbers. Most
-- of them belong to people who simply do not use a phone much.
sparse AS (
    SELECT dev FROM (
        SELECT dev, row_number() OVER (ORDER BY {{ rnd('dev', 6001) }}, dev) AS rk
        FROM devices
        WHERE citizen_id IS NULL
          AND dev NOT IN (SELECT dev FROM pinned_devices)
    ) WHERE rk <= 40
),
-- A number can only turn up as the far end of someone else's call if it is in
-- ordinary circulation. A handset that keeps to a handful of numbers is not, so
-- its record stays as short as its own calling does — which is the whole point
-- of the pattern being worth noticing.
reachable AS (
    SELECT msisdn, row_number() OVER (ORDER BY dev) AS ridx
    FROM devices
    WHERE dev NOT IN (SELECT dev FROM sparse)
      AND dev NOT IN (SELECT dev FROM pinned_devices)
),
reachable_count AS (SELECT count(*) AS n FROM reachable),
profile AS (
    SELECT
        d.*,
        CASE
            WHEN s.dev IS NOT NULL     THEN {{ rnd_int('d.dev', 6002, 2, 5) }}
            WHEN d.citizen_id IS NULL  THEN {{ rnd_int('d.dev', 6003, 4, 22) }}
            ELSE                            {{ rnd_int('d.dev', 6004, 6, 40) }}
        END AS contact_count,
        CASE
            WHEN s.dev IS NOT NULL THEN 0.05
            ELSE round(0.03 + {{ rnd('d.dev', 6005) }} * 0.45, 3)
        END AS call_rate
    FROM devices d
    LEFT JOIN sparse s USING (dev)
    WHERE d.dev NOT IN (SELECT dev FROM pinned_devices)
),
spine AS (
    SELECT
        p.dev,
        p.msisdn,
        p.home_cell,
        p.contact_count,
        (p.dev IN (SELECT dev FROM sparse)) AS two_way,
        d.day,
        k.k
    FROM profile p
    CROSS JOIN generate_series(0, {{ ledger_days() }} - 1) d(day)
    CROSS JOIN generate_series(1, 3) k(k)
    WHERE {{ rnd('p.dev * 1000000 + d.day * 10 + k.k', 6010) }} < p.call_rate / k.k
),
dialled AS (
    SELECT
        s.msisdn                                                  AS own_msisdn,
        s.two_way,
        (SELECT msisdn FROM reachable WHERE ridx =
            1 + floor({{ rnd("s.dev::VARCHAR || '|' || (1 + floor(" + rnd('s.dev * 1000000 + s.day * 10 + s.k', 6011) + " * s.contact_count)::BIGINT)::VARCHAR", 6012) }} * (SELECT n FROM reachable_count))::BIGINT
        )                                                         AS far_msisdn,
        {{ chance('s.dev * 1000000 + s.day * 10 + s.k', 6018, 0.45) }} AS inbound_leg,
        ({{ ledger_start() }} + INTERVAL (s.day) DAY
            + INTERVAL ({{ rnd_int('s.dev * 1000000 + s.day * 10 + s.k', 6013, 7, 22) }}) HOUR
            + INTERVAL ({{ rnd_int('s.dev * 1000000 + s.day * 10 + s.k', 6014, 0, 59) }}) MINUTE)::TIMESTAMP AS started_at,
        CASE
            WHEN {{ rnd('s.dev * 1000000 + s.day * 10 + s.k', 6015) }} < 0.17 THEN 0
            ELSE 8 + floor(pow({{ rnd('s.dev * 1000000 + s.day * 10 + s.k', 6016) }}, 2.3) * 1100)::INTEGER
        END                                                       AS duration_sec,
        s.home_cell                                               AS cell_id,
        {{ weighted('s.dev * 1000000 + s.day * 10 + s.k', 6017, [['outgoing', 52], ['incoming', 100]]) }} AS direction
    FROM spine s
)
SELECT
    'CL-' || lpad(row_number() OVER (ORDER BY started_at, caller_msisdn, callee_msisdn, duration_sec, cell_id, direction)::VARCHAR, 7, '0') AS call_id,
    caller_msisdn,
    callee_msisdn,
    started_at,
    duration_sec,
    cell_id,
    direction
FROM (
    SELECT
        CASE WHEN two_way AND inbound_leg THEN far_msisdn ELSE own_msisdn END AS caller_msisdn,
        CASE WHEN two_way AND inbound_leg THEN own_msisdn ELSE far_msisdn END AS callee_msisdn,
        started_at,
        duration_sec,
        cell_id,
        direction
    FROM dialled
    WHERE far_msisdn IS NOT NULL AND far_msisdn <> own_msisdn
    UNION ALL
    SELECT caller_msisdn, callee_msisdn, started_at, duration_sec, cell_id, direction FROM _gen.call_overrides
)
