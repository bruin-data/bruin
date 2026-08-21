/* @bruin
name: town.device_pings
description: |
  Tower registrations for every handset on the Ashmont networks. A handset
  re-registers roughly every two hours while it is being carried; the six hours
  around the rally are kept at fifteen-minute resolution for every handset on the
  network, as operators do for any public event.
materialization:
  type: table
depends:
  - _gen.pings_led
  - _gen.device_plan
  - _gen.call_overrides
  - _gen.device_base
columns:
  - name: device_id
    type: varchar
    description: Handset that registered
    checks:
      - name: not_null
  - name: ts
    type: timestamp
    description: Fifteen-minute bucket the registration falls in
    checks:
      - name: not_null
  - name: cell_id
    type: varchar
    description: Cell site the handset registered on
    checks:
      - name: not_null
@bruin */

WITH carried_along AS (
    SELECT
        f.device_id,
        f.dev,
        f.follow_rate,
        p.ts,
        p.cell_id
    FROM _gen.device_plan f
    JOIN _gen.pings_led p ON p.device_id = f.follows_device_id
    WHERE f.follows_device_id IS NOT NULL AND NOT f.dark
),
-- A handset that spends most of its life switched off leaves nothing behind but
-- the window it was used in.
switched_on AS (
    SELECT
        d.device_id,
        ({{ rally_date() }}::TIMESTAMP + INTERVAL 17 HOUR + INTERVAL (b * 15) MINUTE) AS ts,
        d.fixed_cell AS cell_id
    FROM _gen.device_plan d
    CROSS JOIN generate_series(3, 8) g(b)
    WHERE d.dark AND d.fixed_cell IS NOT NULL
),
-- Placing a call registers the handset, so every pinned call leaves a row here
-- too. Without this a handset would appear to have called from nowhere.
placed_calls AS (
    SELECT
        b.device_id,
        (date_trunc('hour', c.started_at)
            + INTERVAL (15 * (extract(minute FROM c.started_at)::INTEGER // 15)) MINUTE) AS ts,
        c.cell_id
    FROM _gen.call_overrides c
    JOIN _gen.device_base b ON b.msisdn = c.caller_msisdn
)
SELECT device_id, ts, cell_id FROM _gen.pings_led
UNION ALL
SELECT device_id, ts, cell_id FROM placed_calls
UNION ALL
SELECT device_id, ts, cell_id
FROM carried_along
WHERE {{ rnd("dev::VARCHAR || '|' || ts::VARCHAR", 4501) }} < follow_rate
UNION ALL
SELECT device_id, ts, cell_id FROM switched_on
