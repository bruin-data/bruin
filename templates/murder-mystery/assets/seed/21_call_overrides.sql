/* @bruin
name: _gen.call_overrides
description: |
  Generation scaffolding. Individual calls pinned to a time and a pair of
  numbers, because the case timeline depends on them. Dropped with the rest of
  this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.device_overrides
  - _gen.device_base
  - _gen.scene_overrides
  - _gen.actor_assignments
  - _gen.citizen_base
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
num AS (
    SELECT o.slot, b.msisdn, b.home_cell
    FROM _gen.device_overrides o
    JOIN _gen.device_base b USING (device_id)
),
handset AS (
    SELECT b.citizen_id, b.msisdn, b.home_cell
    FROM _gen.device_base b
    WHERE b.primary_handset
),
-- a counter-sold handset is used where its user happens to be, which for most
-- of them is close to home
carried_near AS (
    SELECT h.home_cell FROM handset h JOIN a ON h.citizen_id = a.actor_a
),
legs AS (
    SELECT * FROM (VALUES
        -- the counter-sold handsets keep to a very short list of numbers
        ('p', 'r', TIMESTAMP '2026-04-10 21:14:00', 214),
        ('p', 'r', TIMESTAMP '2026-04-18 20:02:00', 96),
        ('p', 'q', TIMESTAMP '2026-04-22 19:41:00', 143),
        ('p', 'r', TIMESTAMP '2026-04-29 21:37:00', 62),
        ('p', 'q', TIMESTAMP '2026-05-04 20:19:00', 88),
        ('p', 'r', TIMESTAMP '2026-05-08 22:06:00', 175),
        ('p', 'q', TIMESTAMP '2026-05-11 19:58:00', 51),
        ('p', 'r', TIMESTAMP '2026-05-13 21:22:00', 240),
        ('p', 'q', TIMESTAMP '2026-05-14 16:38:00', 34),
        ('p', 'r', TIMESTAMP '2026-05-14 19:02:00', 27),
        ('r', 'q', TIMESTAMP '2026-04-25 18:44:00', 118),
        ('r', 'q', TIMESTAMP '2026-05-06 20:11:00', 74),
        ('r', 'q', TIMESTAMP '2026-05-14 19:26:00', 41)
    ) AS t(from_slot, to_slot, started_at, duration_sec)
)
SELECT
    f.msisdn        AS caller_msisdn,
    t.msisdn        AS callee_msisdn,
    l.started_at,
    l.duration_sec,
    CASE
        WHEN l.from_slot = 'p' AND l.started_at < {{ rally_date() }}::TIMESTAMP
            THEN (SELECT home_cell FROM carried_near)
        ELSE f.home_cell
    END             AS cell_id,
    'outgoing'      AS direction
FROM legs l
JOIN num f ON f.slot = l.from_slot
JOIN num t ON t.slot = l.to_slot

UNION ALL

-- the one call that was not made to another counter-sold handset
SELECT
    (SELECT msisdn FROM num WHERE slot = 'p'),
    (SELECT h.msisdn FROM handset h JOIN a ON h.citizen_id = a.actor_d),
    TIMESTAMP '2026-05-02 22:47:00',
    71,
    (SELECT home_cell FROM carried_near),
    'outgoing'

UNION ALL

-- a call answered while the square was emptying
SELECT
    (SELECT h.msisdn FROM handset h JOIN _gen.citizen_base b ON b.citizen_id = h.citizen_id
     WHERE b.age_years BETWEEN 25 AND 70 ORDER BY {{ rnd('b.seq', 5901) }}, h.msisdn LIMIT 1),
    (SELECT h.msisdn FROM handset h JOIN _gen.scene_overrides s ON s.citizen_id = h.citizen_id WHERE s.slot = 'g'),
    TIMESTAMP '2026-05-14 18:52:00',
    40,
    (SELECT h.home_cell FROM handset h JOIN _gen.scene_overrides s ON s.citizen_id = h.citizen_id WHERE s.slot = 'g'),
    'incoming'
