/* @bruin
name: _gen.pings_led
description: |
  Generation scaffolding. Tower registrations for every handset that moves under
  its own steam. Handsets that travel with another one are added downstream by
  copying from here. Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.device_plan
@bruin */

WITH leaders AS (
    SELECT *
    FROM _gen.device_plan
    WHERE follows_device_id IS NULL AND NOT dark
),
-- Ordinary reporting: a carried handset re-registers roughly every two hours.
grid AS (
    SELECT
        l.device_id,
        l.dev,
        l.home_cell,
        l.work_cell,
        l.night_cell,
        l.night_share,
        l.night_key,
        l.night_invert,
        l.mobility,
        l.pool_size,
        l.gap_start_day,
        l.gap_days,
        d.day,
        s.slot
    FROM leaders l
    CROSS JOIN generate_series(0, {{ var.ping_window_days | int }} - 1) d(day)
    CROSS JOIN generate_series(0, 11) s(slot)
),
placed AS (
    SELECT
        device_id,
        ({{ movement_start() }} + INTERVAL (day) DAY + INTERVAL (slot * 2) HOUR)::TIMESTAMP AS ts,
        CASE
            -- A night spans midnight, so the evening slots belong to the same
            -- night as the small hours that follow them. Keying the decision on
            -- the calendar day instead would move every sleeper at midnight.
            WHEN slot <= 3 OR slot >= 10 THEN
                CASE
                    WHEN night_invert
                        THEN CASE WHEN {{ rnd('night_key * 100000 + day + CASE WHEN slot >= 10 THEN 1 ELSE 0 END', 4401) }} >= 1.0 - night_share THEN night_cell ELSE home_cell END
                    ELSE CASE WHEN {{ rnd('night_key * 100000 + day + CASE WHEN slot >= 10 THEN 1 ELSE 0 END', 4401) }} <  night_share       THEN night_cell ELSE home_cell END
                END
            WHEN slot BETWEEN 5 AND 8 AND mobility IN ('static', 'normal') THEN work_cell
            ELSE {{ cell_id_of('1 + floor(' + rnd('dev * 1000 + floor(' + rnd('dev * 1000000 + day * 100 + slot', 4402) + ' * pool_size)::BIGINT', 4403) + ' * 58)::BIGINT') }}
        END AS cell_id
    FROM grid
    -- the rally evening is logged at a finer resolution, below
    WHERE NOT (day = date_diff('day', {{ movement_start() }}, {{ rally_date() }}) AND slot >= 9)
      AND (gap_start_day IS NULL OR day < gap_start_day OR day >= gap_start_day + gap_days)
),
-- The rally evening. Operators keep every fifteen-minute registration for a
-- public event, so the same six hours are recorded at four times the usual
-- resolution for every handset on the network.
evening AS (
    SELECT
        l.device_id,
        l.dev,
        l.home_cell,
        l.pool_size,
        l.evening_pattern,
        b.b,
        ({{ rally_date() }}::TIMESTAMP + INTERVAL 17 HOUR + INTERVAL (b * 15) MINUTE) AS ts
    FROM leaders l
    CROSS JOIN generate_series(0, 24) b(b)
),
evening_placed AS (
    SELECT
        device_id,
        ts,
        CASE evening_pattern
            -- a handset lying still re-registers on the same site every time
            WHEN 'locked' THEN home_cell
            -- a handset on a windowsill hands back and forth between neighbours
            WHEN 'drift'  THEN {{ cell_id_of('1 + floor(' + rnd('dev * 1000 + (b // 3) % 3', 4410) + ' * 58)::BIGINT') }}
            -- the crowd in the square, arriving and drifting away afterwards
            WHEN 'rally'  THEN CASE
                WHEN b BETWEEN 2 AND 11 THEN {{ square_cell() }}
                ELSE {{ cell_id_of('1 + floor(' + rnd('dev * 1000 + (b // 4)', 4411) + ' * 58)::BIGINT') }}
            END
            ELSE {{ cell_id_of('1 + floor(' + rnd('dev * 1000 + floor(' + rnd('dev * 100000 + (b // 4)', 4412) + ' * pool_size)::BIGINT', 4403) + ' * 58)::BIGINT') }}
        END AS cell_id
    FROM evening
)
SELECT device_id, ts, cell_id FROM placed
UNION ALL
SELECT device_id, ts, cell_id FROM evening_placed
