/* @bruin
name: _gen.device_plan
description: |
  Generation scaffolding. Every handset's resolved movement profile, after both
  override layers have been coalesced over the drawn values. This is what the
  ping generator reads. Dropped with the rest of this schema at the end of the
  run.
materialization:
  type: table
depends:
  - _gen.device_base
  - _gen.scene_overrides
  - _gen.device_overrides
@bruin */

-- A second handset travels in the same pocket as the first, so it reports from
-- wherever the first one is. This is ordinary for anyone carrying a work phone.
WITH primary_of AS (
    SELECT citizen_id, min(device_id) AS device_id
    FROM _gen.device_base
    WHERE primary_handset
    GROUP BY 1
)
SELECT
    b.device_id,
    b.dev,
    b.msisdn,
    b.citizen_id,
    b.primary_handset,
    b.home_cell,
    b.work_cell,
    coalesce(CASE WHEN b.primary_handset THEN s.night_cell END, b.night_cell)   AS night_cell,
    coalesce(CASE WHEN b.primary_handset THEN s.night_share END, b.night_share) AS night_share,
    coalesce(CASE WHEN b.primary_handset THEN s.night_key END, b.night_key)     AS night_key,
    coalesce(CASE WHEN b.primary_handset THEN s.night_invert END, b.night_invert) AS night_invert,
    coalesce(CASE WHEN b.primary_handset THEN s.mobility END, b.mobility)       AS mobility,
    CASE coalesce(CASE WHEN b.primary_handset THEN s.mobility END, b.mobility)
        WHEN 'static'      THEN 2
        WHEN 'normal'      THEN 5
        WHEN 'mobile'      THEN 17
        ELSE                    28
    END                                                                        AS pool_size,
    coalesce(d.activated_date, b.activated_date)                               AS activated_date,
    b.handset_model,
    coalesce(d.evening_pattern, CASE WHEN b.primary_handset THEN s.evening_pattern END, b.evening_pattern) AS evening_pattern,
    coalesce(d.follows_device_id, p.device_id)                                 AS follows_device_id,
    coalesce(d.follow_rate, round(0.52 + {{ rnd('b.dev', 4301) }} * 0.40, 2))   AS follow_rate,
    coalesce(d.dark, FALSE)                                                    AS dark,
    d.fixed_cell,
    coalesce(CASE WHEN b.primary_handset THEN s.gap_start_day END, b.gap_start_day) AS gap_start_day,
    coalesce(CASE WHEN b.primary_handset THEN s.gap_days END, b.gap_days)           AS gap_days
FROM _gen.device_base b
LEFT JOIN _gen.scene_overrides  s ON s.citizen_id = b.citizen_id
LEFT JOIN _gen.device_overrides d ON d.device_id  = b.device_id
LEFT JOIN primary_of            p ON p.citizen_id = b.citizen_id AND NOT b.primary_handset
