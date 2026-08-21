/* @bruin
name: _gen.range_squad
description: |
  Generation scaffolding. The Bracondale club's long-range competition squad, filled
  to a quota within each stature-and-training pool so the squad's composition is
  stable. Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_overrides
@bruin */

WITH resolved AS (
    SELECT
        b.citizen_id,
        b.seq,
        b.sex,
        coalesce(o.height_cm, b.height_cm)                                      AS height_cm,
        coalesce(o.service_qualification, b.service_qualification_raw)           AS qualification,
        o.range_skill IS NOT NULL                                               AS pinned
    FROM _gen.citizen_base b
    LEFT JOIN _gen.actor_overrides o USING (citizen_id)
    WHERE b.age_years BETWEEN 18 AND 70
),
tagged AS (
    SELECT
        *,
        (sex = 'M' AND height_cm BETWEEN 186 AND 194)                           AS in_band,
        (qualification IN ('marksman', 'designated marksman'))                  AS trained
    FROM resolved
),
ranked AS (
    SELECT
        citizen_id,
        in_band,
        trained,
        row_number() OVER (
            PARTITION BY in_band, trained
            ORDER BY {{ rnd('seq', 3101) }}, citizen_id
        ) AS rk
    FROM tagged
    WHERE NOT pinned
)
SELECT citizen_id, in_band, trained FROM tagged WHERE pinned
UNION ALL
SELECT citizen_id, in_band, trained
FROM ranked
WHERE (in_band     AND trained     AND rk <= 2)
   OR (in_band     AND NOT trained AND rk <= 6)
   OR (NOT in_band AND trained     AND rk <= 12)
   OR (NOT in_band AND NOT trained AND rk <= 24)
