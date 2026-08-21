/* @bruin
name: _gen.square_traffic
description: |
  Generation scaffolding. Which vehicles passed the square approaches while the
  rally was breaking up, and what each of them did with the rest of the evening.
  Dropped with the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.vehicle_plan
  - _gen.actor_overrides
@bruin */

WITH pinned AS (
    SELECT v.plate
    FROM _gen.vehicle_plan v
    JOIN _gen.actor_overrides o ON o.citizen_id = v.owner_citizen_id
    WHERE o.keeps_vehicle
),
candidates AS (
    SELECT
        v.plate,
        v.n,
        v.plate_group,
        v.owner_business_id IS NOT NULL                        AS company_held,
        p.plate IS NOT NULL                                    AS is_pinned
    FROM _gen.vehicle_plan v
    LEFT JOIN pinned p USING (plate)
),
-- The cordon cameras saw about a hundred and forty vehicles in the thirty-five
-- minutes the square was emptying. The count is filled per registration series
-- so the figure is stable.
ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY plate_group = 'T', company_held
            ORDER BY CASE WHEN is_pinned THEN {{ rnd('n', 5401) }} * 0.20 ELSE {{ rnd('n', 5401) }} END,
            plate
        ) AS rk
    FROM candidates
),
present AS (
    SELECT plate, n, plate_group, company_held, is_pinned, rk
    FROM ranked
    WHERE (plate_group = 'T'  AND     company_held AND rk <= 1)
       OR (plate_group = 'T'  AND NOT company_held AND rk <= 6)
       OR (plate_group <> 'T' AND     company_held AND rk <= 18)
       OR (plate_group <> 'T' AND NOT company_held AND rk <= 112)
)
SELECT
    plate,
    n,
    CASE
        WHEN is_pinned      THEN 'north_exit'
        WHEN company_held   THEN 'fleet_evening'
        WHEN rk % 17 = 3    THEN 'hospital'
        WHEN rk % 23 = 5    THEN 'north_exit'
        WHEN rk % 5 = 0     THEN 'through'
        ELSE                     'short_home'
    END                                                        AS route,
    -- when it crossed the cordon, in minutes past 18:40
    CASE WHEN is_pinned THEN 9 ELSE {{ rnd_int('n', 5410, 0, 34) }} END AS cordon_minute,
    CASE WHEN is_pinned THEN 3 ELSE 1 + (n % 6) END                     AS cordon_camera
FROM present
