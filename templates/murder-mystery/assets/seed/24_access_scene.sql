/* @bruin
name: _gen.access_scene
description: |
  Generation scaffolding. Who was in the Loma House while the rally was on,
  and which badge opened which door. Dropped with the rest of this schema at the
  end of the run.
materialization:
  type: table
depends:
  - town.employment
  - town.businesses
  - _gen.citizen_base
@bruin */

WITH maintenance_firm AS (
    SELECT business_id FROM town.businesses WHERE business_id = 'B-0037'
),
contractor_staff AS (
    SELECT
        e.citizen_id,
        row_number() OVER (ORDER BY {{ rnd('e.citizen_id', 6301) }}, e.citizen_id) AS rk
    FROM town.employment e
    JOIN maintenance_firm f USING (business_id)
    WHERE e.ended IS NULL
),
-- the building's own people: a caretaker who is in every day, and two tenants
occupants AS (
    SELECT citizen_id, row_number() OVER (ORDER BY ord, citizen_id) AS rk
    FROM (
        SELECT DISTINCT b.citizen_id, {{ rnd('b.seq', 6302) }} AS ord
        FROM _gen.citizen_base b
        JOIN town.employment e ON e.citizen_id = b.citizen_id AND e.ended IS NULL
        WHERE b.age_years BETWEEN 24 AND 64
          AND b.citizen_id NOT IN (SELECT citizen_id FROM contractor_staff)
    )
)
SELECT * FROM (VALUES
    ('contractor', (SELECT citizen_id FROM contractor_staff WHERE rk = 1), 2, 7),
    ('caretaker',  (SELECT citizen_id FROM occupants WHERE rk = 1),        1, 2),
    ('tenant_1',   (SELECT citizen_id FROM occupants WHERE rk = 2),        1, 11),
    ('tenant_2',   (SELECT citizen_id FROM occupants WHERE rk = 3),        3, 16)
) AS t(part, citizen_id, zone_n, minute_past_eighteen)
