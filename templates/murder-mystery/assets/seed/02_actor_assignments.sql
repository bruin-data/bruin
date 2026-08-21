/* @bruin
name: _gen.actor_assignments
description: |
  Generation scaffolding. Draws a handful of residents out of broad pools by
  ordering each pool on a derived value and taking the first row. Dropped with
  the rest of this schema at the end of the run.
materialization:
  type: table
depends:
  - _gen.citizen_base
@bruin */

WITH keeper_homes AS (
    SELECT address_id
    FROM _gen.citizen_base
    WHERE age_years BETWEEN 65 AND 82
      AND licence_number IS NULL
    GROUP BY 1
),
pool_a AS (
    SELECT citizen_id, seq, address_id
    FROM _gen.citizen_base
    WHERE sex = 'M' AND age_years BETWEEN 30 AND 55
),
pick_a AS (
    SELECT citizen_id, address_id FROM pool_a ORDER BY {{ rnd('seq', 2101) }}, citizen_id LIMIT 1
),
pool_b AS (
    SELECT b.citizen_id, b.seq, b.address_id
    FROM _gen.citizen_base b
    JOIN keeper_homes k USING (address_id)
    WHERE b.sex = 'M' AND b.age_years BETWEEN 22 AND 40
      AND b.citizen_id NOT IN (SELECT citizen_id FROM pick_a)
),
pick_b AS (
    SELECT citizen_id, address_id FROM pool_b ORDER BY {{ rnd('seq', 2102) }}, citizen_id LIMIT 1
),
pool_e AS (
    SELECT e.citizen_id, e.seq
    FROM _gen.citizen_base e
    JOIN pick_b pb ON pb.address_id = e.address_id
    WHERE e.age_years BETWEEN 65 AND 82 AND e.licence_number IS NULL
),
pick_e AS (
    SELECT citizen_id FROM pool_e ORDER BY {{ rnd('seq', 2103) }}, citizen_id LIMIT 1
),
pool_c AS (
    SELECT citizen_id, seq
    FROM _gen.citizen_base
    WHERE age_years BETWEEN 40 AND 65
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_a)
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_b)
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_e)
),
pick_c AS (
    SELECT citizen_id FROM pool_c ORDER BY {{ rnd('seq', 2104) }}, citizen_id LIMIT 1
),
pool_d AS (
    SELECT d.citizen_id, d.seq
    FROM _gen.citizen_base d
    WHERE d.sex = 'F' AND d.age_years BETWEEN 25 AND 45
      AND d.address_id NOT IN (SELECT address_id FROM pick_a)
      AND d.citizen_id NOT IN (SELECT citizen_id FROM pick_c)
),
pick_d AS (
    SELECT citizen_id FROM pool_d ORDER BY {{ rnd('seq', 2105) }}, citizen_id LIMIT 1
),
pool_v AS (
    SELECT citizen_id, seq
    FROM _gen.citizen_base
    WHERE sex = 'M' AND age_years BETWEEN 48 AND 66
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_a)
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_b)
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_c)
      AND citizen_id NOT IN (SELECT citizen_id FROM pick_e)
),
pick_v AS (
    SELECT citizen_id FROM pool_v ORDER BY {{ rnd('seq', 2106) }}, citizen_id LIMIT 1
)
SELECT
    (SELECT citizen_id FROM pick_a) AS actor_a,
    (SELECT citizen_id FROM pick_b) AS actor_b,
    (SELECT citizen_id FROM pick_c) AS actor_c,
    (SELECT citizen_id FROM pick_d) AS actor_d,
    (SELECT citizen_id FROM pick_e) AS actor_e,
    (SELECT citizen_id FROM pick_v) AS actor_v
