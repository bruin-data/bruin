/* @bruin
name: town.vehicle_insurance
description: |
  Live motor policies. A policy may name an additional driver, which is how a
  vehicle comes to be driven by someone other than its registered keeper.
materialization:
  type: table
depends:
  - _gen.vehicle_plan
  - _gen.citizen_base
  - _gen.actor_assignments
columns:
  - name: policy_id
    type: varchar
    description: Policy identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: plate
    type: varchar
    description: Vehicle the policy covers
    checks:
      - name: not_null
  - name: policyholder_citizen_id
    type: varchar
    description: Person who holds the policy, null for company policies
  - name: named_driver_citizen_id
    type: varchar
    description: Additional driver named on the policy, null where none is named
  - name: cover
    type: varchar
    description: Level of cover
  - name: started
    type: date
    description: Date cover began
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
-- an additional driver is usually someone at the same address
housemates AS (
    SELECT
        b.citizen_id,
        b.address_id,
        b.licence_number,
        row_number() OVER (PARTITION BY b.address_id ORDER BY {{ rnd('b.seq', 5201) }}, b.citizen_id) AS rk
    FROM _gen.citizen_base b
    WHERE b.age_years BETWEEN 18 AND 78 AND b.licence_number IS NOT NULL
),
keeper_home AS (
    SELECT v.plate, v.n, v.owner_citizen_id, b.address_id
    FROM _gen.vehicle_plan v
    LEFT JOIN _gen.citizen_base b ON b.citizen_id = v.owner_citizen_id
)
SELECT
    'POL-' || lpad(k.n::VARCHAR, 5, '0')                        AS policy_id,
    k.plate,
    k.owner_citizen_id                                          AS policyholder_citizen_id,
    CASE
        -- a keeper who holds no licence of their own always names a driver
        WHEN k.owner_citizen_id = (SELECT actor_e FROM a) THEN (SELECT actor_b FROM a)
        WHEN k.owner_citizen_id IS NULL THEN NULL
        WHEN {{ rnd('k.n', 5210) }} < 0.34 THEN (
            SELECT h.citizen_id FROM housemates h
            WHERE h.address_id = k.address_id AND h.citizen_id <> k.owner_citizen_id
            ORDER BY h.rk LIMIT 1
        )
        ELSE NULL
    END                                                         AS named_driver_citizen_id,
    {{ weighted('k.n', 5211, [['comprehensive', 62], ['third party fire and theft', 86], ['third party', 100]]) }} AS cover,
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('k.n', 5212, 5, 360) }}) DAY)::DATE AS started
FROM keeper_home k
