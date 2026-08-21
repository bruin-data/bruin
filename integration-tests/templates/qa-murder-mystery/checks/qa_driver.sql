/* @bruin
name: qa.driver_stages
description: SPOILER. Cardinality of each stage of the driver thread.
materialization:
  type: table
depends:
  - town.vehicles
  - town.plate_reads
  - town.cameras
  - town.vehicle_insurance
  - town.citizens
  - town.clinic_visits
custom_checks:
  - name: D2 grey or silver hatchbacks on a T plate with a 7 is exactly 58
    query: SELECT count(*) FROM qa.driver_stages
    value: 58
  - name: D2 the same plate pattern is not confined to those 58
    query: SELECT count(*) FROM town.vehicles WHERE plate LIKE 'T%' AND plate LIKE '%7%'
    count: 1
  - name: D3 of the 58, exactly 7 crossed a square approach as the rally broke up
    query: SELECT count(*) FROM qa.driver_stages WHERE in_d3
    value: 7
  - name: D4 exactly one of the 7 left a gap over two hours in its evening route
    query: SELECT count(*) FROM qa.driver_stages WHERE in_d4
    value: 1
  - name: D4 the runner-up gap is under an hour, so the separation is not marginal
    query: SELECT coalesce(max(evening_gap_min), 0) FROM qa.driver_stages WHERE in_d3 AND NOT in_d4
    count: 1
  - name: D5 the vehicle is kept by someone who holds no driving licence
    query: SELECT count(*) FROM qa.driver_stages WHERE in_d4 AND keeper_has_licence
    value: 0
  - name: D5 its policy names exactly one additional driver
    query: SELECT count(*) FROM qa.driver_stages WHERE in_d4 AND named_driver_citizen_id IS NOT NULL
    value: 1
  - name: D5 the keeper and the named driver share a surname and an address
    query: SELECT count(*) FROM qa.driver_stages WHERE in_d4 AND NOT (shares_surname AND shares_address)
    value: 0
  - name: a named additional driver is common enough not to be a tell on its own
    query: SELECT count(*) FROM town.vehicle_insurance WHERE named_driver_citizen_id IS NOT NULL
    count: 1
@bruin */

WITH fifty_eight AS (
    SELECT plate, owner_citizen_id
    FROM town.vehicles
    WHERE colour IN ('grey', 'silver') AND body_type = 'hatchback'
      AND plate LIKE 'T%' AND plate LIKE '%7%'
),
cordon AS (
    SELECT DISTINCT r.plate
    FROM town.plate_reads r
    JOIN town.cameras c USING (camera_id)
    JOIN fifty_eight f USING (plate)
    WHERE c.road_class = 'square approach'
      AND r.ts BETWEEN TIMESTAMP '2026-05-14 18:40:00' AND TIMESTAMP '2026-05-14 19:15:00'
),
evening_route AS (
    SELECT
        plate,
        max(date_diff('minute', ts, nxt)) AS evening_gap_min
    FROM (
        SELECT
            r.plate,
            r.ts,
            lead(r.ts) OVER (PARTITION BY r.plate ORDER BY r.ts) AS nxt
        FROM town.plate_reads r
        JOIN cordon USING (plate)
        WHERE r.ts BETWEEN TIMESTAMP '2026-05-14 18:40:00' AND TIMESTAMP '2026-05-14 22:30:00'
    )
    GROUP BY 1
)
SELECT
    f.plate,
    f.owner_citizen_id,
    i.named_driver_citizen_id,
    coalesce(er.evening_gap_min, 0)                              AS evening_gap_min,
    (k.licence_number IS NOT NULL)                               AS keeper_has_licence,
    (k.last_name = nd.last_name)                                 AS shares_surname,
    (k.address_id = nd.address_id)                               AS shares_address,
    (c.plate IS NOT NULL)                                        AS in_d3,
    coalesce(er.evening_gap_min, 0) > 120                        AS in_d4
FROM fifty_eight f
LEFT JOIN cordon        c  USING (plate)
LEFT JOIN evening_route er USING (plate)
LEFT JOIN town.vehicle_insurance i USING (plate)
LEFT JOIN town.citizens k  ON k.citizen_id  = f.owner_citizen_id
LEFT JOIN town.citizens nd ON nd.citizen_id = i.named_driver_citizen_id
