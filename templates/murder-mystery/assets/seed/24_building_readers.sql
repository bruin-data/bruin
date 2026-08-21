/* @bruin
name: town.building_readers
description: |
  Badge readers in Yorkville's twelve managed commercial buildings. Every door with
  a reader on it appears here, including the ones tenants rarely use.
materialization:
  type: table
columns:
  - name: reader_id
    type: varchar
    description: Reader identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: building
    type: varchar
    description: Building the reader is fitted in
  - name: zone
    type: varchar
    description: Which door or route the reader controls
@bruin */

WITH buildings AS (
    SELECT * FROM (VALUES
        (1, 'Loma House'), (2, 'Wychwood Exchange'), (3, 'Hillcrest Chambers'),
        (4, 'Davenport House'), (5, 'Bracondale Works'),   (6, 'Seaton Green Offices'),
        (7, 'Wells Hill Court'), (8, 'Walmer House'), (9, 'Tarragona Mill'),
        (10, 'Spadina Annexe'), (11, 'Poplar Plains House'), (12, 'Clarendon Chambers')
    ) AS t(bn, building)
),
zones AS (
    SELECT * FROM (VALUES
        (1, 'main entrance'), (2, 'stairwell'), (3, 'service door'), (4, 'loading bay')
    ) AS t(zn, zone)
)
SELECT
    'RDR-' || lpad(b.bn::VARCHAR, 2, '0') || '-' || z.zn::VARCHAR AS reader_id,
    b.building,
    z.zone
FROM buildings b
CROSS JOIN zones z
WHERE z.zn <= 3 OR b.bn % 2 = 1
