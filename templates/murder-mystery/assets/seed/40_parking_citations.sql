/* @bruin
name: town.parking_citations
description: |
  Parking notices issued by the town wardens. The warden records the plate, not
  the driver.
materialization:
  type: table
depends:
  - _gen.vehicle_plan
columns:
  - name: citation_id
    type: varchar
    description: Notice identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: plate
    type: varchar
    description: Registration mark on the notice
    checks:
      - name: not_null
  - name: issued_at
    type: timestamp
    description: When the notice was issued
  - name: street
    type: varchar
    description: Where the vehicle was standing
  - name: contravention
    type: varchar
    description: What the notice was issued for
  - name: paid
    type: boolean
    description: Whether the notice has been paid
@bruin */

WITH spine AS (
    SELECT v.plate, v.n, c.cite
    FROM _gen.vehicle_plan v
    CROSS JOIN generate_series(1, 6) c(cite)
    WHERE {{ rnd('v.n * 10 + c.cite', 7201) }} < list_extract([0.92, 0.61, 0.38, 0.21, 0.11, 0.05], c.cite)
)
SELECT
    'PC-' || lpad(row_number() OVER (ORDER BY n, cite)::VARCHAR, 6, '0') AS citation_id,
    plate,
    ({{ ledger_start() }} + INTERVAL ({{ rnd_int('n * 10 + cite', 7210, 0, 90) }}) DAY
        + INTERVAL ({{ rnd_int('n * 10 + cite', 7211, 7, 21) }}) HOUR
        + INTERVAL ({{ rnd_int('n * 10 + cite', 7212, 0, 59) }}) MINUTE)::TIMESTAMP AS issued_at,
    {{ pick('n * 10 + cite', 7213, streets) }}                            AS street,
    {{ weighted('n * 10 + cite', 7214, [['parked on a restricted length', 34], ['overstayed a paid period', 61], ['no valid permit displayed', 79], ['obstructing a service access', 90], ['parked on a loading bay', 100]]) }} AS contravention,
    {{ chance('n * 10 + cite', 7215, 0.71) }}                             AS paid
FROM spine
