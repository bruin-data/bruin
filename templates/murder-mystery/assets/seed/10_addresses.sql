/* @bruin
name: town.addresses
description: |
  Every postal address in Yorkville. Each street has an anchor point on the town
  grid and its addresses run along it, so neighbours really are neighbours and
  share a cell site.
materialization:
  type: table
depends:
  - town.cell_towers
columns:
  - name: address_id
    type: integer
    description: Address identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: street
    type: varchar
    description: Street name
  - name: number
    type: integer
    description: House or building number
  - name: unit
    type: varchar
    description: Flat or unit designation, null for whole-building addresses
  - name: district
    type: varchar
    description: District, taken from the covering cell site
  - name: lat
    type: double
    description: Latitude on the town grid
  - name: lon
    type: double
    description: Longitude on the town grid
  - name: building_type
    type: varchar
    description: house, terrace, flat_block, maisonette, commercial or mixed_use
  - name: nearest_cell_id
    type: varchar
    description: Cell site whose coverage this address falls in
    checks:
      - name: not_null
@bruin */

WITH spine AS (
    SELECT
        n                                       AS address_id,
        (n - 1) % 90                            AS street_idx,
        (n - 1) // 90                           AS along
    FROM generate_series(1, 4900) t(n)
),
placed AS (
    SELECT
        address_id,
        street_idx,
        along,
        list_extract([{% for s in streets %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor %}], street_idx + 1) AS street,
        1 + along * 2 + (address_id % 2)        AS number,
        -- the street's anchor on the grid, then a walk along it
        0.0045 + (street_idx // 10) * 0.0062
            + ({{ rnd('street_idx', 1010) }} - 0.5) * 0.0016
            + (along - 27) * 0.000042            AS lat,
        0.0045 + (street_idx % 10) * 0.0066
            + ({{ rnd('street_idx', 1011) }} - 0.5) * 0.0016
            + (along - 27) * 0.000038            AS lon,
        {{ weighted('address_id', 1012, [['house', 34], ['terrace', 59], ['flat_block', 81], ['maisonette', 89], ['commercial', 97], ['mixed_use', 100]]) }} AS building_type
    FROM spine
),
nearest AS (
    SELECT
        p.address_id,
        c.cell_id,
        c.district,
        row_number() OVER (
            PARTITION BY p.address_id
            ORDER BY (p.lat - c.lat) * (p.lat - c.lat) + (p.lon - c.lon) * (p.lon - c.lon), c.cell_id
        ) AS proximity_rank
    FROM placed p
    CROSS JOIN town.cell_towers c
)
SELECT
    p.address_id,
    p.street,
    p.number,
    CASE
        WHEN p.building_type = 'flat_block'  THEN 'Flat ' || {{ rnd_int('p.address_id', 1013, 1, 24) }}
        WHEN p.building_type = 'maisonette'  THEN CASE WHEN p.address_id % 2 = 0 THEN 'Upper' ELSE 'Lower' END
        ELSE NULL
    END                             AS unit,
    n.district,
    round(p.lat, 6)                 AS lat,
    round(p.lon, 6)                 AS lon,
    p.building_type,
    n.cell_id                       AS nearest_cell_id
FROM placed p
JOIN nearest n
  ON n.address_id = p.address_id AND n.proximity_rank = 1
