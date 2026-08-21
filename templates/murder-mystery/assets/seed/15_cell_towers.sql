/* @bruin
name: town.cell_towers
description: |
  The 58 mobile cell sites covering Ashmont, laid out on the town's local
  coordinate grid. Coordinates are a synthetic local grid, not a real-world
  location; only distances between points are meaningful.
materialization:
  type: table
columns:
  - name: cell_id
    type: varchar
    description: Site identifier, format CELL-###
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: lat
    type: double
    description: Latitude on the town grid
  - name: lon
    type: double
    description: Longitude on the town grid
  - name: district
    type: varchar
    description: District the site sits in
  - name: coverage_note
    type: varchar
    description: What the operator records this site as serving
@bruin */

WITH grid AS (
    SELECT
        n,
        (n - 1) % 8    AS col,
        (n - 1) // 8   AS row
    FROM generate_series(1, 58) t(n)
)
SELECT
    'CELL-' || lpad(n::VARCHAR, 3, '0')                                          AS cell_id,
    -- three sites are placed by hand because the case depends on exactly where
    -- their coverage falls; the rest are laid out on the grid
    round(CASE n
        WHEN 28 THEN {{ square_lat() }}
        WHEN 36 THEN {{ corvid_lat() }}
        WHEN 20 THEN 0.021500
        ELSE 0.0040 + row * 0.0075 + ({{ rnd('n', 1501) }} - 0.5) * 0.0022
    END, 6)                                                                      AS lat,
    round(CASE n
        WHEN 28 THEN {{ square_lon() }}
        WHEN 36 THEN {{ corvid_lon() }}
        WHEN 20 THEN 0.031200
        ELSE 0.0040 + col * 0.0085 + ({{ rnd('n', 1502) }} - 0.5) * 0.0022
    END, 6)                                                                      AS lon,
    list_extract([{% for d in districts %}'{{ d }}'{% if not loop.last %}, {% endif %}{% endfor %}], 1 + (row // 2) * 2 + (col // 4)) AS district,
    CASE n
        WHEN 28 THEN 'Foundry Square and the civic steps; carries the heaviest daytime load in the town'
        WHEN 36 THEN 'operator microcell on the Corvid Building roof, serving the north block only'
        WHEN 20 THEN 'Kestrel Lane and the service yards behind the square'
        WHEN 35 THEN 'Tollgate Road, the northern approach and the freight weighbridge'
        WHEN 12 THEN 'Almoner Street, Ashmont General Clinic and the ambulance apron'
        WHEN 43 THEN 'Quarry Rise, the Marlpit ranges, reservoir path and allotments'
        ELSE 'residential and retail coverage, ' || {{ pick('n', 1503, streets) }}
    END                                                                          AS coverage_note
FROM grid
