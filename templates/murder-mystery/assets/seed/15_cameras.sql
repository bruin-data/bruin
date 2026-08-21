/* @bruin
name: town.cameras
description: |
  Yorkville's automatic number plate cameras. Six of them cover the approaches to
  Wychwood Square; the rest sit on through routes and the town boundary.
materialization:
  type: table
depends:
  - town.cell_towers
columns:
  - name: camera_id
    type: varchar
    description: Camera identifier, format CAM-##
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: location
    type: varchar
    description: Where the camera is mounted and which way it faces
  - name: lat
    type: double
    description: Latitude on the town grid
  - name: lon
    type: double
    description: Longitude on the town grid
  - name: road_class
    type: varchar
    description: square approach, through route or boundary
@bruin */

WITH fixed AS (
    SELECT * FROM (VALUES
        ( 1, 'Austin Terrace at the square, eastbound',        0.028700, 0.030100, 'square approach'),
        ( 2, 'Austin Terrace at the square, westbound',        0.028700, 0.031900, 'square approach'),
        ( 3, 'Macpherson Mews at the service yards, northbound', 0.022600, 0.031000, 'square approach'),
        ( 4, 'Macpherson Mews at the service yards, southbound', 0.022400, 0.031400, 'square approach'),
        ( 5, 'Walmer Road below the north block, northbound', 0.029300, 0.036800, 'square approach'),
        ( 6, 'Spadina Crescent at the square, westbound',         0.029500, 0.024300, 'square approach'),
        ( 7, 'Davenport Road at the weighbridge, northbound', 0.035900, 0.024100, 'through route'),
        ( 8, 'Davenport Road at the weighbridge, southbound', 0.036100, 0.024500, 'through route'),
        ( 9, 'Northern approach at the town boundary',      0.048000, 0.024000, 'boundary'),
        (10, 'Russell Hill Road at the clinic apron',          0.010700, 0.031100, 'through route')
    ) AS t(n, location, lat, lon, road_class)
),
spread AS (
    SELECT
        n,
        'camera on ' || {{ pick('n', 5301, streets) }} || ', ' || {{ pick('n', 5302, ['northbound', 'southbound', 'eastbound', 'westbound']) }} AS location,
        round(0.005 + {{ rnd('n', 5303) }} * 0.045, 6) AS lat,
        round(0.005 + {{ rnd('n', 5304) }} * 0.050, 6) AS lon,
        {{ weighted('n', 5305, [['through route', 70], ['boundary', 100]]) }} AS road_class
    FROM generate_series(11, 24) t(n)
)
SELECT 'CAM-' || lpad(n::VARCHAR, 2, '0') AS camera_id, location, lat, lon, road_class FROM fixed
UNION ALL
SELECT 'CAM-' || lpad(n::VARCHAR, 2, '0'), location, lat, lon, road_class FROM spread
