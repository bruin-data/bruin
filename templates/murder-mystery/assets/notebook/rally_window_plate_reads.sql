/* @bruin
name: notebook.rally_window_plate_reads
description: |
  Every camera read on an approach to Wychwood Square while the rally was breaking
  up, with the vehicle's description attached.

  This asset is here as a worked example of the one habit that makes this case
  tractable: when a query gives you a set you will need again, materialize it as an
  asset instead of retyping it. Copy this shape for your own notes. Anything under
  assets/notebook/ is yours, and Bruin will work out the order to build them in.

  Try it:
    bruin run assets/notebook/rally_window_plate_reads.sql
    bruin query -c duckdb-yorkville -q "SELECT * FROM notebook.rally_window_plate_reads LIMIT 20"
materialization:
  type: table
depends:
  - town.plate_reads
  - town.cameras
  - town.vehicles
columns:
  - name: plate
    type: varchar
    description: Registration mark read
  - name: ts
    type: timestamp
    description: When the read was made
  - name: camera_id
    type: varchar
    description: Camera that made the read
  - name: location
    type: varchar
    description: Where that camera is mounted
  - name: direction
    type: varchar
    description: Direction of travel past the camera
  - name: colour
    type: varchar
    description: Recorded colour of the vehicle
  - name: body_type
    type: varchar
    description: Body style of the vehicle
  - name: owner_citizen_id
    type: varchar
    description: Registered keeper, null where a company holds the vehicle
@bruin */

SELECT
    r.plate,
    r.ts,
    r.camera_id,
    c.location,
    r.direction,
    v.colour,
    v.body_type,
    v.owner_citizen_id
FROM town.plate_reads r
JOIN town.cameras  c ON c.camera_id = r.camera_id
JOIN town.vehicles v ON v.plate = r.plate
WHERE c.road_class = 'square approach'
  AND r.ts BETWEEN TIMESTAMP '2026-05-14 18:40:00' AND TIMESTAMP '2026-05-14 19:15:00'
ORDER BY r.ts
