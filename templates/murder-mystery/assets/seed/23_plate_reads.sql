/* @bruin
name: town.plate_reads
description: |
  Automatic number plate reads from Ashmont's 24 cameras. A vehicle is read only
  when it passes a camera, so an absence means either that the vehicle was parked
  or that it was on roads no camera covers.
materialization:
  type: table
depends:
  - _gen.vehicle_plan
  - _gen.square_traffic
  - town.cameras
columns:
  - name: read_id
    type: varchar
    description: Read identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: camera_id
    type: varchar
    description: Camera that made the read
    checks:
      - name: not_null
  - name: plate
    type: varchar
    description: Registration mark read
    checks:
      - name: not_null
  - name: ts
    type: timestamp
    description: When the read was made
    checks:
      - name: not_null
  - name: direction
    type: varchar
    description: Direction of travel past the camera
@bruin */

WITH ordinary AS (
    SELECT
        v.plate,
        v.n,
        d.day,
        k.k
    FROM _gen.vehicle_plan v
    CROSS JOIN generate_series(0, {{ var.ping_window_days | int }} - 1) d(day)
    CROSS JOIN generate_series(1, 4) k(k)
    WHERE {{ rnd('v.n * 10000 + d.day * 10 + k.k', 5501) }} <
        CASE v.usage_class
            WHEN 'light'    THEN list_extract([0.45, 0.15, 0.04, 0.01], k.k)
            WHEN 'ordinary' THEN list_extract([0.85, 0.42, 0.12, 0.04], k.k)
            ELSE                 list_extract([0.98, 0.85, 0.55, 0.30], k.k)
        END
),
ordinary_reads AS (
    SELECT
        'CAM-' || lpad((1 + floor({{ rnd('n * 10000 + day * 10 + k', 5502) }} * 24)::BIGINT)::VARCHAR, 2, '0') AS camera_id,
        plate,
        ({{ movement_start() }} + INTERVAL (day) DAY
            + INTERVAL ({{ rnd_int('n * 10000 + day * 10 + k', 5503, 6, 23) }}) HOUR
            + INTERVAL ({{ rnd_int('n * 10000 + day * 10 + k', 5504, 0, 59) }}) MINUTE)::TIMESTAMP AS ts,
        {{ weighted('n * 10000 + day * 10 + k', 5505, [['inbound', 50], ['outbound', 100]]) }} AS direction
    FROM ordinary
),
-- The evening the rally broke up is reconstructed from the cordon logs rather
-- than sampled, so each vehicle's route through the cameras is continuous.
legs AS (
    SELECT * FROM (VALUES
        ('north_exit',    0,   0, 'cordon',      'outbound'),
        ('north_exit',    1,   9,  'CAM-07',     'outbound'),
        ('north_exit',    2, 163, 'CAM-09',      'inbound'),
        ('north_exit',    3, 170, 'CAM-08',      'inbound'),
        ('north_exit',    4, 184, 'residential', 'inbound'),
        ('short_home',    0,   0, 'cordon',      'outbound'),
        ('short_home',    1,  14, 'residential', 'inbound'),
        ('hospital',      0,   0, 'cordon',      'outbound'),
        ('hospital',      1,  26, 'CAM-10',      'inbound'),
        ('through',       0,   0, 'cordon',      'outbound'),
        ('through',       1,   7, 'CAM-07',      'outbound'),
        ('through',       2,  21, 'CAM-09',      'outbound'),
        ('fleet_evening', 0,   0, 'cordon',      'outbound'),
        ('fleet_evening', 1,  18, 'residential', 'outbound'),
        ('fleet_evening', 2,  37, 'residential', 'inbound'),
        ('fleet_evening', 3,  55, 'residential', 'outbound'),
        ('fleet_evening', 4,  74, 'residential', 'inbound'),
        ('fleet_evening', 5,  96, 'residential', 'outbound'),
        ('fleet_evening', 6, 120, 'residential', 'inbound'),
        ('fleet_evening', 7, 151, 'residential', 'outbound'),
        ('fleet_evening', 8, 188, 'residential', 'inbound')
    ) AS t(route, leg, minute_offset, camera_slot, direction)
),
route_reads AS (
    SELECT
        CASE l.camera_slot
            WHEN 'cordon'      THEN 'CAM-' || lpad(t.cordon_camera::VARCHAR, 2, '0')
            WHEN 'residential' THEN 'CAM-' || lpad((11 + floor({{ rnd('t.n * 100 + l.leg', 5510) }} * 14)::BIGINT)::VARCHAR, 2, '0')
            ELSE l.camera_slot
        END AS camera_id,
        t.plate,
        ({{ rally_date() }}::TIMESTAMP + INTERVAL 18 HOUR + INTERVAL 40 MINUTE
            + INTERVAL (t.cordon_minute + l.minute_offset) MINUTE) AS ts,
        l.direction
    FROM _gen.square_traffic t
    JOIN legs l ON l.route = t.route
)
SELECT
    'PR-' || lpad(row_number() OVER (ORDER BY ts, plate, camera_id, direction)::VARCHAR, 7, '0') AS read_id,
    camera_id,
    plate,
    ts,
    direction
FROM (
    SELECT camera_id, plate, ts, direction FROM ordinary_reads
    WHERE NOT (ts >= {{ rally_date() }}::TIMESTAMP + INTERVAL 18 HOUR
               AND ts <  {{ rally_date() }}::TIMESTAMP + INTERVAL 23 HOUR)
    UNION ALL
    SELECT camera_id, plate, ts, direction FROM route_reads
)
