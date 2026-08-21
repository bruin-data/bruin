/* @bruin
name: town.travel_records
description: |
  Departures and arrivals recorded against Ashmont residents at the regional
  terminals. Journeys by road within the region are not recorded here.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_assignments
columns:
  - name: record_id
    type: varchar
    description: Record identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Traveller
    checks:
      - name: not_null
  - name: direction
    type: varchar
    description: departure or arrival
  - name: ts
    type: timestamp
    description: When the traveller passed the desk
  - name: carrier
    type: varchar
    description: Carrier the traveller booked with
  - name: destination_code
    type: varchar
    description: Three-letter code for the other end of the journey
  - name: destination_type
    type: varchar
    description: domestic or international
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
travellers AS (
    SELECT citizen_id, seq FROM _gen.citizen_base WHERE age_years >= 16
),
spine AS (
    SELECT
        t.citizen_id,
        t.seq,
        j.trip
    FROM travellers t
    CROSS JOIN generate_series(1, 3) j(trip)
    WHERE {{ rnd('t.seq * 10 + j.trip', 6701) }} < list_extract([0.36, 0.13, 0.04], j.trip)
),
legs AS (
    SELECT
        s.citizen_id,
        s.seq,
        s.trip,
        ({{ ledger_start() }} + INTERVAL ({{ rnd_int('s.seq * 10 + s.trip', 6702, 0, 96) }}) DAY
            + INTERVAL ({{ rnd_int('s.seq * 10 + s.trip', 6703, 5, 22) }}) HOUR
            + INTERVAL ({{ rnd_int('s.seq * 10 + s.trip', 6704, 0, 59) }}) MINUTE)::TIMESTAMP AS out_ts,
        {{ rnd_int('s.seq * 10 + s.trip', 6705, 2, 21) }} AS nights,
        {{ pick('s.seq * 10 + s.trip', 6706, ['Coastal Air', 'Northern Rail Link', 'Marlpit Coaches', 'Regional Air', 'Weirs Ferry']) }} AS carrier,
        {{ pick('s.seq * 10 + s.trip', 6707, ['HRB', 'DNM', 'CWD', 'NTH', 'STW', 'THR', 'LDB', 'PDS', 'RVM', 'IKG', 'BYW', 'WNL']) }} AS destination_code,
        {{ weighted('s.seq * 10 + s.trip', 6708, [['international', 34], ['domestic', 100]]) }} AS destination_type
    FROM spine s
),
pinned AS (
    SELECT
        (SELECT actor_d FROM a)                     AS citizen_id,
        TIMESTAMP '2026-05-17 07:35:00'             AS out_ts,
        16                                          AS nights,
        'Coastal Air'                               AS carrier,
        'HRB'                                       AS destination_code,
        'international'                              AS destination_type
),
both_ways AS (
    SELECT citizen_id, 'departure' AS direction, out_ts AS ts, carrier, destination_code, destination_type FROM legs
    UNION ALL
    SELECT citizen_id, 'arrival', (out_ts + INTERVAL (nights) DAY)::TIMESTAMP, carrier, destination_code, destination_type FROM legs
    UNION ALL
    SELECT citizen_id, 'departure', out_ts, carrier, destination_code, destination_type FROM pinned
)
SELECT
    'TR-' || lpad(row_number() OVER (ORDER BY ts, citizen_id, direction, carrier, destination_code, destination_type)::VARCHAR, 6, '0') AS record_id,
    citizen_id,
    direction,
    ts,
    carrier,
    destination_code,
    destination_type
FROM both_ways
WHERE ts <= {{ ledger_end() }}::TIMESTAMP + INTERVAL 20 DAY
