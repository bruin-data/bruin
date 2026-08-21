/* @bruin
name: town.hotel_stays
description: |
  Hotel bookings made by Yorkville residents, in the town and in the towns around
  it. The booking is recorded against whoever paid for the room.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_assignments
columns:
  - name: booking_id
    type: varchar
    description: Booking identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: hotel_id
    type: varchar
    description: Hotel identifier
  - name: booker_citizen_id
    type: varchar
    description: Resident who made the booking
    checks:
      - name: not_null
  - name: check_in
    type: date
    description: First night of the stay
  - name: check_out
    type: date
    description: Morning the room was given up
  - name: guests
    type: integer
    description: Guests on the booking
  - name: town_name
    type: varchar
    description: Town the hotel is in
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
bookers AS (
    SELECT citizen_id, seq FROM _gen.citizen_base WHERE age_years >= 20
),
spine AS (
    SELECT b.citizen_id, b.seq, j.trip
    FROM bookers b
    CROSS JOIN generate_series(1, 3) j(trip)
    WHERE {{ rnd('b.seq * 10 + j.trip', 6801) }} < list_extract([0.44, 0.14, 0.04], j.trip)
),
drawn AS (
    SELECT
        s.citizen_id,
        {{ rnd_int('s.seq * 10 + s.trip', 6810, 1, 34) }}                AS hotel_n,
        ({{ ledger_start() }} + INTERVAL ({{ rnd_int('s.seq * 10 + s.trip', 6811, 0, 90) }}) DAY)::DATE AS check_in,
        {{ rnd_int('s.seq * 10 + s.trip', 6812, 1, 6) }}                 AS nights,
        {{ rnd_int('s.seq * 10 + s.trip', 6813, 1, 4) }}                 AS guests,
        {{ pick('s.seq * 10 + s.trip', 6814, ['Yorkville', 'Deer Park', 'Summerhill', 'Moore Park', 'Rathnelly', 'Humewood', 'Cedarvale', 'Lytton Park']) }} AS town_name
    FROM spine s
),
pinned AS (
    SELECT
        (SELECT actor_d FROM a) AS citizen_id,
        12                      AS hotel_n,
        DATE '2026-05-12'       AS check_in,
        2                       AS nights,
        2                       AS guests,
        'Humewood'              AS town_name
)
SELECT
    'HB-' || lpad(row_number() OVER (ORDER BY check_in, citizen_id, hotel_n, nights, guests, town_name)::VARCHAR, 5, '0') AS booking_id,
    'HT-' || lpad(hotel_n::VARCHAR, 2, '0')                     AS hotel_id,
    citizen_id                                                  AS booker_citizen_id,
    check_in,
    (check_in + INTERVAL (nights) DAY)::DATE                    AS check_out,
    guests,
    town_name
FROM (
    SELECT citizen_id, hotel_n, check_in, nights, guests, town_name FROM drawn
    UNION ALL
    SELECT citizen_id, hotel_n, check_in, nights, guests, town_name FROM pinned
)
