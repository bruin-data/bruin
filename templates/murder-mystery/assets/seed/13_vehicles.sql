/* @bruin
name: town.vehicles
description: |
  Vehicles taxed to an Yorkville address. Registration marks are three letters, a
  dash and three digits. Yorkville's letter series were issued in blocks over the
  years, so the leading letter clusters rather than spreading evenly.
materialization:
  type: table
depends:
  - _gen.vehicle_plan
columns:
  - name: plate
    type: varchar
    description: Registration mark, format AAA-###
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: make
    type: varchar
    description: Manufacturer
  - name: model
    type: varchar
    description: Model
  - name: body_type
    type: varchar
    description: hatchback, saloon, estate, suv, van, coupe or pickup
  - name: colour
    type: varchar
    description: Recorded colour
  - name: year
    type: integer
    description: Model year
  - name: owner_citizen_id
    type: varchar
    description: Registered keeper, null where a company holds the vehicle
  - name: owner_business_id
    type: varchar
    description: Registered company keeper, null for privately held vehicles
  - name: registered_date
    type: date
    description: Date the current keeper was recorded
@bruin */

SELECT
    plate,
    make,
    model,
    body_type,
    colour,
    year,
    owner_citizen_id,
    owner_business_id,
    registered_date
FROM _gen.vehicle_plan
