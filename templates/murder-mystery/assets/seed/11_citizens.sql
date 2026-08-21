/* @bruin
name: town.citizens
description: |
  The residents of Yorkville as the civic register holds them.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_overrides
columns:
  - name: citizen_id
    type: varchar
    description: Resident identifier, format C#####
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: first_name
    type: varchar
    description: Given name
  - name: last_name
    type: varchar
    description: Family name
  - name: date_of_birth
    type: date
    description: Date of birth
  - name: sex
    type: varchar
    description: M or F as recorded on the register
  - name: address_id
    type: integer
    description: Registered address
  - name: height_cm
    type: integer
    description: Height in centimetres as last recorded
  - name: handedness
    type: varchar
    description: right, left or ambidextrous
  - name: eye_colour
    type: varchar
    description: Eye colour
  - name: phone_number
    type: varchar
    description: Registered contact number, format 55-###-####
    checks:
      - name: unique
  - name: marital_status
    type: varchar
    description: single, married, cohabiting, divorced, widowed, or minor
  - name: moved_in_date
    type: date
    description: Date the resident registered at the current address
  - name: birth_town
    type: varchar
    description: Town of birth
  - name: prior_service
    type: varchar
    description: Service branch, null for residents who never served
  - name: service_qualification
    type: varchar
    description: Recorded service qualification, null for most who served
  - name: shoe_size_eu
    type: integer
    description: Shoe size, continental sizing
  - name: licence_number
    type: varchar
    description: Driving licence number, null for residents who hold none
@bruin */

SELECT
    b.citizen_id,
    coalesce(o.first_name, b.first_name)                        AS first_name,
    coalesce(o.last_name, b.last_name)                          AS last_name,
    b.date_of_birth,
    b.sex,
    b.address_id,
    coalesce(o.height_cm, b.height_cm)                          AS height_cm,
    coalesce(o.handedness, b.handedness)                        AS handedness,
    coalesce(o.eye_colour, b.eye_colour)                        AS eye_colour,
    b.phone_number,
    coalesce(o.marital_status, b.marital_status)                AS marital_status,
    b.moved_in_date,
    b.birth_town,
    coalesce(o.prior_service, b.prior_service)                  AS prior_service,
    nullif(coalesce(o.service_qualification, b.service_qualification_raw), '') AS service_qualification,
    coalesce(o.shoe_size_eu, b.shoe_size_eu)                    AS shoe_size_eu,
    coalesce(o.licence_number, b.licence_number)                AS licence_number
FROM _gen.citizen_base b
LEFT JOIN _gen.actor_overrides o USING (citizen_id)
