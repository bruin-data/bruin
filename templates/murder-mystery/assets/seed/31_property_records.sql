/* @bruin
name: town.property_records
description: |
  The land register: every parcel in Ashmont, who holds it and how it is zoned.
  The Foundry Quay parcels are the town's last large undeveloped riverside site.
materialization:
  type: table
depends:
  - town.addresses
  - town.businesses
  - _gen.citizen_base
columns:
  - name: parcel_id
    type: varchar
    description: Parcel identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: address_id
    type: integer
    description: Address the parcel sits at
  - name: district
    type: varchar
    description: District the parcel sits in
  - name: owner_citizen_id
    type: varchar
    description: Resident holder, null where a company holds the parcel
  - name: owner_business_id
    type: varchar
    description: Company holder, null for privately held parcels
  - name: zoning_class
    type: varchar
    description: residential, commercial, industrial, mixed or undeveloped
  - name: last_transfer
    type: date
    description: Date the parcel last changed hands
@bruin */

WITH holders AS (
    SELECT citizen_id, seq, row_number() OVER (ORDER BY {{ rnd('seq', 6501) }}, citizen_id) AS rk
    FROM _gen.citizen_base
    WHERE age_years >= 22
),
holder_count AS (SELECT count(*) AS n FROM holders),
firms AS (
    SELECT business_id, row_number() OVER (ORDER BY business_id) AS rk FROM town.businesses
),
firm_count AS (SELECT count(*) AS n FROM firms),
sites AS (
    SELECT
        address_id,
        district,
        building_type,
        row_number() OVER (ORDER BY address_id) AS pn
    FROM town.addresses
),
quay AS (
    -- the eighteen riverside parcels the rezoning motion turned on
    SELECT pn FROM sites WHERE district = 'Foundry Quay' ORDER BY pn LIMIT 18
)
SELECT
    'P-' || lpad(s.pn::VARCHAR, 5, '0')                          AS parcel_id,
    s.address_id,
    s.district,
    CASE
        WHEN q.pn IS NOT NULL              THEN NULL
        WHEN {{ rnd('s.pn', 6510) }} < 0.79 THEN (SELECT citizen_id FROM holders WHERE rk = 1 + (s.pn - 1) % (SELECT n FROM holder_count))
        ELSE NULL
    END                                                          AS owner_citizen_id,
    CASE
        WHEN q.pn IS NOT NULL              THEN 'B-0001'
        WHEN {{ rnd('s.pn', 6510) }} < 0.79 THEN NULL
        ELSE (SELECT business_id FROM firms WHERE rk = 1 + floor({{ rnd('s.pn', 6511) }} * (SELECT n FROM firm_count))::BIGINT)
    END                                                          AS owner_business_id,
    CASE
        WHEN q.pn IS NOT NULL                                    THEN 'undeveloped'
        WHEN s.building_type IN ('commercial')                    THEN 'commercial'
        WHEN s.building_type = 'mixed_use'                        THEN 'mixed'
        WHEN {{ rnd('s.pn', 6512) }} < 0.04                       THEN 'industrial'
        ELSE                                                          'residential'
    END                                                          AS zoning_class,
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('s.pn', 6513, 60, 12000) }}) DAY)::DATE AS last_transfer
FROM sites s
LEFT JOIN quay q ON q.pn = s.pn
