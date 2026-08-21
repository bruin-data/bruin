/* @bruin
name: town.badges
description: |
  Access badges issued for the managed buildings. A badge is issued by whichever
  company sponsors the holder, which for contractors is not the company that
  occupies the building.
materialization:
  type: table
depends:
  - town.employment
  - town.businesses
  - town.building_readers
  - _gen.access_scene
columns:
  - name: badge_id
    type: varchar
    description: Badge identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Badge holder
    checks:
      - name: not_null
  - name: issued_by_business_id
    type: varchar
    description: Company that sponsored the badge
  - name: building
    type: varchar
    description: Building the badge opens
  - name: status
    type: varchar
    description: active, expired or withdrawn
@bruin */

WITH current_spells AS (
    SELECT
        e.citizen_id,
        e.business_id,
        e.role_title,
        row_number() OVER (PARTITION BY e.citizen_id ORDER BY e.started DESC) AS recency
    FROM town.employment e
    WHERE e.ended IS NULL
),
holders AS (
    SELECT citizen_id, business_id, role_title FROM current_spells WHERE recency = 1
),
sites AS (
    SELECT DISTINCT building, row_number() OVER (ORDER BY building) AS bn
    FROM town.building_readers
),
site_count AS (SELECT count(*) AS n FROM sites),
issued AS (
    SELECT citizen_id, business_id, role_title, rk FROM (
        SELECT
            h.*,
            row_number() OVER (
                ORDER BY CASE WHEN sc.citizen_id IS NOT NULL
                              THEN {{ rnd('h.citizen_id', 6201) }} * 0.50
                              ELSE {{ rnd('h.citizen_id', 6201) }} END,
                h.citizen_id
            ) AS rk
        FROM holders h
        LEFT JOIN _gen.access_scene sc ON sc.citizen_id = h.citizen_id
    ) WHERE rk <= 2900
)
SELECT
    'BG-' || lpad(i.rk::VARCHAR, 5, '0')                          AS badge_id,
    i.citizen_id,
    i.business_id                                                 AS issued_by_business_id,
    coalesce(
        CASE WHEN sc.citizen_id IS NOT NULL THEN 'Loma House' END,
        (SELECT building FROM sites WHERE bn = 1 + floor({{ rnd('i.citizen_id', 6202) }} * (SELECT n FROM site_count))::BIGINT)
    )                                                             AS building,
    CASE WHEN sc.citizen_id IS NOT NULL THEN 'active'
         ELSE {{ weighted('i.rk', 6203, [['active', 91], ['expired', 97], ['withdrawn', 100]]) }} END AS status
FROM issued i
LEFT JOIN _gen.access_scene sc USING (citizen_id)
