/* @bruin
name: qa.single_column_exposure
description: |
  SPOILER-ADJACENT. For each of the four, the size of the crowd standing in every
  single attribute that incriminates them. If any of these ever falls to a handful,
  a player can reach a culprit with one WHERE clause and the case is trivial.
materialization:
  type: table
custom_checks:
  - name: no single-column filter isolates any of the four
    query: SELECT count(*) FROM qa.single_column_exposure WHERE population < 20
    value: 0
depends:
  - qa.integrity
  - town.citizens
  - town.firearm_licences
  - town.devices
  - town.vehicles
@bruin */

WITH four AS (
    SELECT expected AS citizen_id, part FROM qa.integrity WHERE part <> 'the registered keeper'
),
attrs AS (
    SELECT f.part, 'height to the centimetre' AS attribute,
           (SELECT count(*) FROM town.citizens x WHERE x.height_cm = c.height_cm) AS population
    FROM four f JOIN town.citizens c USING (citizen_id)
    UNION ALL
    SELECT f.part, 'shoe size',
           (SELECT count(*) FROM town.citizens x WHERE x.shoe_size_eu = c.shoe_size_eu)
    FROM four f JOIN town.citizens c USING (citizen_id)
    UNION ALL
    SELECT f.part, 'service qualification',
           (SELECT count(*) FROM town.citizens x WHERE x.service_qualification IS NOT DISTINCT FROM c.service_qualification)
    FROM four f JOIN town.citizens c USING (citizen_id)
    UNION ALL
    SELECT f.part, 'birth town',
           (SELECT count(*) FROM town.citizens x WHERE x.birth_town = c.birth_town)
    FROM four f JOIN town.citizens c USING (citizen_id)
    UNION ALL
    SELECT f.part, 'exact date of birth',
           (SELECT count(*) FROM town.citizens x WHERE date_diff('year', x.date_of_birth, DATE '2026-05-14') = date_diff('year', c.date_of_birth, DATE '2026-05-14'))
    FROM four f JOIN town.citizens c USING (citizen_id)
    UNION ALL
    SELECT f.part, 'address district',
           (SELECT count(*) FROM town.citizens x JOIN town.addresses ax USING (address_id)
            WHERE ax.district = (SELECT a2.district FROM town.addresses a2 WHERE a2.address_id = c.address_id))
    FROM four f JOIN town.citizens c USING (citizen_id)
    UNION ALL
    SELECT f.part, 'handset model',
           (SELECT count(*) FROM town.devices x WHERE x.handset_model = d.handset_model)
    FROM four f JOIN town.devices d ON d.citizen_id = f.citizen_id
)
SELECT part, attribute, population FROM attrs
