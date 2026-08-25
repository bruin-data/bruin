/* @bruin
name: qa.base_rates
description: |
  SPOILER-ADJACENT. The anti-trivia guarantee: every incriminating attribute has a
  crowd standing in it, so no single-column filter can isolate anybody. Each check
  asserts an observed population count against its design target within about ten
  per cent, and fails by returning a row when it drifts out of tolerance.
materialization:
  type: table
depends:
  - town.citizens
  - town.firearm_licences
  - town.range_visits
  - town.vehicles
  - town.devices
  - town.call_records
  - town.travel_records
  - town.card_transactions
custom_checks:
  - name: every base rate is within tolerance of its design target
    query: SELECT count(*) FROM qa.base_rates WHERE observed NOT BETWEEN low AND high
    value: 0
@bruin */

WITH rates AS (
    SELECT 'male, 186-194cm' AS attribute, 412 AS target,
           (SELECT count(*) FROM town.citizens WHERE sex = 'M' AND height_cm BETWEEN 186 AND 194) AS observed
    UNION ALL SELECT 'active firearm certificate', 940,
           (SELECT count(*) FROM town.firearm_licences WHERE status = 'active')
    UNION ALL SELECT 'active certificate, 7.62x51', 340,
           (SELECT count(*) FROM town.firearm_licences WHERE status = 'active' AND calibre = '7.62x51')
    UNION ALL SELECT 'prior service', 1050,
           (SELECT count(*) FROM town.citizens WHERE prior_service IS NOT NULL)
    UNION ALL SELECT 'service with a marksman qualification', 44,
           (SELECT count(*) FROM town.citizens WHERE service_qualification IN ('marksman', 'designated marksman'))
    UNION ALL SELECT 'range club members', 1600,
           (SELECT count(DISTINCT citizen_id) FROM town.range_visits)
    UNION ALL SELECT 'grey or silver hatchbacks', 890,
           (SELECT count(*) FROM town.vehicles WHERE colour IN ('grey', 'silver') AND body_type = 'hatchback')
    UNION ALL SELECT 'grey or silver hatchback, T plate with a 7', 58,
           (SELECT count(*) FROM town.vehicles WHERE colour IN ('grey', 'silver') AND body_type = 'hatchback' AND plate LIKE 'T%' AND plate LIKE '%7%')
    UNION ALL SELECT 'prepaid handsets', 1800,
           (SELECT count(*) FROM town.devices WHERE citizen_id IS NULL)
    UNION ALL SELECT 'prepaid handsets with under six contacts', 40, (
           SELECT count(*) FROM (
               SELECT d.msisdn
               FROM town.devices d
               JOIN town.call_records c ON d.msisdn IN (c.caller_msisdn, c.callee_msisdn)
               WHERE d.citizen_id IS NULL
               GROUP BY 1
               HAVING count(DISTINCT CASE WHEN c.caller_msisdn = d.msisdn THEN c.callee_msisdn ELSE c.caller_msisdn END) < 6))
    UNION ALL SELECT 'left the country in the week after the rally', 120,
           (SELECT count(DISTINCT citizen_id) FROM town.travel_records
            WHERE direction = 'departure' AND destination_type = 'international'
              AND ts::date BETWEEN DATE '2026-05-15' AND DATE '2026-05-21')
    UNION ALL SELECT 'accounts with an unexplained inflow over 2000', 230,
           (SELECT count(DISTINCT account_id) FROM town.card_transactions
            WHERE channel IN ('transfer_in', 'atm_deposit') AND amount > 2000)
)
SELECT
    attribute,
    target,
    observed,
    -- a wider floor on the small populations, where ten per cent is a rounding error
    greatest(1, floor(target * 0.88) - 2)::BIGINT AS low,
    ceil(target * 1.12)::BIGINT + 2               AS high
FROM rates
