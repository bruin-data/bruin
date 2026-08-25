/* @bruin
name: town.employment
description: |
  Job spells recorded against Yorkville companies. A spell with a null end date is
  still running.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - town.businesses
columns:
  - name: spell_id
    type: varchar
    description: Job spell identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: citizen_id
    type: varchar
    description: Employee
    checks:
      - name: not_null
  - name: business_id
    type: varchar
    description: Employer
    checks:
      - name: not_null
  - name: role_title
    type: varchar
    description: Job title as filed
  - name: started
    type: date
    description: Date the spell began
  - name: ended
    type: date
    description: Date the spell ended, null while it is still running
@bruin */

WITH workers AS (
    SELECT citizen_id, seq
    FROM _gen.citizen_base
    WHERE age_years BETWEEN 16 AND 70
),
employers AS (
    SELECT business_id, sector, row_number() OVER (ORDER BY business_id) AS rk
    FROM town.businesses
    WHERE sector <> 'holding'
),
employer_count AS (SELECT count(*) AS n FROM employers),
spine AS (
    SELECT
        w.citizen_id,
        w.seq,
        s.spell
    FROM workers w
    CROSS JOIN generate_series(1, 3) s(spell)
    WHERE {{ rnd('w.seq * 10 + s.spell', 6101) }} < list_extract([0.86, 0.52, 0.22], s.spell)
),
placed AS (
    SELECT
        p.citizen_id,
        p.seq,
        p.spell,
        e.business_id,
        e.sector,
        ({{ rally_date() }} - INTERVAL ({{ rnd_int('p.seq * 10 + p.spell', 6110, 40, 9000) }}) DAY)::DATE AS started
    FROM spine p
    JOIN employers e ON e.rk = 1 + floor({{ rnd('p.seq * 10 + p.spell', 6111) }} * (SELECT n FROM employer_count))::BIGINT
)
SELECT
    'EM-' || lpad(row_number() OVER (ORDER BY citizen_id, spell)::VARCHAR, 6, '0') AS spell_id,
    citizen_id,
    business_id,
    CASE sector
        WHEN 'construction'          THEN {{ pick('seq * 10 + spell', 6120, ['site labourer', 'carpenter', 'roofer', 'plant operator', 'site supervisor', 'maintenance fitter', 'estimator']) }}
        WHEN 'logistics'             THEN {{ pick('seq * 10 + spell', 6121, ['driver', 'warehouse operative', 'loader', 'dispatcher', 'transport clerk']) }}
        WHEN 'retail'                THEN {{ pick('seq * 10 + spell', 6122, ['sales assistant', 'supervisor', 'stock assistant', 'branch manager']) }}
        WHEN 'hospitality'           THEN {{ pick('seq * 10 + spell', 6123, ['kitchen porter', 'cook', 'bar staff', 'waiting staff', 'duty manager']) }}
        WHEN 'professional services' THEN {{ pick('seq * 10 + spell', 6124, ['clerk', 'bookkeeper', 'surveyor', 'draughtsman', 'office manager']) }}
        WHEN 'manufacturing'         THEN {{ pick('seq * 10 + spell', 6125, ['machinist', 'fitter', 'finisher', 'shift supervisor', 'quality inspector']) }}
        WHEN 'care'                  THEN {{ pick('seq * 10 + spell', 6126, ['care assistant', 'nurse', 'porter', 'housekeeper']) }}
        WHEN 'motor trade'           THEN {{ pick('seq * 10 + spell', 6127, ['mechanic', 'tyre fitter', 'parts advisor', 'valeter']) }}
        ELSE                              {{ pick('seq * 10 + spell', 6128, ['farm hand', 'tractor driver', 'stockman', 'groundskeeper']) }}
    END AS role_title,
    started,
    CASE
        WHEN {{ rnd('seq * 10 + spell', 6130) }} < 0.44
            THEN (started + INTERVAL ({{ rnd_int('seq * 10 + spell', 6131, 120, 3000) }}) DAY)::DATE
    END AS ended
FROM placed
