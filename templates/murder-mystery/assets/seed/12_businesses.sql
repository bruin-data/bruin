/* @bruin
name: town.businesses
description: |
  Companies registered in Yorkville: employers, fleet operators, trade contractors
  and a handful of holding companies that own property rather than trade.
materialization:
  type: table
depends:
  - _gen.citizen_base
  - _gen.actor_assignments
  - town.addresses
columns:
  - name: business_id
    type: varchar
    description: Company identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: varchar
    description: Registered company name
  - name: sector
    type: varchar
    description: Trade the company is registered under
  - name: founded
    type: date
    description: Date of incorporation
  - name: principal_citizen_id
    type: varchar
    description: Registered principal, null where the company files no named principal
  - name: address_id
    type: integer
    description: Registered office
  - name: fleet_size
    type: integer
    description: Vehicles registered to the company
@bruin */

WITH a AS (SELECT * FROM _gen.actor_assignments),
-- residents who could plausibly appear on a filing: working age, and not the
-- same person twice
principals AS (
    SELECT
        citizen_id,
        row_number() OVER (ORDER BY {{ rnd('seq', 5001) }}, citizen_id) AS rk
    FROM _gen.citizen_base
    WHERE age_years BETWEEN 26 AND 74
      AND citizen_id NOT IN (SELECT actor_c FROM a)
),
offices AS (
    SELECT
        address_id,
        row_number() OVER (ORDER BY {{ rnd('address_id', 5002) }}, address_id) AS rk
    FROM town.addresses
    WHERE building_type IN ('commercial', 'mixed_use')
),
office_count AS (SELECT count(*) AS n FROM offices),
spine AS (
    SELECT
        n,
        'B-' || lpad(n::VARCHAR, 4, '0')                          AS business_id,
        -- the first four filings on the register are holding companies, which
        -- own property and employ nobody
        CASE
            WHEN n <= 4  THEN 'holding'
            WHEN n = 37  THEN 'construction'
            ELSE {{ weighted('n', 5010, [['retail', 19], ['hospitality', 31], ['construction', 45], ['logistics', 55], ['professional services', 68], ['manufacturing', 78], ['care', 87], ['motor trade', 93], ['agriculture', 100]]) }}
        END                                                       AS sector,
        {{ pick('n', 5011, business_words_a) }}                    AS word_a,
        {{ pick('n', 5012, business_words_b) }}                    AS word_b
    FROM generate_series(1, 620) t(n)
)
SELECT
    s.business_id,
    -- the suffix follows the trade, so a filing never reads as a roofer while it
    -- is registered as a surveyor
    s.word_a || ' ' || CASE s.sector
        WHEN 'holding'               THEN 'Estates'
        WHEN 'construction'          THEN {{ pick('s.n', 5013, ['Contracts', 'Building Works', 'Fabrication', 'Roofing', 'Joinery', 'Plant Hire']) }}
        WHEN 'logistics'             THEN {{ pick('s.n', 5014, ['Haulage', 'Removals', 'Freight', 'Depot', 'Couriers']) }}
        WHEN 'motor trade'           THEN {{ pick('s.n', 5015, ['Motors', 'Garage', 'Tyres', 'Autobody', 'Servicing']) }}
        WHEN 'retail'                THEN {{ pick('s.n', 5016, ['Provisions', 'Outfitters', 'Grocers', 'Hardware', 'Stores', 'Supply']) }}
        WHEN 'hospitality'           THEN {{ pick('s.n', 5017, ['Kitchen', 'Tearooms', 'Tavern', 'Bakery', 'Catering']) }}
        WHEN 'care'                  THEN {{ pick('s.n', 5018, ['Clinic', 'Care', 'Nursing', 'Surgery']) }}
        WHEN 'agriculture'           THEN {{ pick('s.n', 5019, ['Farms', 'Nurseries', 'Feed', 'Growers']) }}
        WHEN 'manufacturing'         THEN {{ pick('s.n', 5020, ['Ironworks', 'Fabrication', 'Works', 'Pressings']) }}
        ELSE                              {{ pick('s.n', 5021, ['Partners', 'Chambers', 'Surveying', 'Group', 'Associates', 'Bookkeeping']) }}
    END                                                           AS name,
    s.sector,
    ({{ rally_date() }} - INTERVAL ({{ rnd_int('s.n', 5020, 400, 14000) }}) DAY)::DATE AS founded,
    -- two filings name the same principal, which is ordinary for anyone who runs
    -- a trade alongside a property company
    CASE
        WHEN s.n IN (1, 37) THEN (SELECT actor_c FROM a)
        WHEN {{ rnd('s.n', 5021) }} < 0.86 THEN (SELECT citizen_id FROM principals WHERE rk = s.n)
        ELSE NULL
    END                                                           AS principal_citizen_id,
    (SELECT address_id FROM offices WHERE rk = 1 + (s.n - 1) % (SELECT n FROM office_count)) AS address_id,
    CASE
        WHEN s.sector = 'holding'   THEN 0
        WHEN s.sector = 'logistics' THEN {{ rnd_int('s.n', 5022, 4, 26) }}
        WHEN s.sector IN ('construction', 'motor trade', 'agriculture') THEN {{ rnd_int('s.n', 5023, 1, 9) }}
        WHEN {{ rnd('s.n', 5024) }} < 0.42 THEN {{ rnd_int('s.n', 5025, 1, 3) }}
        ELSE 0
    END                                                           AS fleet_size
FROM spine s
