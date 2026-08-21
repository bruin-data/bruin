/* @bruin
name: town.merchants
description: |
  Card terminals registered in Yorkville, one per trading premises.
materialization:
  type: table
depends:
  - town.addresses
columns:
  - name: merchant_id
    type: varchar
    description: Terminal identifier
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: varchar
    description: Trading name
  - name: category
    type: varchar
    description: Category the terminal reports under
  - name: address_id
    type: integer
    description: Trading address
@bruin */

WITH pitches AS (
    SELECT
        address_id,
        row_number() OVER (ORDER BY {{ rnd('address_id', 5601) }}, address_id) AS rk
    FROM town.addresses
    WHERE building_type IN ('commercial', 'mixed_use')
),
pitch_count AS (SELECT count(*) AS n FROM pitches),
spine AS (
    SELECT
        n,
        {{ rnd_int('n', 5610, 0, 39) }}                AS trade_idx,
        {{ pick('n', 5611, streets) }}                 AS street
    FROM generate_series(1, 740) t(n)
)
SELECT
    'M-' || lpad(s.n::VARCHAR, 4, '0')                 AS merchant_id,
    replace(s.street, ' Lane', '') || ' ' || list_extract([{% for t in trade_types %}'{{ t[0] }}'{% if not loop.last %}, {% endif %}{% endfor %}], s.trade_idx + 1) AS name,
    list_extract([{% for t in trade_types %}'{{ t[1] }}'{% if not loop.last %}, {% endif %}{% endfor %}], s.trade_idx + 1) AS category,
    (SELECT address_id FROM pitches WHERE rk = 1 + (s.n - 1) % (SELECT n FROM pitch_count)) AS address_id
FROM spine s
