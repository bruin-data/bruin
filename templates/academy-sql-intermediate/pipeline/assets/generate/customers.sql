/* @bruin
name: customers
type: duckdb.sql

description: "Customer accounts."

materialization:
  type: table
  strategy: create+replace

columns:
  - name: customer_id
    type: integer
  - name: first_name
    type: varchar
  - name: last_name
    type: varchar
  - name: city
    type: varchar
  - name: state
    type: varchar
  - name: country
    type: varchar
  - name: signed_up_on
    type: date
  - name: segment
    type: varchar
@bruin */

-- Deterministic generation. Every value is a pure function of the row number i.
-- Names and locations are indexed out of short hardcoded lists.
-- Do not introduce random(), now(), or current_date - see docs/_data-design.md.

WITH lists AS (
    SELECT
        ['Alice','Bob','Carla','David','Elena','Frank','Grace','Hassan','Ivy','Jack',
         'Kira','Liam','Mona','Noah','Olivia','Priya','Quinn','Rosa','Sam','Tara'] AS first_names,
        ['Anderson','Brooks','Chen','Diaz','Evans','Fischer','Gupta','Hansen','Ibrahim','Jensen',
         'Kowalski','Lopez','Murphy','Nguyen','Owens','Patel','Rossi','Silva','Tanaka','Vargas'] AS last_names,
        -- Slot 1 is the home market and takes a quarter of the book. The other
        -- eleven share the rest evenly.
        [
            {'city':'New York',  'state':'New York',        'country':'USA'},
            {'city':'Toronto',   'state':'Ontario',         'country':'Canada'},
            {'city':'London',    'state':'England',         'country':'United Kingdom'},
            {'city':'Berlin',    'state':'Berlin',          'country':'Germany'},
            {'city':'Paris',     'state':'Ile-de-France',   'country':'France'},
            {'city':'Sydney',    'state':'New South Wales', 'country':'Australia'},
            {'city':'Madrid',    'state':'Madrid',          'country':'Spain'},
            {'city':'Rome',      'state':'Lazio',           'country':'Italy'},
            {'city':'Amsterdam', 'state':'North Holland',   'country':'Netherlands'},
            {'city':'Dublin',    'state':'Leinster',        'country':'Ireland'},
            {'city':'Stockholm', 'state':'Stockholm',       'country':'Sweden'},
            {'city':'Tokyo',     'state':'Tokyo',           'country':'Japan'}
        ] AS locations,
        -- Five ways of writing the same country. Two differ only by a space, so
        -- TRIM alone is not enough and LOWER alone is not enough either.
        ['USA', 'usa', 'U.S.A.', ' USA', 'USA '] AS home_spellings
),
scrambled AS (
    -- One scrambled counter per column. Indexing two lists with (i * m) % 20 and
    -- (i * n) % 20 looks fine but repeats every twenty rows, so first and last
    -- name would move together and 500 customers would share 20 full names.
    -- Scrambling through a larger prime first breaks that - see rule 5 in
    -- docs/_data-design.md.
    -- range() produces BIGINT; cast to INTEGER so DATE + offset type-checks.
    SELECT
        i,
        (i * 29)  % 101  AS k_first,
        (i * 47)  % 103  AS k_last,
        (i * 61)  % 107  AS k_location,
        (i * 83)  % 109  AS k_segment,
        (i * 313) % 1499 AS k_signup,
        (i * 71)  % 113  AS k_spelling,
        (i * 97)  % 211  AS k_case
    FROM (SELECT CAST(i AS INTEGER) AS i FROM range(1, 501) AS t(i)) AS s
),
placed AS (
    SELECT
        c.i,
        c.k_first,
        c.k_last,
        c.k_segment,
        c.k_signup,
        c.k_spelling,
        c.k_case,
        -- A quarter of the book sits in the home market, the rest spread over the
        -- other eleven cities.
        CASE WHEN c.k_location % 4 = 0
             THEN 1
             ELSE 2 + c.k_location % 11
        END AS location_slot
    FROM scrambled AS c
),
base AS (
    SELECT
        p.i                                                   AS customer_id,
        l.first_names[1 + p.k_first % 20]                     AS first_name,
        l.last_names [1 + p.k_last  % 20]                     AS last_name,
        -- Casing on the city is left alone most of the time and shouted or
        -- whispered on a small slice of rows.
        CASE
            WHEN p.k_case % 40 = 0 THEN upper(l.locations[p.location_slot].city)
            WHEN p.k_case % 40 = 1 THEN lower(l.locations[p.location_slot].city)
            ELSE l.locations[p.location_slot].city
        END                                                   AS city,
        l.locations[p.location_slot].state                    AS state,
        CASE
            WHEN p.location_slot = 1
                 THEN l.home_spellings[1 + p.k_spelling % 5]
            ELSE l.locations[p.location_slot].country
        END                                                   AS country,
        (DATE '2020-01-01' + p.k_signup % 1461)               AS signed_up_on,
        CASE
            WHEN p.k_segment % 10 < 6 THEN 'consumer'
            WHEN p.k_segment % 10 < 9 THEN 'small_business'
            ELSE 'enterprise'
        END                                                   AS segment
    FROM placed AS p
    CROSS JOIN lists AS l
),
-- customer_ids 1..10 appear a second time, as exact copies.
repeated AS (
    SELECT * FROM base WHERE customer_id <= 10
)
SELECT * FROM base
UNION ALL
SELECT * FROM repeated
ORDER BY customer_id, city;
