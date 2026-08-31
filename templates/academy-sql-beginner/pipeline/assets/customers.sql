/* @bruin
name: customers
type: duckdb.sql

description: >-
  One row per customer. Names, cities and countries are picked from short
  hand-written lists so the rows read like real people while the file stays
  small. Generated sample data for the Agentic Data Analysis course.
  Deterministic: the same rows are produced on every run.

  Note: customer_id is meant to be unique, but ten customers are duplicated on
  purpose (see docs/known-defects.md), so this table has 510 rows for 500
  distinct customers.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: customer_id
    type: integer
    description: >-
      Identifier for the customer. Meant to be one row per customer, but ten
      ids are duplicated on purpose, so it is NOT unique in this table.
    checks:
      - name: not_null
  - name: first_name
    type: varchar
    description: "Customer first name."
  - name: last_name
    type: varchar
    description: "Customer last name."
  - name: city
    type: varchar
    description: "City the customer lives in."
  - name: state
    type: varchar
    description: "State or region within the country."
  - name: country
    type: varchar
    description: "Country the customer lives in. There are twelve."
  - name: signed_up_on
    type: date
    description: "The date the customer created their account."
  - name: segment
    type: varchar
    description: "Customer segment: consumer, small_business, or enterprise."
@bruin */

-- Deterministic generation. Every value is a pure function of the row number i.
-- Names and locations are indexed out of short hardcoded lists.
-- Do not introduce random(), now(), or current_date - see docs/data-design.md.

WITH lists AS (
    SELECT
        ['Alice','Bob','Carla','David','Elena','Frank','Grace','Hassan','Ivy','Jack',
         'Kira','Liam','Mona','Noah','Olivia','Priya','Quinn','Rosa','Sam','Tara'] AS first_names,
        ['Anderson','Brooks','Chen','Diaz','Evans','Fischer','Gupta','Hansen','Ibrahim','Jensen',
         'Kowalski','Lopez','Murphy','Nguyen','Owens','Patel','Rossi','Silva','Tanaka','Vargas'] AS last_names,
        [
            {'city':'New York',  'state':'New York',        'country':'United States'},
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
        ] AS locations
),
scrambled AS (
    -- One scrambled counter per column. Indexing two lists with (i * m) % 20 and
    -- (i * n) % 20 looks fine but repeats every twenty rows, so first and last
    -- name would move together and 500 customers would share 20 full names.
    -- Scrambling through a larger prime first breaks that - see rule 5 in
    -- docs/data-design.md.
    -- range() produces BIGINT; cast to INTEGER so DATE + offset type-checks.
    SELECT
        i,
        (i * 29)  % 101  AS k_first,
        (i * 47)  % 103  AS k_last,
        (i * 61)  % 107  AS k_location,
        (i * 83)  % 109  AS k_segment,
        (i * 313) % 1499 AS k_signup
    FROM (SELECT CAST(i AS INTEGER) AS i FROM range(1, 501) AS t(i)) AS s
),
base AS (
    SELECT
        c.i                                                     AS customer_id,
        l.first_names[1 + c.k_first    % 20]                    AS first_name,
        l.last_names [1 + c.k_last     % 20]                    AS last_name,
        l.locations  [1 + c.k_location % 12].city               AS city,
        l.locations  [1 + c.k_location % 12].state              AS state,
        l.locations  [1 + c.k_location % 12].country            AS country,
        (DATE '2020-01-01' + c.k_signup % 1461)                 AS signed_up_on,
        CASE
            WHEN c.k_segment % 10 < 6 THEN 'consumer'
            WHEN c.k_segment % 10 < 9 THEN 'small_business'
            ELSE 'enterprise'
        END                                                     AS segment
    FROM scrambled AS c
    CROSS JOIN lists AS l
),
-- Defect 4: ten customers duplicated. customer_ids 1..10 appear twice, as exact
-- copies. See docs/known-defects.md. This is what makes the capstone's
-- dimension fan-out appear, and what DISTINCT seems - but only seems - to fix.
duplicates AS (
    SELECT * FROM base WHERE customer_id <= 10
)
SELECT * FROM base
UNION ALL
SELECT * FROM duplicates
ORDER BY customer_id, city;
