/* @bruin

name: hello_synapse_table_create_replace
type: synapse.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: id
    type: integer
    description: Just a number
    primary_key: true
    checks:
      - name: not_null
      - name: positive
      - name: non_negative
  - name: country
    type: varchar
    description: the country
    primary_key: true
    checks:
      - name: not_null
  - name: name
    type: varchar
    description: Just a name
    update_on_merge: true
    checks:
      - name: unique
      - name: not_null

@bruin */

SELECT
    1 AS id,
    'morocco' AS country,
    'mohammed' AS name
UNION ALL
SELECT
    2 AS id,
    'japan' AS country,
    'hiroshi' AS name
UNION ALL
SELECT
    3 AS id,
    'russia' AS country,
    'vladimir' AS name
UNION ALL
SELECT
    4 AS id,
    'italy' AS country,
    'gianni' AS name
UNION ALL
SELECT
    5 AS id,
    'united kindgom' AS country,
    'john' AS name
