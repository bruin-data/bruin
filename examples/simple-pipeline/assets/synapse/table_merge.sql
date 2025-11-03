/* @bruin

name: hello_synapse_table_merge
type: synapse.sql

materialization:
  type: table
  strategy: merge

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
-- hiroshi from "table_create_replace.sql" should be replaced by kentaro
SELECT
    2 AS id,
    'japan' AS country,
    'kentaro' AS name
UNION ALL
-- vladimir from "table_create_replace.sql" should be replaced by ilya
SELECT
    3 AS id,
    'russia' AS country,
    'ilya' AS name
UNION ALL
SELECT
    3 AS id,
    'japan' AS country,
    'satoshi' AS name
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
