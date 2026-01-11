/* @bruin

materialization:
  type: table
  partition_by: country
  cluster_by:
    - id

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
    type: string
    description: the country
    primary_key: true
    checks:
      - name: not_null
  - name: name
    type: string
    description: Just a name
    nullable: true
    checks:
      - name: unique

@bruin */

SELECT
    1 AS id,
    'spain' AS country,
    'alberto' AS name
UNION ALL
SELECT
    2 AS id,
    'germany' AS country,
    'frank' AS name
UNION ALL
SELECT
    3 AS id,
    'germany' AS country,
    'franz' AS name
UNION ALL
SELECT
    4 AS id,
    'france' AS country,
    'jean' AS name
UNION ALL
SELECT
    5 AS id,
    'poland' AS country,
    'maciej' AS name
UNION ALL
SELECT
    6 AS id,
    'india' AS country,
    'yuvraj' AS name
