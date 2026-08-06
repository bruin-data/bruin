/* @bruin
name: local.bruin_test.merge_accounts
type: spark.sql

materialization:
  type: table
  strategy: merge

columns:
  - name: account_id
    type: INT
    primary_key: true
  - name: account_name
    type: STRING
    update_on_merge: true
  - name: score
    type: INT
    merge_sql: GREATEST(target.score, source.score)
  - name: note
    type: STRING
@bruin */

SELECT 1 AS account_id, 'Alice' AS account_name, 10 AS score, 'initial-one' AS note
UNION ALL
SELECT 2 AS account_id, 'Bob' AS account_name, 20 AS score, 'initial-two' AS note
