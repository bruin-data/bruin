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

SELECT 2 AS account_id, 'Bobby' AS account_name, 15 AS score, 'changed-but-not-updated' AS note
UNION ALL
SELECT 3 AS account_id, 'Cara' AS account_name, 30 AS score, 'inserted-three' AS note
